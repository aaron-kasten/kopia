package repo

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/crypto/scrypt"

	"bufio"
	"bytes"
	"encoding/pem"
	"fmt"
	"io"
	"runtime/pprof"

	"runtime"

	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/internal/cacheprot"
	"github.com/kopia/kopia/internal/epoch"
	"github.com/kopia/kopia/internal/feature"
	"github.com/kopia/kopia/internal/metrics"
	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/beforeop"
	loggingwrapper "github.com/kopia/kopia/repo/blob/logging"
	"github.com/kopia/kopia/repo/blob/readonly"
	"github.com/kopia/kopia/repo/blob/storagemetrics"
	"github.com/kopia/kopia/repo/blob/throttling"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/content/indexblob"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
)

// The list below keeps track of features this version of Kopia supports for forwards compatibility.
//
// Repository can specify which features are required to open it and clients will refuse to open the
// repository if they don't have all required features.
//
// In the future we'll be removing features from the list to deprecate them and this will ensure newer
// versions of kopia won't be able to work with old, unmigrated repositories.
//
// The strings are arbitrary, but should be short, human-readable and immutable once a version
// that starts requiring them is released.
//
//nolint:gochecknoglobals
var supportedFeatures = []feature.Feature{
	"index-v1",
	"index-v2",
}

// throttlingWindow is the duration window during which the throttling token bucket fully replenishes.
// the maximum number of tokens in the bucket is multiplied by the number of seconds.
const throttlingWindow = 60 * time.Second

// start with 10% of tokens in the bucket.
const throttleBucketInitialFill = 0.1

// localCacheIntegrityHMACSecretLength length of HMAC secret protecting local cache items.
const localCacheIntegrityHMACSecretLength = 16

//nolint:gochecknoglobals
var localCacheIntegrityPurpose = []byte("local-cache-integrity")

var log = logging.Module("kopia/repo")

// Options provides configuration parameters for connection to a repository.
type Options struct {
	TraceStorage        bool                       // Logs all storage access using provided Printf-style function
	TimeNowFunc         func() time.Time           // Time provider
	DisableInternalLog  bool                       // Disable internal log
	UpgradeOwnerID      string                     // Owner-ID of any upgrade in progress, when this is not set the access may be restricted
	DoNotWaitForUpgrade bool                       // Disable the exponential forever backoff on an upgrade lock.
	BeforeFlush         []RepositoryWriterCallback // list of callbacks to invoke before every flush

	OnFatalError func(err error) // function to invoke when repository encounters a fatal error, usually invokes os.Exit

	// test-only flags
	TestOnlyIgnoreMissingRequiredFeatures bool // ignore missing features
}

// ErrInvalidPassword is returned when repository password is invalid.
var ErrInvalidPassword = format.ErrInvalidPassword

// ErrAlreadyInitialized is returned when repository is already initialized in the provided storage.
var ErrAlreadyInitialized = format.ErrAlreadyInitialized

// ErrRepositoryUnavailableDueToUpgradeInProgress is returned when repository
// is undergoing upgrade that requires exclusive access.
var ErrRepositoryUnavailableDueToUpgradeInProgress = errors.Errorf("repository upgrade in progress")

// Open opens a Repository specified in the configuration file.
func Open(ctx context.Context, configFile, password string, options *Options) (rep Repository, err error) {
	ctx, span := tracer.Start(ctx, "OpenRepository")
	defer span.End()

	defer func() {
		if err != nil {
			log(ctx).Errorf("failed to open repository: %v", err)
		}
	}()

	if options == nil {
		options = &Options{}
	}

	if options.OnFatalError == nil {
		options.OnFatalError = func(err error) {
			log(ctx).Errorf("FATAL: %v", err)
			os.Exit(1)
		}
	}

	configFile, err = filepath.Abs(configFile)
	if err != nil {
		return nil, errors.Wrap(err, "error resolving config file path")
	}

	lc, err := LoadConfigFromFile(configFile)
	if err != nil {
		return nil, err
	}

	if lc.PermissiveCacheLoading && !lc.ReadOnly {
		return nil, ErrCannotWriteToRepoConnectionWithPermissiveCacheLoading
	}

	if lc.APIServer != nil {
		return openAPIServer(ctx, lc.APIServer, lc.ClientOptions, lc.Caching, password, options)
	}

	return openDirect(ctx, configFile, lc, password, options)
}

func getContentCacheOrNil(ctx context.Context, opt *content.CachingOptions, password string, mr *metrics.Registry, timeNow func() time.Time) (*cache.PersistentCache, error) {
	opt = opt.CloneOrDefault()

	cs, err := cache.NewStorageOrNil(ctx, opt.CacheDirectory, opt.MaxCacheSizeBytes, "server-contents")
	if cs == nil {
		// this may be (nil, nil) or (nil, err)
		return nil, errors.Wrap(err, "error opening storage")
	}

	// derive content cache key from the password & HMAC secret using scrypt.
	salt := append([]byte("content-cache-protection"), opt.HMACSecret...)

	//nolint:gomnd
	cacheEncryptionKey, err := scrypt.Key([]byte(password), salt, 65536, 8, 1, 32)
	if err != nil {
		return nil, errors.Wrap(err, "unable to derive cache encryption key from password")
	}

	prot, err := cacheprot.AuthenticatedEncryptionProtection(cacheEncryptionKey)
	if err != nil {
		return nil, errors.Wrap(err, "unable to initialize protection")
	}

	pc, err := cache.NewPersistentCache(ctx, "cache-storage", cs, prot, cache.SweepSettings{
		MaxSizeBytes: opt.MaxCacheSizeBytes,
		MinSweepAge:  opt.MinContentSweepAge.DurationOrDefault(content.DefaultDataCacheSweepAge),
	}, mr, timeNow)
	if err != nil {
		return nil, errors.Wrap(err, "unable to open persistent cache")
	}

	return pc, nil
}

// openAPIServer connects remote repository over Kopia API.
func openAPIServer(ctx context.Context, si *APIServerInfo, cliOpts ClientOptions, cachingOptions *content.CachingOptions, password string, options *Options) (Repository, error) {
	cachingOptions = cachingOptions.CloneOrDefault()

	mr := metrics.NewRegistry()

	contentCache, err := getContentCacheOrNil(ctx, cachingOptions, password, mr, options.TimeNowFunc)
	if err != nil {
		return nil, errors.Wrap(err, "error opening content cache")
	}

	closer := newRefCountedCloser(
		func(ctx context.Context) error {
			if contentCache != nil {
				contentCache.Close(ctx)
			}

			return nil
		},
		mr.Close,
	)

	par := &immutableServerRepositoryParameters{
		cliOpts:          cliOpts,
		contentCache:     contentCache,
		metricsRegistry:  mr,
		refCountedCloser: closer,
		beforeFlush:      options.BeforeFlush,
	}

	if si.DisableGRPC {
		return openRestAPIRepository(ctx, si, password, par)
	}

	return openGRPCAPIRepository(ctx, si, password, par)
}

type ProfileBuffers struct {
	configured           bool
	class                string
	pprofCPUBuf          *bytes.Buffer
	pprofHeapBuf         *bytes.Buffer
	pprofMutexBuf        *bytes.Buffer
	pprofBlockBuf        *bytes.Buffer
	pprofThreadCreateBuf *bytes.Buffer
}

const (
	FeatureKopiaDebugProfileServices               = "KopiaDebugProfileServices"
	FeatureKopiaDebugProfileDumpBufferSizeB        = "KopiaDebugProfileDumpBufferSizeB"
	FeatureKopiaDebugCPUProfileRateHZ              = "KopiaDebugCPUProfileRateHZ"
	FeatureKopiaDebugMutexProfileFraction          = "KopiaDebugMutexProfileFraction"
	FeatureKopiaDebugCPUProfileDumpOnExit          = "KopiaDebugCPUProfileDumpOnExit"
	FeatureKopiaDebugHeapProfileDumpOnExit         = "KopiaDebugHeapProfileDumpOnExit"
	FeatureKopiaDebugMutexProfileDumpOnExit        = "KopiaDebugMutexProfileDumpOnExit"
	FeatureKopiaDebugBlockProfileDumpOnExit        = "KopiaDebugBlockProfileDumpOnExit"
	FeatureKopiaDebugThreadCreateProfileDumpOnExit = "KopiaDebugThreadCreateProfileDumpOnExit"
	FeatureKopiaDebugTraceDumpOnExit               = "KopiaDebugTraceDumpOnExit"
	FeatureK10ComplianceCacheSizeN                 = "K10ComplianceCacheSizeN"
	FeatureK10DefaultDebugProfileServices          = ""
	FeatureK10DefaultComplianceCacheSizeN          = 15000 // results in approx 1mb footprint
	FeatureK10DefaultDebugProfileDumpBufferSizeB   = 1 << 24
	FeatureKopiaDebugSuffixGoMemLimit              = "GoMemLimit"
	FeatureKopiaDebugSuffixGoMaxProcs              = "GoMaxProcs"
	FeatureKopiaDebugSuffixGoGc                    = "GoGc"
	FeatureKopiaDebugSuffixGoTraceback             = "GoTraceback"
	FeatureKopiaDebugSuffixGoDebug                 = "GoDebug"
	FeatureKopiaDebugDeleteCollectionPrefix        = "KopiaDebugDeleteCollection"
	FeatureKopiaDebugDeleteDataPrefix              = "KopiaDebugDeleteData"
	FeatureKopiaDebugDatamoverPrefix               = "KopiaDebugDatamover"
	FeatureKopiaDebugDatamoverServicePrefix        = "KopiaDebugDatamoverService"
	FeatureKopiaDebugCopyVolumeDataPrefix          = "KopiaDebugCopyVolumeData"
	FeatureKopiaDebugKopiaMaintenancePrefix        = "KopiaDebugKopiaMaintenance"
	GoDebugEnvvarGoMemLimit                        = "GOMEMLIMIT"
	GoDebugEnvvarGoMaxProcs                        = "GOMAXPROCS"
	GoDebugEnvvarGoGc                              = "GOGC"
	GoDebugEnvvarGoTraceback                       = "GOTRACEBACK"
	GoDebugEnvvarGoDebug                           = "GODEBUG"
)

// StartProfileBuffers start profile buffers for enabled profiles/trace.  Buffers
// are returned in an slice of buffers: CPU, Heap and trace respectively.
func StartProfileBuffers(class string) (bufs ProfileBuffers, err error) {
	bufSizeB := FeatureK10DefaultDebugProfileDumpBufferSizeB
	// look for matching services.  "*" signals all services for profiling
	fmt.Fprintf(os.Stdout, "configuring profile buffers for %q\n", class)
	bufs.class = class
	bufs.pprofCPUBuf = bytes.NewBuffer(make([]byte, 0, bufSizeB))
	bufs.pprofHeapBuf = bytes.NewBuffer(make([]byte, 0, bufSizeB))
	bufs.pprofThreadCreateBuf = bytes.NewBuffer(make([]byte, 0, bufSizeB))
	err = pprof.StartCPUProfile(bufs.pprofCPUBuf)
	if err != nil {
		return ProfileBuffers{}, err
	}
	bufs.configured = true
	return bufs, nil
}

// DumpPem dump a PEM version of the reader, rdr, onto writer, wrt
func DumpPem(ctx context.Context, bs []byte, types string, wrt io.Writer) error {
	blk := &pem.Block{
		Type:  types,
		Bytes: bs,
	}
	// wrt is likely a line oriented writer, so writing individual lines
	// will make best use of output buffer and help prevent overflows or
	// stalls in the output path.
	pr, pw := io.Pipe()
	// encode PEM in the background and output in a line oriented
	// fashion - this prevents the need for a large buffer to hold
	// the encoded PEM.
	go func() {
		defer pw.Close()
		err := pem.Encode(pw, blk)
		if err != nil {
			log(ctx).With("cause", err, "type", types).Error("cannot encode PEM")
		}
	}()
	rdr := bufio.NewReader(pr)
	for {
		ln, err0 := rdr.ReadBytes('\n')
		_, err1 := fmt.Fprint(wrt, string(ln))
		if errors.Is(err0, io.EOF) {
			break
		}
		if err0 != nil {
			return err0
		}
		if err1 != nil {
			return err1
		}
	}
	return nil
}

// StopProfileBuffers stop and dump the contents of the buffers to the log as PEMs.  Buffers
// supplied here are from StartProfileBuffers
func StopProfileBuffers(ctx context.Context, bufs ProfileBuffers) {
	if !bufs.configured {
		fmt.Fprintf(os.Stderr, "profile buffers unconfigured for %q.\n", bufs.class)
		return
	}
	fmt.Fprintf(os.Stderr, "saving %q PEM buffers for output\n", bufs.class)
	if bufs.pprofThreadCreateBuf != nil {
		pprof.Lookup("threadcreate").WriteTo(bufs.pprofThreadCreateBuf, 0)
	}
	// each profile type requires special handling
	if bufs.pprofCPUBuf != nil {
		// don't get heap profile dump data in CPU profile
		pprof.StopCPUProfile()
	}
	if bufs.pprofHeapBuf != nil {
		// don't get buffer writes in heap profile
		runtime.GC()
		err := pprof.Lookup("heap").WriteTo(bufs.pprofHeapBuf, 0)
		if err != nil {
			log(ctx).With("cause", err).Errorf("cannot write heap profile for %q", bufs.class)
		}
	}
	// dump the profiles out into their respective PEMs
	pems := []*bytes.Buffer{bufs.pprofCPUBuf, bufs.pprofHeapBuf, bufs.pprofThreadCreateBuf}
	types := []string{
		fmt.Sprintf("%s PPROF CPU", bufs.class),
		fmt.Sprintf("%s PPROF MEM", bufs.class),
		fmt.Sprintf("%s PPROF THREAD_CREATION", bufs.class)}
	for i := range pems {
		if pems[i] == nil || pems[i].Len() == 0 {
			continue
		}
		fmt.Fprintf(os.Stderr, "dumping PEM for %q\n", types[i])
		err := DumpPem(ctx, pems[i].Bytes(), types[i], os.Stderr)
		if err != nil {
			log(ctx).With("cause", err).Errorf("cannot write PEM for %q", bufs.class)
		}
	}
}

// openDirect opens the repository that directly manipulates blob storage..
func openDirect(ctx context.Context, configFile string, lc *LocalConfig, password string, options *Options) (rep Repository, err error) {
	if lc.Storage == nil {
		return nil, errors.Errorf("storage not set in the configuration file")
	}

	st, err := blob.NewStorage(ctx, *lc.Storage, false)
	if err != nil {
		return nil, errors.Wrap(err, "cannot open storage")
	}

	bufs, err := StartProfileBuffers("KOPIA OPEN DIRECT REPO")
	if err != nil {
		return nil, errors.Wrap(err, "unable to setup profile buffers")
	}

	if options.TraceStorage {
		st = loggingwrapper.NewWrapper(st, log(ctx), "[STORAGE] ")
	}

	if lc.ReadOnly {
		st = readonly.NewWrapper(st)
	}

	cliOpts := lc.ApplyDefaults(ctx, "Repository in "+st.DisplayName())

	r, err := openWithConfig(ctx, st, bufs, cliOpts, password, options, lc.Caching, configFile)
	if err != nil {
		st.Close(ctx) //nolint:errcheck
		return nil, err
	}

	return r, nil
}

// openWithConfig opens the repository with a given configuration, avoiding the need for a config file.
//
//nolint:funlen,gocyclo
func openWithConfig(ctx context.Context, st blob.Storage, bufs ProfileBuffers, cliOpts ClientOptions, password string, options *Options, cacheOpts *content.CachingOptions, configFile string) (DirectRepository, error) {
	cacheOpts = cacheOpts.CloneOrDefault()
	cmOpts := &content.ManagerOptions{
		TimeNow:                defaultTime(options.TimeNowFunc),
		DisableInternalLog:     options.DisableInternalLog,
		PermissiveCacheLoading: cliOpts.PermissiveCacheLoading,
	}

	mr := metrics.NewRegistry()
	st = storagemetrics.NewWrapper(st, mr)

	fmgr, ferr := format.NewManager(ctx, st, cacheOpts.CacheDirectory, cliOpts.FormatBlobCacheDuration, password, cmOpts.TimeNow)
	if ferr != nil {
		return nil, errors.Wrap(ferr, "unable to create format manager")
	}

	// check features before and perform configuration before performing IO
	if err := handleMissingRequiredFeatures(ctx, fmgr, options.TestOnlyIgnoreMissingRequiredFeatures); err != nil {
		return nil, err
	}

	if fmgr.SupportsPasswordChange() {
		cacheOpts.HMACSecret = format.DeriveKeyFromMasterKey(fmgr.GetHmacSecret(), fmgr.UniqueID(), localCacheIntegrityPurpose, localCacheIntegrityHMACSecretLength)
	} else {
		// deriving from ufb.FormatEncryptionKey was actually a bug, that only matters will change when we change the password
		cacheOpts.HMACSecret = format.DeriveKeyFromMasterKey(fmgr.FormatEncryptionKey(), fmgr.UniqueID(), localCacheIntegrityPurpose, localCacheIntegrityHMACSecretLength)
	}

	limits := throttlingLimitsFromConnectionInfo(ctx, st.ConnectionInfo())
	if cliOpts.Throttling != nil {
		limits = *cliOpts.Throttling
	}

	st, throttler, ferr := addThrottler(st, limits)
	if ferr != nil {
		return nil, errors.Wrap(ferr, "unable to add throttler")
	}

	throttler.OnUpdate(func(l throttling.Limits) error {
		lc2, err2 := LoadConfigFromFile(configFile)
		if err2 != nil {
			return err2
		}

		lc2.Throttling = &l

		return lc2.writeToFile(configFile)
	})

	blobcfg, err := fmgr.BlobCfgBlob()
	if err != nil {
		return nil, errors.Wrap(err, "blob configuration")
	}

	if blobcfg.IsRetentionEnabled() {
		st = wrapLockingStorage(st, blobcfg)
	}

	_, err = retry.WithExponentialBackoffMaxRetries(ctx, -1, "wait for upgrade", func() (interface{}, error) {
		//nolint:govet
		uli, err := fmgr.UpgradeLockIntent()
		if err != nil {
			//nolint:wrapcheck
			return nil, err
		}

		// retry if upgrade lock has been taken
		if !cliOpts.PermissiveCacheLoading {
			if locked, _ := uli.IsLocked(cmOpts.TimeNow()); locked && options.UpgradeOwnerID != uli.OwnerID {
				return nil, ErrRepositoryUnavailableDueToUpgradeInProgress
			}
		}

		return false, nil
	}, func(internalErr error) bool {
		return !options.DoNotWaitForUpgrade && errors.Is(internalErr, ErrRepositoryUnavailableDueToUpgradeInProgress)
	})
	if err != nil {
		return nil, err
	}

	if !cliOpts.PermissiveCacheLoading {
		// background/interleaving upgrade lock storage monitor
		st = upgradeLockMonitor(fmgr, options.UpgradeOwnerID, st, cmOpts.TimeNow, options.OnFatalError, options.TestOnlyIgnoreMissingRequiredFeatures)
	}

	scm, ferr := content.NewSharedManager(ctx, st, fmgr, cacheOpts, cmOpts, mr)
	if ferr != nil {
		return nil, errors.Wrap(ferr, "unable to create shared content manager")
	}

	cm := content.NewWriteManager(ctx, scm, content.SessionOptions{
		SessionUser: cliOpts.Username,
		SessionHost: cliOpts.Hostname,
	}, "")

	om, ferr := object.NewObjectManager(ctx, cm, fmgr.ObjectFormat(), mr)
	if ferr != nil {
		return nil, errors.Wrap(ferr, "unable to open object manager")
	}

	manifests, ferr := manifest.NewManager(ctx, cm, manifest.ManagerOptions{TimeNow: cmOpts.TimeNow}, mr)
	if ferr != nil {
		return nil, errors.Wrap(ferr, "unable to open manifests")
	}

	closer := newRefCountedCloser(
		scm.CloseShared,
		mr.Close,
	)

	dr := &directRepository{
		bufs:  bufs,
		cmgr:  cm,
		omgr:  om,
		blobs: st,
		mmgr:  manifests,
		sm:    scm,
		immutableDirectRepositoryParameters: immutableDirectRepositoryParameters{
			cachingOptions:   *cacheOpts,
			fmgr:             fmgr,
			timeNow:          cmOpts.TimeNow,
			cliOpts:          cliOpts,
			configFile:       configFile,
			nextWriterID:     new(int32),
			throttler:        throttler,
			metricsRegistry:  mr,
			refCountedCloser: closer,
			beforeFlush:      options.BeforeFlush,
		},
	}

	dr.registerEarlyCloseFunc(func(ctx context.Context) error {
		dr.CloseDebug(ctx)
		return nil
	})

	return dr, nil
}

func handleMissingRequiredFeatures(ctx context.Context, fmgr *format.Manager, ignoreErrors bool) error {
	required, err := fmgr.RequiredFeatures()
	if err != nil {
		return errors.Wrap(err, "required features")
	}

	// See if the current version of Kopia supports all features required by the repository format.
	// so we can safely fail to start in case repository has been upgraded to a new, incompatible version.
	if missingFeatures := feature.GetUnsupportedFeatures(required, supportedFeatures); len(missingFeatures) > 0 {
		for _, mf := range missingFeatures {
			if ignoreErrors || mf.IfNotUnderstood.Warn {
				log(ctx).Warnf("%s", mf.UnsupportedMessage())
			} else {
				// by default, fail hard
				return errors.Errorf("%s", mf.UnsupportedMessage())
			}
		}
	}

	return nil
}

func wrapLockingStorage(st blob.Storage, r format.BlobStorageConfiguration) blob.Storage {
	// collect prefixes that need to be locked on put
	var prefixes []string
	for _, prefix := range content.PackBlobIDPrefixes {
		prefixes = append(prefixes, string(prefix))
	}

	prefixes = append(prefixes, indexblob.V0IndexBlobPrefix, epoch.EpochManagerIndexUberPrefix, format.KopiaRepositoryBlobID,
		format.KopiaBlobCfgBlobID)

	return beforeop.NewWrapper(st, nil, nil, nil, func(ctx context.Context, id blob.ID, opts *blob.PutOptions) error {
		for _, prefix := range prefixes {
			if strings.HasPrefix(string(id), prefix) {
				opts.RetentionMode = r.RetentionMode
				opts.RetentionPeriod = r.RetentionPeriod
				break
			}
		}
		return nil
	})
}

func addThrottler(st blob.Storage, limits throttling.Limits) (blob.Storage, throttling.SettableThrottler, error) {
	throttler, err := throttling.NewThrottler(limits, throttlingWindow, throttleBucketInitialFill)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to create throttler")
	}

	return throttling.NewWrapper(st, throttler), throttler, nil
}

func upgradeLockMonitor(
	fmgr *format.Manager,
	upgradeOwnerID string,
	st blob.Storage,
	now func() time.Time,
	onFatalError func(err error),
	ignoreMissingRequiredFeatures bool,
) blob.Storage {
	var (
		m             sync.RWMutex
		lastCheckTime time.Time
	)

	cb := func(ctx context.Context) error {
		m.RLock()
		// see if we already checked that revision
		if lastCheckTime.Equal(fmgr.LoadedTime()) {
			m.RUnlock()
			return nil
		}
		m.RUnlock()

		// upgrade the lock and verify again in-case someone else won the race to refresh
		m.Lock()
		defer m.Unlock()

		ltime := fmgr.LoadedTime()

		if lastCheckTime.Equal(ltime) {
			return nil
		}

		uli, err := fmgr.UpgradeLockIntent()
		if err != nil {
			return errors.Wrap(err, "upgrade lock intent")
		}

		if err := handleMissingRequiredFeatures(ctx, fmgr, ignoreMissingRequiredFeatures); err != nil {
			onFatalError(err)
			return err
		}

		if uli != nil {
			// only allow the upgrade owner to perform storage operations
			if locked, _ := uli.IsLocked(now()); locked && upgradeOwnerID != uli.OwnerID {
				return ErrRepositoryUnavailableDueToUpgradeInProgress
			}
		}

		lastCheckTime = ltime

		return nil
	}

	return beforeop.NewUniformWrapper(st, cb)
}

func throttlingLimitsFromConnectionInfo(ctx context.Context, ci blob.ConnectionInfo) throttling.Limits {
	v, err := json.Marshal(ci.Config)
	if err != nil {
		return throttling.Limits{}
	}

	var l throttling.Limits

	if err := json.Unmarshal(v, &l); err != nil {
		return throttling.Limits{}
	}

	log(ctx).Debugw("throttling limits from connection info", "limits", l)

	return l
}
