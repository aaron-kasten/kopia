package stress_test

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/kopia/kopia/cli"
	"github.com/kopia/kopia/internal/logfile"
	"github.com/kopia/kopia/tests/testenv"
)

var (
	ppnms           []string
	fLabel          string
	nFlag           int
	n0Flag          int
	n1Flag          int
	f0Size          int
	nSeed           int64
	fRootDir        string
	fCacheDir       string
	fSnapDir        string
	fRepoDir        string
	fLogDir         string
	fRepoBucket     string
	fRepoFormat0    string
	fProfileFormat3 string
	fRootTmpPrefix  string
	fConfigPath     string
	fProfileDir     string
	nReplacement    int
	bCreateRepo     bool
	bVerbose        bool
	nPassword       string
)

func init() {
	flag.StringVar(&fLabel, "stress_test.label", "label", "label for profile dumps")
	flag.IntVar(&nFlag, "stress_test.n", 10, "number of snapshots")
	flag.IntVar(&n0Flag, "stress_test.n0", 10, "number of first level directories")
	flag.IntVar(&n1Flag, "stress_test.n1", 10, "number of second level directories")
	flag.IntVar(&f0Size, "stress_test.fsize0", 4*1024, "size of files to create in bytes")
	//nolint:forbidigo
	flag.Int64Var(&nSeed, "stress_test.seed", time.Now().Unix(), "seed for tests")
	flag.StringVar(&fRootDir, "stress_test.rootdir", "", "output directory for repo")
	flag.StringVar(&fCacheDir, "stress_test.cachedir", "", "cache directory for repo")
	flag.StringVar(&fSnapDir, "stress_test.snapdir", "", "snapshot directory for repo")
	flag.StringVar(&fLogDir, "stress_test.logdir", "", "repository log directory")
	flag.StringVar(&fRepoDir, "stress_test.repodir", "", "repository directory")
	flag.StringVar(&fProfileDir, "stress_test.profiledir", "", "destination directory for profile dump")
	flag.StringVar(&fConfigPath, "stress_test.configfile", "", "configuration file path")
	flag.StringVar(&fRepoBucket, "stress_test.repobucket", "", "repository bucket")
	flag.StringVar(&fRepoFormat0, "stress_test.repoformat", "s3", "format of repository")
	flag.StringVar(&fRootTmpPrefix, "stress_test.roottmpprefix", "Unknown", "generated root dir prefix if no root dir is specified")
	flag.StringVar(&fProfileFormat3, "stress_test.profileformat", "Unknown.%s.%s.%d", "format string for profile dump")
	flag.IntVar(&nReplacement, "stress_test.replacement", 0, "0: no repository, 1: replace, 2: skip, 3: add")
	flag.BoolVar(&bCreateRepo, "stress_test.createrepo", false, "create repository")
	flag.BoolVar(&bVerbose, "stress_test.verbose", false, "verbose output")
	flag.StringVar(&nPassword, "stress_test.repopass", "password", "password for the repository")

	if os.Getenv("KOPIA_STRESS_REPO_PASSWORD") != "" {
		nPassword = os.Getenv("KOPIA_STRESS_REPO_PASSWORD")
	}
	if os.Getenv("KOPIA_STRESS_REPO_CONFIG") != "" {
		fConfigPath = os.Getenv("KOPIA_STRESS_REPO_CONFIG")
	}
	if os.Getenv("KOPIA_STRESS_REPO_FS_PATH") != "" {
		fRepoDir = os.Getenv("KOPIA_STRESS_REPO_FS_PATH")
	}
	if os.Getenv("KOPIA_STRESS_REPO_S3_BUCKET") != "" {
		fRepoBucket = os.Getenv("KOPIA_STRESS_REPO_S3_BUCKET")
	}

	ppnms = []string{
		"goroutine",    //    - stack traces of all current goroutines
		"heap",         // - a sampling of memory allocations of live objects
		"allocs",       // - a sampling of all past memory allocations
		"threadcreate", // - stack traces that led to the creation of new OS threads
		"block",        //     - stack traces that led to blocking on synchronization primitives
		"mutex",        //        - stack traces of holders of contended mutexes
	}
}

//nolint:unparam
func CreateRepoFiles(b *testing.B, rnd *rand.Rand, n0, n1, fsize0, replacement int, root string) {
	b.Helper()

	size := fsize0
	bs := make([]byte, fsize0)

	_, err := rnd.Read(bs)
	if err != nil {
		b.Fatalf("%v", err)
	}

	for i0 := 0; i0 < n0; i0++ {
		dname0 := fmt.Sprintf("dir-%d", i0)

		drootname := fmt.Sprintf("%s/%s", root, dname0)

		err = os.Mkdir(drootname, os.FileMode(0o755))
		if os.IsExist(err) {
			if bVerbose {
				b.Logf("directory %q exists.", drootname)
			}
		} else if err != nil {
			b.Fatalf("%v", err)
		}

		for i1 := 0; i1 < n1; i1++ {
			dname1 := fmt.Sprintf("dir-%d-%d", i0, i1)

			drootname = fmt.Sprintf("%s/%s/%s", root, dname0, dname1)

			err = os.Mkdir(drootname, os.FileMode(0o755))
			if os.IsExist(err) {
				if bVerbose {
					b.Logf("directory %q exists.", drootname)
				}
			} else if err != nil {
				b.Fatalf("%v", err)
			}

			var (
				fname1 string
				fpath1 string
			)

			fname1 = fmt.Sprintf("file-%d-%d", i0, i1)
			fpath1 = fmt.Sprintf("%s/%s/%s/%s", root, dname0, dname1, fname1)

			_, err = os.Stat(fpath1)
			if err != nil && !os.IsNotExist(err) {
				b.Fatalf("%v", err)
			}

			var f *os.File

			if bVerbose {
				b.Logf("creating file %q", fpath1)
			}

			f, err = os.Create(fpath1)
			if err != nil {
				b.Fatalf("%v", err)
			}

			n0, err = rnd.Read(bs)
			if err != nil {
				b.Fatalf("%v", err)
			}

			if n0 != size {
				b.Fatalf("unexpected size")
			}

			buf := bytes.NewBuffer(bs)

			var n2 int64
			n2, err = io.Copy(f, buf)
			if err != nil {
				b.Fatalf("%v", err)
			}

			if n2 != int64(size) {
				b.Fatalf("unexpected size")
			}
		}
	}
}

//nolint:cyclop,gocyclo
func TweakRepoFiles(b *testing.B, rnd *rand.Rand, n0, n1, fsize0, replacement int, root string) {
	deln := 0
	errn := 0
	modn := 0
	addn := 0

	for i0 := 0; i0 < n0; i0++ {
		// first level directory
		dname0 := fmt.Sprintf("dir-%d", i0)
		dpath0 := fmt.Sprintf("%s/%s", root, dname0)

		b.Logf("first level directory %q..", dpath0)

		for i1 := 0; i1 < n1; i1++ {
			var err error

			// second level directory
			dname1 := fmt.Sprintf("dir-%d-%d", i0, i1)
			dpath1 := fmt.Sprintf("%s/%s", dpath0, dname1)

			b.Logf("second level directory %q..", dpath1)

			what := rnd.Intn(5)

			// create file fname1 at dpath1
			fname1 := fmt.Sprintf("file-%d-%d", i0, i1)
			fpath1 := fmt.Sprintf("%s/%s", dpath1, fname1)

			switch what {
			case 0: // make dir and fill
				b.Logf("target file to make-path %q..", dpath1)

				err = os.Mkdir(dpath1, 0o777)
				if os.IsExist(err) {
					errn++

					if bVerbose {
						b.Logf("mkdir %q: directory already exists.", dpath1)
					}
				} else if err != nil {
					errn++

					b.Fatalf("mkdir %q: %v", dpath1, err)
				}

				b.Logf("target file to create %q..", fpath1)

				var f *os.File

				f, err = os.OpenFile(fpath1, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
				if os.IsExist(err) {
					// file already exists
					errn++

					if bVerbose {
						b.Logf("warning: file %q already exists.", fpath1)
					}

					continue
				} else if err != nil {
					errn++

					b.Fatalf("%v", err)
				}

				// fill the file with random data
				_, err = io.CopyN(f, rnd, int64(fsize0))
				if err != nil {
					errn++

					err1 := f.Close()
					if err1 != nil {
						b.Logf("copyn %q: %v", f.Name(), err)
						b.Fatalf("close: %v", err1)
					} else {
						b.Fatalf("copyn %q: %v", f.Name(), err)
					}
				}

				err1 := f.Close()
				if err1 != nil {
					b.Fatalf("close: %v", err1)
				}

				addn++
			case 1: // remove file
				b.Logf("target file to delete %q..", fpath1)

				err = os.Remove(fpath1)
				if os.IsNotExist(err) {
					errn++

					b.Logf("warning: %v", err)
				} else if err != nil {
					errn++

					b.Fatalf("%v", err)
				}

				deln++
			case 2: // modify file
				b.Logf("target file to change %q", fpath1)

				k0 := rnd.Intn(fsize0)
				k1 := rnd.Intn(fsize0)
				imin := k0
				imax := k1

				// reorder the two
				if imin > imax {
					imin, imax = imax, imin
				}

				bs := make([]byte, imax-imin)

				_, err = rnd.Read(bs)
				if err != nil {
					b.Fatalf("%v", err)
				}

				var f *os.File

				f, err = os.OpenFile(fpath1, os.O_RDWR, 0o644)
				if err != nil {
					errn++

					if os.IsNotExist(err) {
						b.Logf("openfile %q: %v", f.Name(), err)
						continue
					}

					err1 := f.Close()
					if err1 != nil {
						b.Logf("openfile %q: %v", f.Name(), err)
						b.Fatalf("close: %v", err1)
					} else {
						b.Fatalf("openfile %q: %v", f.Name(), err)
					}
				}

				// write the data
				_, err = f.WriteAt(bs, int64(imin))
				if err != nil {
					errn++

					err1 := f.Close()
					if err1 != nil {
						b.Logf("writeat %q: %v", f.Name(), err)
						b.Fatalf("close: %v", err1)
					} else {
						b.Fatalf("writeat %q: %v", f.Name(), err)
					}
				}

				_ = f.Close()
				modn++
			}
		}
	}
	b.Logf("deln = %d, errn = %d, modn = %d, addn = %d", deln, errn, modn, addn)
}

// RunKopiaSubcommand run a kopia sub-command in process.
func RunKopiaSubcommand(b *testing.B, ctx context.Context, app *cli.App, kpapp *kingpin.Application, cmd ...string) {
	b.Helper()

	bs0 := bytes.NewBuffer(make([]byte, 1024*64))
	bs1 := bytes.NewBuffer(make([]byte, 1024*64))

	stdout, stderr, wait, _ := app.RunSubcommand(ctx, kpapp, strings.NewReader(""), cmd)

	bs0.Reset()
	bs1.Reset()

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()
		io.Copy(bs0, stdout)
	}()

	go func() {
		defer wg.Done()
		io.Copy(bs1, stderr)
	}()

	err := wait()

	wg.Wait()

	if err != nil {
		b.Fatalf("cannot run subcommand: %s %v", cmd, err)
	}

	b.Logf("%s", bs0)
	b.Logf("%s", bs1)
}

type testDirectories struct {
	rootPath       string
	cachePath      string
	configFilePath string
	repoPath       string
	snapPath       string
	logPath        string
	profPath       string
}

//nolint:unparam
func checkBucket(b *testing.B, ctx context.Context, endpoint, accessKeyID, secretAccessKey, bucketName string, useSSL bool) (bool, error) {
	b.Helper()

	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return false, err
	}

	ok, err := minioClient.BucketExists(ctx, bucketName)
	if err != nil {
		return false, err
	}

	return ok, nil
}

func createBucket(b *testing.B, ctx context.Context, endpoint, accessKeyID, secretAccessKey, bucketName string, useSSL bool) error {
	b.Helper()

	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return err
	}

	err = minioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
	if err != nil {
		return err
	}

	return nil
}

func removeBucket(b *testing.B, ctx context.Context, endpoint, accessKeyID, secretAccessKey, bucketName string, useSSL bool) error {
	b.Helper()

	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return err
	}

	err = minioClient.RemoveBucketWithOptions(ctx, bucketName, minio.RemoveBucketOptions{ForceDelete: true})
	if err != nil {
		return err
	}

	return nil
}

//nolint:unused,gocyclo
func removeObjects(b *testing.B, ctx context.Context, endpoint, accessKeyID, secretAccessKey, bucketName string, useSSL bool) error {
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return err
	}

	cnt := 0

	// List all objects from a bucket-name with a matching prefix.
	osc := minioClient.ListObjects(ctx, bucketName, minio.ListObjectsOptions{Recursive: true})
	for o := range osc {
		if o.Err != nil {
			b.Fatal(o.Err)
		}

		if o.IsDeleteMarker {
			b.Fatal("found delete marker in non-versioned list request.")
		}

		opts := minio.RemoveObjectOptions{
			ForceDelete: true,
			VersionID:   o.VersionID,
		}

		err = minioClient.RemoveObject(ctx, bucketName, o.Key, opts)
		if err != nil {
			b.Fatal(err)
		}

		cnt++

		m := 500
		if cnt%m == 0 {
			b.Logf("removed %d objects", cnt)
		}
	}

	b.Logf("removed %d objects", cnt)

	cnt = 0

	// List all objects from a bucket-name with a matching prefix.
	osc = minioClient.ListObjects(ctx, bucketName, minio.ListObjectsOptions{Recursive: true, WithVersions: true})
	for o := range osc {
		if o.Err != nil {
			b.Fatal(o.Err)
		}

		if o.IsDeleteMarker {
			// skip all the delete markers
			continue
		}

		opts := minio.RemoveObjectOptions{
			ForceDelete: true,
			VersionID:   o.VersionID,
		}

		err = minioClient.RemoveObject(ctx, bucketName, o.Key, opts)
		if err != nil {
			b.Fatal(err)
		}

		cnt++

		m := 500
		if cnt%m == 0 {
			b.Logf("removed %d objects", cnt)
		}
	}

	b.Logf("removed %d objects", cnt)
	cnt = 0

	// List all objects from a bucket-name with a matching prefix.
	osc = minioClient.ListObjects(ctx, bucketName, minio.ListObjectsOptions{Recursive: true, WithVersions: true})
	for o := range osc {
		if o.Err != nil {
			b.Fatal(o.Err)
		}

		// remove only delete markers
		if !o.IsDeleteMarker {
			b.Fatalf("found non-delete marker at %s %s", o.Key, o.VersionID)
		}

		opts := minio.RemoveObjectOptions{
			ForceDelete: true,
			VersionID:   o.VersionID,
		}

		err = minioClient.RemoveObject(ctx, bucketName, o.Key, opts)
		if err != nil {
			b.Fatal(err)
		}

		cnt++

		m := 500
		if cnt%m == 0 {
			b.Logf("removed %d objects", cnt)
		}
	}

	b.Logf("removed %d objects", cnt)

	return nil
}

func setDefaultDirectories(b *testing.B, rootdir, repodir, snapdir, logdir, configpath string) *testDirectories {
	b.Helper()

	q := &testDirectories{
		rootPath:       rootdir,
		repoPath:       repodir,
		snapPath:       snapdir,
		logPath:        logdir,
		configFilePath: configpath,
	}

	q.rootPath = createRootDirectory(b, q.rootPath)

	if q.cachePath == "" {
		q.cachePath = q.rootPath + "/cache"
	}

	if q.configFilePath == "" {
		q.configFilePath = q.rootPath + "/kopia.config"
	}

	if q.logPath == "" {
		q.logPath = q.rootPath + "/logs"
	}

	if q.profPath == "" {
		q.profPath = q.rootPath + "/profiles"
	}

	if q.repoPath == "" {
		q.repoPath = q.rootPath + "/repo"
	}

	if q.snapPath == "" {
		q.snapPath = q.rootPath + "/snap"
	}

	if q.logPath == "" {
		q.logPath = q.rootPath + "/logs"
	}

	return q
}

func newTestingDirectories(b *testing.B, dirs *testDirectories) {
	b.Helper()

	dirs.rootPath = createRootDirectory(b, dirs.rootPath)

	dirMode := os.FileMode(0o775)

	err := os.Mkdir(dirs.cachePath, dirMode)
	if os.IsExist(err) {
		if bVerbose {
			b.Logf("directory %q exists.", dirs.cachePath)
		}
	} else if err != nil {
		b.Fatalf("err: %v", err)
	}

	err = os.Mkdir(dirs.repoPath, dirMode)
	if os.IsExist(err) {
		if bVerbose {
			b.Logf("directory %q exists.", dirs.repoPath)
		}
	} else if err != nil {
		b.Fatalf("err: %v", err)
	}

	err = os.Mkdir(dirs.snapPath, dirMode)
	if os.IsExist(err) {
		if bVerbose {
			b.Logf("directory %q exists.", dirs.snapPath)
		}
	} else if err != nil {
		b.Fatalf("err: %v", err)
	}

	err = os.Mkdir(dirs.logPath, dirMode)
	if os.IsExist(err) {
		if bVerbose {
			b.Logf("directory %q exists.", dirs.logPath)
		}
	} else if err != nil {
		b.Fatalf("err: %v", err)
	}

	err = os.Mkdir(dirs.profPath, dirMode)
	if os.IsExist(err) {
		if bVerbose {
			b.Logf("directory %q exists.", dirs.profPath)
		}
	} else if err != nil {
		b.Fatalf("err: %v", err)
	}

	d := path.Dir(dirs.configFilePath)
	err = os.Mkdir(d, dirMode)
	if os.IsExist(err) {
		if bVerbose {
			b.Logf("directory %q exists.", d)
		}
	} else if err != nil {
		b.Fatalf("err: %v", err)
	}
}

func createRootDirectory(b *testing.B, rootdir string) string {
	b.Helper()

	var err error

	if rootdir != "" {
		var fst os.FileInfo
		fst, err = os.Stat(rootdir)
		if err != nil {
			b.Fatalf("%v", err)
		}

		if !fst.IsDir() {
			b.Fatalf("must be a directory")
		}
	} else {
		rootdir, err = os.MkdirTemp("", "")
		if err != nil {
			b.Fatalf("%v", err)
		}
	}

	return rootdir
}

func startFakeTimeServer(b *testing.B, ctx context.Context, t0 time.Time, factor float64) func() {
	b.Helper()

	fts := testenv.NewFakeTimeServer(func() time.Time {
		//nolint:forbidigo
		t1 := time.Now()
		t2 := t0.Add(time.Duration(float64(t1.Sub(t0)) * factor))
		return t2
	})

	b.Logf("starting time server")

	server := &http.Server{Addr: ":0", Handler: fts}
	addr := server.Addr

	if addr == "" {
		addr = ":http"
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		b.Fatalf("%v", err)
	}

	go func() {
		err = server.Serve(ln)
		if err != nil {
			b.Logf("WARN: error while closing server: %v", err)
		}
	}()

	b.Setenv("KOPIA_FAKE_CLOCK_ENDPOINT", ln.Addr().String())
	b.Logf("time server listening on %q", os.Getenv("KOPIA_FAKE_CLOCK_ENDPOINT"))

	return func() {
		server.Shutdown(ctx)
	}
}

// BenchmarkBlockManager benchmark.
//
//nolint:gocyclo
func BenchmarkBlockManager(b *testing.B) {
	ctx := context.Background()

	//nolint:forbidigo
	firstNow := time.Now()

	shutdownfn := startFakeTimeServer(b, ctx, firstNow, 60.0) // 60 ms for every 1 ms
	defer shutdownfn()

	bs0 := bytes.NewBuffer(make([]byte, 1024*64))
	bs1 := bytes.NewBuffer(make([]byte, 1024*64))

	flag.Parse()

	n0 := n0Flag
	n1 := n1Flag
	fsize0 := f0Size
	flabel0 := fLabel
	seed := nSeed
	n := nFlag
	frepoformat0 := fRepoFormat0
	frepobucket0 := fRepoBucket
	fprofileformat3 := fProfileFormat3
	replacement0 := nReplacement
	createrepo0 := bCreateRepo
	password := nPassword

	tdirs := setDefaultDirectories(b, fRootDir, fRepoDir, fSnapDir, fLogDir, fConfigPath)

	//nolint:lll
	b.Logf("file size = %d; n0 = %d; n1 = %d; label = %q; seed = %d; n = %d; repoformat = %q, rootdir = %q; snapdir = %q, repodir = %q, bucket = %q, replacement = %d, createrepo = %t, cachedir = %q, configpath = %q; logdir = %q; profileformat = %q",
		f0Size, n0, n1, flabel0, seed, n, frepoformat0, tdirs.rootPath, tdirs.snapPath, tdirs.repoPath, frepobucket0, replacement0, createrepo0, tdirs.cachePath, tdirs.configFilePath, tdirs.logPath, fprofileformat3)

	rnd := rand.New(rand.NewSource(seed))

	newTestingDirectories(b, tdirs)

	if nReplacement != 0 {
		b.Logf("creating reposiory files...")
		CreateRepoFiles(b, rnd, n0, n1, fsize0, 0, tdirs.snapPath)
	}

	b.Logf("rootdir = %q", tdirs.rootPath)

	app := cli.NewApp()
	app.AdvancedCommands = "enabled"

	envPrefix := fmt.Sprintf("T%v_", "TESTOLA")
	app.SetEnvNamePrefixForTesting(envPrefix)

	kpapp := kingpin.New("test", "test")
	logfile.Attach(app, kpapp)

	awsSecretAccessKey := ""
	awsAccessKeyID := ""

	switch frepoformat0 {
	case "s3":
		awsSecretAccessKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
		awsAccessKeyID = os.Getenv("AWS_ACCESS_KEY_ID")

		b.Logf("AWS access key ID %q", awsAccessKeyID)
	case "filesystem":
	}

	if createrepo0 {
		// s3 --bucket=BUCKET --access-key=ACCESS-KEY --secret-access-key=SECRET-ACCESS-KEY
		b.Logf("create repository ...")

		switch frepoformat0 {
		case "s3":
			ok, err := checkBucket(b, ctx, "s3.amazonaws.com", awsAccessKeyID, awsSecretAccessKey, frepobucket0, true)
			if err != nil {
				b.Fatalf("cannot access bucket: %v", err)
			}

			if ok {
				b.Logf("discovered old bucket ... removing objects ...")

				err = removeObjects(b, ctx, "s3.amazonaws.com", awsAccessKeyID, awsSecretAccessKey, frepobucket0, true)
				if err != nil {
					b.Fatalf("cannot remove bucket: %v", err)
				}

				b.Logf("removing bucket ...")

				err = removeBucket(b, ctx, "s3.amazonaws.com", awsAccessKeyID, awsSecretAccessKey, frepobucket0, true)
				if err != nil {
					b.Fatalf("%v", err)
				}
			}

			b.Logf("creating new bucket ...")

			err = createBucket(b, ctx, "s3.amazonaws.com", awsAccessKeyID, awsSecretAccessKey, frepobucket0, true)
			if err != nil {
				b.Fatalf("%v", err)
			}

			ok = false
			for !ok {
				ok, err = checkBucket(b, ctx, "s3.amazonaws.com", awsAccessKeyID, awsSecretAccessKey, frepobucket0, true)
				if err != nil {
					b.Fatalf("%#v", err)
				}
				b.Logf("ok = %t", ok)
			}

			RunKopiaSubcommand(b, ctx, app, kpapp, "repository", "create",
				"s3",
				fmt.Sprintf("--bucket=%s", frepobucket0),
				fmt.Sprintf("--secret-access-key=%s", awsSecretAccessKey),
				fmt.Sprintf("--access-key=%s", awsAccessKeyID),
				fmt.Sprintf("--config-file=%s", tdirs.configFilePath),
				fmt.Sprintf("--password=%s", password),
				fmt.Sprintf("--cache-directory=%s", tdirs.cachePath),
				"--persist-credentials")
		case "filesystem":
			RunKopiaSubcommand(b, ctx, app, kpapp, "repository", "create",
				"filesystem",
				fmt.Sprintf("--dir=%s", tdirs.repoPath),
				fmt.Sprintf("--config-file=%s", tdirs.configFilePath),
				fmt.Sprintf("--password=%s", password),
				fmt.Sprintf("--cache-directory=%s", tdirs.cachePath),
				"--persist-credentials")
		}
	}

	func() {
		b.Logf("connecting to repository ...")

		switch frepoformat0 {
		case "s3":
			RunKopiaSubcommand(b, ctx, app, kpapp, "repository", "connect",
				"s3",
				fmt.Sprintf("--bucket=%s", frepobucket0),
				fmt.Sprintf("--secret-access-key=%s", awsSecretAccessKey),
				fmt.Sprintf("--access-key=%s", awsAccessKeyID),
				fmt.Sprintf("--config-file=%s", tdirs.configFilePath),
				fmt.Sprintf("--password=%s", password),
				fmt.Sprintf("--cache-directory=%s", tdirs.cachePath),
				"--persist-credentials")
		case "filesystem":
			RunKopiaSubcommand(b, ctx, app, kpapp, "repository", "connect",
				"filesystem",
				fmt.Sprintf("--dir=%s", tdirs.repoPath),
				fmt.Sprintf("--config-file=%s", tdirs.configFilePath),
				fmt.Sprintf("--password=%s", password),
				fmt.Sprintf("--cache-directory=%s", tdirs.cachePath),
				"--persist-credentials")
		}

		runtime.GC()
	}()

	for j := range ppnms {
		dumpfn := fmt.Sprintf(fprofileformat3, "connect", ppnms[j], 0)
		ppf0, err := os.Create(path.Join(tdirs.profPath, dumpfn))
		if err != nil {
			b.Fatalf("%v", err)
		}

		err = pprof.Lookup(ppnms[j]).WriteTo(ppf0, 0)
		if err != nil {
			ppf0.Close()
			b.Fatalf("%v", err)
		}

		ppf0.Close()
	}

	for i := 0; i < n; i++ {
		func() {
			app = cli.NewApp()
			app.AdvancedCommands = "enabled"

			envPrefix = fmt.Sprintf("T%v_", "TESTOLA")
			app.SetEnvNamePrefixForTesting(envPrefix)

			kpapp = kingpin.New("test", "test")
			logfile.Attach(app, kpapp)

			b.Logf("snapshotting filesystem ...")

			RunKopiaSubcommand(b, ctx, app, kpapp, "snapshot", "create",
				fmt.Sprintf("--config-file=%s", tdirs.configFilePath),
				tdirs.snapPath)
			runtime.GC()
		}()

		for j := range ppnms {
			dumpfn := fmt.Sprintf(fprofileformat3, "connect", ppnms[j], 0)
			ppf0, err := os.Create(path.Join(tdirs.profPath, dumpfn))
			if err != nil {
				b.Fatalf("%v", err)
			}

			err = pprof.Lookup(ppnms[j]).WriteTo(ppf0, 0)
			if err != nil {
				err0 := ppf0.Close()
				if err0 != nil {
					b.Logf("pprof lookup: %v", err)
					b.Fatalf("close: %v", err0)
				} else {
					b.Fatalf("pprof lookup: %v", err)
				}
			}

			ppf0.Close()
		}

		b.Logf("%s", bs0)
		b.Logf("%s", bs1)

		if nReplacement != 0 {
			func() {
				b.Logf("altering filesystem ...")
				TweakRepoFiles(b, rnd, n0, n1, fsize0, 0, tdirs.snapPath)
				runtime.GC()
			}()
		}
	}
}
