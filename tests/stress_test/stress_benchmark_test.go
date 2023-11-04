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
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/kingpin/v2"

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
	fProfileFormat3 string
	fConfigPath     string
	nReplacement    int
	bCreateRepo     bool
	nPassword       string
)

func init() {
	flag.StringVar(&fLabel, "stress_test.label", "label", "label for profile dumps")
	flag.IntVar(&nFlag, "stress_test.n", 10, "number of snapshots")
	flag.IntVar(&n0Flag, "stress_test.n0", 10, "number of first level directories")
	flag.IntVar(&n1Flag, "stress_test.n1", 10, "number of second level directories")
	flag.IntVar(&f0Size, "stress_test.fsize0", 4*1024, "size of files to create in bytes")
	flag.Int64Var(&nSeed, "stress_test.seed", time.Now().Unix(), "seed for tests")
	flag.StringVar(&fRootDir, "stress_test.rootdir", "", "output directory for repo")
	flag.StringVar(&fCacheDir, "stress_test.cachedir", "", "cache directory for repo")
	flag.StringVar(&fSnapDir, "stress_test.snapdir", "", "snapshot directory for repo")
	flag.StringVar(&fLogDir, "stress_test.logdir", "", "repository log directory")
	flag.StringVar(&fRepoDir, "stress_test.repodir", "", "repository directory")
	flag.StringVar(&fConfigPath, "stress_test.configfile", "", "configuration file path")
	flag.StringVar(&fProfileFormat3, "stress_test.profileformat", "Unknown.%s.%s.%d", "prefix for profile dump")
	flag.IntVar(&nReplacement, "stress_test.replacement", 0, "0: no repository, 1: replace, 2: skip, 3: add")
	flag.BoolVar(&bCreateRepo, "stress_test.createrepo", false, "create repository")
	flag.StringVar(&nPassword, "stress_test.repopass", "password", "password for the repository")

	ppnms = []string{
		"goroutine",    //    - stack traces of all current goroutines
		"heap",         // - a sampling of memory allocations of live objects
		"allocs",       // - a sampling of all past memory allocations
		"threadcreate", // - stack traces that led to the creation of new OS threads
		"block",        //     - stack traces that led to blocking on synchronization primitives
		"mutex",        //        - stack traces of holders of contended mutexes
	}
}

func CreateRepoFiles(b *testing.B, rnd *rand.Rand, n0, n1, fsize0 int, replacement int, root string) {
	b.Helper()
	size := fsize0
	bs := make([]byte, fsize0)
	_, err := rnd.Read(bs)
	if err != nil {
		b.Fatalf("%v", err)
	}
	for i0 := 0; i0 < n0; i0++ {
		dname0 := fmt.Sprintf("dir-%d", i0)
		err = os.Mkdir(fmt.Sprintf("%s/%s", root, dname0), os.FileMode(0o775))
		if err != nil {
			b.Fatalf("%v", err)
		}
		for i1 := 0; i1 < n1; i1++ {
			dname1 := fmt.Sprintf("dir-%d-%d", i0, i1)
			err = os.Mkdir(fmt.Sprintf("%s/%s/%s", root, dname0, dname1), os.FileMode(0o775))
			if err != nil {
				b.Fatalf("%v", err)
			}
			var (
				fname1 string
				fpath1 string
			)
			fname1 = fmt.Sprintf("file-%d-%d", i0, i1)
			fpath1 = fmt.Sprintf("%s/%s/%s/%s", root, dname0, dname1, fname1)
			_, err := os.Stat(fpath1)
			if err != nil && !os.IsNotExist(err) {
				b.Fatalf("%v", err)
			}
			f, err := os.Create(fpath1)
			if err != nil {
				b.Fatalf("%v", err)
			}
			n0, err := rnd.Read(bs)
			if err != nil {
				b.Fatalf("%v", err)
			}
			if n0 != size {
				b.Fatalf("unexpected size")
			}
			buf := bytes.NewBuffer(bs)
			n1, err := io.Copy(f, buf)
			if err != nil {
				b.Fatalf("%v", err)
			}
			if n1 != int64(size) {
				b.Fatalf("unexpected size")
			}
		}
	}
}

func TweakRepoFiles(b *testing.B, rnd *rand.Rand, n0, n1, fsize0 int, replacement int, root string) {
	b.Helper()
	bs := make([]byte, fsize0)
	_, err := rnd.Read(bs)
	if err != nil {
		b.Fatalf("%v", err)
	}
	deln := 0
	errn := 0
	modn := 0
	addn := 0
	for i0 := 0; i0 < n0; i0++ {
		dname0 := fmt.Sprintf("dir-%d", i0)
		dpath0 := fmt.Sprintf("%s/%s", root, dname0)
		if err != nil {
			b.Fatalf("%v", err)
		}
		for i1 := 0; i1 < n1; i1++ {
			dname1 := fmt.Sprintf("dir-%d-%d", i0, i1)
			dpath1 := fmt.Sprintf("%s/%s", dpath0, dname1)
			if err != nil {
				errn++
				b.Fatalf("%v", err)
			}
			fname1 := fmt.Sprintf("file-%d-%d", i0, i1)
			fpath1 := fmt.Sprintf("%s/%s", dpath1, fname1)
			var what = rnd.Intn(5)
			switch what {
			case 0:
				_, err := os.Stat(fpath1)
				if err != nil && !os.IsNotExist(err) {
					errn++
					b.Fatalf("%v", err)
				}
				if err == nil {
					errn++
					continue
				}
				// create
				dname1 := fmt.Sprintf("dir-%d-%d", i0, i1)
				err = os.Mkdir(fmt.Sprintf("%s/%s/%s", root, dname0, dname1), os.FileMode(0o775))
				if err != nil {
					errn++
					b.Fatalf("%v", err)
				}
				fname1 := fmt.Sprintf("file-%d-%d", i0, i1)
				f, err := os.Create(fmt.Sprintf("%s/%s/%s/%s", root, dname0, dname1, fname1))
				if err != nil {
					errn++
					b.Fatalf("%v", err)
				}
				n0, err := rnd.Read(bs)
				if err != nil {
					f.Close()
					errn++
					b.Fatalf("%v", err)
				}
				if n0 != fsize0 {
					f.Close()
					errn++
					b.Fatalf("unexpected size")
				}
				buf := bytes.NewBuffer(bs)
				n1, err := io.Copy(f, buf)
				if err != nil {
					f.Close()
					errn++
					b.Fatalf("%v", err)
				}
				if n1 != int64(fsize0) {
					f.Close()
					errn++
					b.Fatalf("unexpected size")
				}
				f.Close()
				addn++
			case 1:
				// delete
				dname1 = fmt.Sprintf("dir-%d-%d", i0, i1)
				err := os.RemoveAll(fmt.Sprintf("%s/%s/%s", root, dname0, dname1))
				if err != nil {
					errn++
					b.Fatalf("%v", err)
				}
				deln++
			case 2:
				// modify
				k0 := rnd.Intn(fsize0)
				k1 := rnd.Intn(fsize0)
				imin := k0
				imax := k1
				if imin > imax {
					imin, imax = imax, imin
				}
				_, err := os.Stat(fpath1)
				if err != nil && !os.IsNotExist(err) {
					errn++
					b.Fatalf("%v", err)
				}
				if err == nil {
					continue
				}
				bs := make([]byte, imax-imin)
				_, err = rnd.Read(bs)
				if err != nil {
					errn++
					b.Fatalf("%v", err)
				}
				// create
				dname1 := fmt.Sprintf("dir-%d-%d", i0, i1)
				err = os.Mkdir(fmt.Sprintf("%s/%s/%s", root, dname0, dname1), os.FileMode(0o775))
				if err != nil {
					errn++
					b.Fatalf("%v", err)
				}
				fname1 := fmt.Sprintf("file-%d-%d", i0, i1)
				f, err := os.Create(fmt.Sprintf("%s/%s/%s/%s", root, dname0, dname1, fname1))
				if err != nil {
					errn++
					b.Fatalf("%v", err)
				}
				_, err = f.Seek(int64(imin), 0)
				if err != nil {
					f.Close()
					errn++
					b.Fatalf("%v", err)
				}
				_, err = f.Write(bs)
				if err != nil {
					f.Close()
					errn++
					b.Fatalf("%v", err)
				}
				f.Close()
				modn++
			}
		}
	}
	b.Logf("deln = %d, errn = %d, modn = %d, addn = %d", deln, errn, modn, addn)
}

func RunKopiaSubcommand(b *testing.B, ctx context.Context, app *cli.App, kpapp *kingpin.Application, cmd ...string) {
	bs0 := &bytes.Buffer{}
	bs0.Grow(1024 * 64)
	bs1 := &bytes.Buffer{}
	bs1.Grow(1024 * 64)

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
		b.Fatalf("%v", err)
	}
	b.Logf("%s", bs0)
	b.Logf("%s", bs1)
}

type testDirectories struct {
	rootPath   string
	cachePath  string
	configPath string
	repoPath   string
	snapPath   string
	logPath    string
}

func newTestingDirectories(b *testing.B, rootdir, repodir, snapdir, logdir, configpath string) *testDirectories {
	b.Helper()
	q := &testDirectories{
		rootPath:   rootdir,
		repoPath:   repodir,
		snapPath:   snapdir,
		logPath:    logdir,
		configPath: configpath,
	}
	q.rootPath = createRootDirectory(b, q.rootPath)
	if q.cachePath == "" {
		q.cachePath = q.rootPath + "/cache"
	}
	if q.configPath == "" {
		q.configPath = q.rootPath + "/kopia.config"
	}
	if q.logPath == "" {
		q.logPath = q.rootPath + "/logs"
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
	dirMode := os.FileMode(0o775)
	os.Mkdir(q.cachePath, dirMode)
	os.Mkdir(q.repoPath, dirMode)
	os.Mkdir(q.snapPath, dirMode)
	os.Mkdir(q.logPath, dirMode)
	os.Mkdir(q.configPath, dirMode)
	return q
}

func createRootDirectory(b *testing.B, rootdir string) string {
	b.Helper()
	if rootdir != "" {
		fst, err := os.Stat(rootdir)
		if err != nil {
			b.Fatalf("%v", err)
		}
		if !fst.IsDir() {
			b.Fatalf("must be a directory")
		}
	} else {
		var err error
		rootdir, err = os.MkdirTemp(os.Getenv("TMPDIR"), "BenchmarkBlockManager.*")
		if err != nil {
			b.Fatalf("ERROR: %v", err)
		}
	}
	return rootdir
}

func startFakeTimeServer(ctx context.Context, b *testing.B, t0 time.Time, factor float64) func() {
	fts := testenv.NewFakeTimeServer(func() time.Time {
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

	os.Setenv("KOPIA_FAKE_CLOCK_ENDPOINT", ln.Addr().String())
	b.Logf("time server listening on %q", os.Getenv("KOPIA_FAKE_CLOCK_ENDPOINT"))

	return func() {
		server.Shutdown(ctx)
	}
}

func BenchmarkBlockManager(b *testing.B) {

	ctx := context.Background()

	firstNow := time.Now()

	shutdownfn := startFakeTimeServer(ctx, b, firstNow, 60.0) // 60 ms for every 1 ms
	defer shutdownfn()

	bs0 := &bytes.Buffer{}
	bs0.Grow(1024 * 64)
	bs1 := &bytes.Buffer{}
	bs1.Grow(1024 * 64)

	flag.Parse()

	n0 := n0Flag
	n1 := n1Flag
	fsize0 := f0Size
	flabel0 := fLabel
	seed := nSeed
	n := nFlag
	frootdir0 := fRootDir
	fsnapdir0 := fSnapDir
	frepodir0 := fRepoDir
	fcachedir0 := fCacheDir
	fconfigpath0 := fRepoDir
	flogdir0 := fLogDir
	fprofileformat3 := fProfileFormat3
	replacement0 := nReplacement
	createrepo0 := bCreateRepo
	password := nPassword

	b.Logf("file size = %d; n0 = %d; n1 = %d; label = %q; seed = %d; n = %d; root = %q; snap = %q, repo = %q, replacement = %d, createrepo = %t, cachedir = %q, configpath = %q; logdir = %q; profileprefix = %q",
		f0Size, n0, n1, flabel0, seed, n, frootdir0, fsnapdir0, frepodir0, replacement0, createrepo0, fcachedir0, fconfigpath0, flogdir0, fprofileformat3)

	rnd := rand.New(rand.NewSource(seed))

	tdirs := newTestingDirectories(b, frootdir0, frepodir0, fsnapdir0, flogdir0, fConfigPath)

	if nReplacement != 0 {
		b.Logf("creating reposiory files...")
		CreateRepoFiles(b, rnd, n0, n1, fsize0, 0, tdirs.snapPath)
	}

	b.Logf("tmpdir = %q", tdirs.rootPath)

	app := cli.NewApp()
	app.AdvancedCommands = "enabled"

	envPrefix := fmt.Sprintf("T%v_", "TESTOLA")
	app.SetEnvNamePrefixForTesting(envPrefix)

	kpapp := kingpin.New("test", "test")
	logfile.Attach(app, kpapp)

	awsSecretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	awsAccessKeyId := os.Getenv("AWS_ACCESS_KEY_ID")

	//if createrepo0 {
	//	// s3 --bucket=BUCKET --access-key=ACCESS-KEY --secret-access-key=SECRET-ACCESS-KEY
	//	RunKopiaSubcommand(b, ctx, app, kpapp, "repository", "create",
	//		"filesystem",
	//		fmt.Sprintf("--path=%s", frepodir0),
	//		fmt.Sprintf("--config-file=%s", fconfigpath0),
	//		fmt.Sprintf("--password=%s", password),
	//		fmt.Sprintf("--cache-directory=%s", fcachedir0),
	//		fmt.Sprintf("--persist-credentials"))
	//}

	if createrepo0 {
		// s3 --bucket=BUCKET --access-key=ACCESS-KEY --secret-access-key=SECRET-ACCESS-KEY
		b.Logf("create repository ...")
		RunKopiaSubcommand(b, ctx, app, kpapp, "repository", "create",
			"s3",
			fmt.Sprintf("--bucket=%s", tdirs.repoPath),
			fmt.Sprintf("--secret-access-key=%s", awsSecretAccessKey),
			fmt.Sprintf("--access-key=%s", awsAccessKeyId),
			fmt.Sprintf("--config-file=%s", fconfigpath0),
			fmt.Sprintf("--password=%s", password),
			fmt.Sprintf("--cache-directory=%s", fcachedir0),
			fmt.Sprintf("--persist-credentials"))
	}

	func() {
		b.Logf("connecting to repository ...")
		RunKopiaSubcommand(b, ctx, app, kpapp, "repository", "connect",
			"s3",
			fmt.Sprintf("--bucket=%s", tdirs.repoPath),
			fmt.Sprintf("--secret-access-key=%s", awsSecretAccessKey),
			fmt.Sprintf("--access-key=%s", awsAccessKeyId),
			fmt.Sprintf("--config-file=%s", tdirs.configPath),
			fmt.Sprintf("--password=%s", password),
			fmt.Sprintf("--cache-directory=%s", tdirs.cachePath),
			fmt.Sprintf("--persist-credentials"))

		runtime.GC()
	}()

	for j := range ppnms {
		ppf0, err := os.Create(fmt.Sprintf(fprofileformat3, "connect", ppnms[j], 0))
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
				fmt.Sprintf("--config-file=%s", tdirs.configPath),
				fmt.Sprintf("%s", tdirs.snapPath))
			runtime.GC()
		}()

		for j := range ppnms {
			ppf0, err := os.Create(fmt.Sprintf(fprofileformat3, "connect", ppnms[j], i+1))
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
