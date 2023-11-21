// Package main
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/alecthomas/kingpin/v2"

	"github.com/kopia/kopia/tools/kats/pems"
)

var (
	// GitTag git tag with sha
	//nolint:gochecknoglobals
	GitTag string
	// ProjectName project name
	//nolint:gochecknoglobals
	ProjectName string
)

const (
	fileSizeMax = 1 << 24
)

var (
	errOverflow = fmt.Errorf("input exceeds maximum input size of %d", fileSizeMax)
	//nolint:gochecknoglobals
	verboseP = kingpin.Flag("verbose", "Verbose mode.").Short('v').Default("false").Bool()
	//nolint:gochecknoglobals
	filenamesP = kingpin.Arg("file", "Input filename").ExistingFiles()
)

func main() {
	filenames := *filenamesP
	verbose := *verboseP

	fmt.Fprintf(os.Stderr, "%s (%s)\n", ProjectName, GitTag)

	_ = kingpin.Parse()
	ctx := context.Background()

	// will use stdin if no args are supplied
	if len(filenames) == 0 {
		// not ideal ... will work for smallish files on stdin
		buf := &bytes.Buffer{}
		// copy Stdin, up to fileSizeMax bytes.
		n, err := io.CopyN(buf, os.Stdin, fileSizeMax)
		if n == fileSizeMax {
			exit("read", errOverflow)
		}

		if err != nil && !errors.Is(err, io.EOF) {
			exit("read", err)
		}

		err = pems.ExportPEMsAsFiles(ctx, verbose, "", buf.Bytes())
		if err != nil && !errors.Is(err, pems.ErrNoPEMFound) {
			exit("export", err)
		}

		return
	}

	for _, fn := range filenames {
		//nolint:gosec
		bs, err := os.ReadFile(fn)
		err1 := pems.ExportPEMsAsFiles(ctx, verbose, "", bs)

		err = errors.Join(err, err1)
		if err != nil {
			exit("export", err)
		}
	}
}

func exit(where string, err error) {
	if err == nil {
		os.Exit(0)
	}

	fmt.Fprintf(os.Stderr, "%s err: %v\n", where, err)
	os.Exit(1)
}
