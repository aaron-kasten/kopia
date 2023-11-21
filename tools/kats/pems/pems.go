// Package pems provides tools for parsing PEM text into binary files.
package pems

import (
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const (
	// OutputFileExt default extension for output files.
	OutputFileExt = "bin"
	// MaxFileNameCollisionIndex the maximum number of file-name collisions allowed before failing to produce a file.
	MaxFileNameCollisionIndex = 1000
	// OutputPermsMask most permissive permissions allowed on output file.
	OutputPermsMask = 0o644
)

var (
	elidePunctAndSpace = regexp.MustCompile("([[:space:][:cntrl:][:punct:]])")
	// ErrNoPEMFound did not find PEM information in input.
	ErrNoPEMFound = errors.New("no PEM found")
)

// CreateOutFile create output file.
func CreateOutFile(ctx context.Context, prefix, blknm, ext string) (*os.File, error) {
	max := MaxFileNameCollisionIndex

	i := 0

	f, err := TryWriteFile(ctx, prefix, blknm, ext, i)
	for i < max && err != nil && os.IsExist(err) {
		_ = f.Close()
		i++
		f, err = TryWriteFile(ctx, prefix, blknm, ext, i)
	}

	return f, err
}

// FilenameFromBlockName turn PEM header name into string that can be used in a filename.
func FilenameFromBlockName(blknm, ext string, i int) string {
	// PEM specs spaces only - so this is safe
	nm := elidePunctAndSpace.ReplaceAllLiteralString(strings.ToLower(blknm), "_")

	var q string
	if i <= 0 {
		q = fmt.Sprintf("%s.%s", nm, ext)
	} else {
		q = fmt.Sprintf("%s.%d.%s", nm, i, ext)
	}

	return q
}

// TryWriteFile try and create a file.
func TryWriteFile(ctx context.Context, prefix, blknm, ext string, i int) (f *os.File, err error) {
	// PEM specs spaces only - so this is safe
	fnm := FilenameFromBlockName(blknm, ext, i)
	//nolint:gosec
	f, err = os.OpenFile(filepath.Join(prefix, fnm), os.O_CREATE|os.O_EXCL|os.O_RDWR, OutputPermsMask)
	if err != nil {
		return nil, fmt.Errorf("cannot open file for writing: %w", err)
	}

	return f, nil
}

// ExportPEMAsFile export PEM representation of bs in file, prefix.
func ExportPEMAsFile(ctx context.Context, verbose bool, prefix string, bs []byte) ([]byte, error) {
	// try and decode the next PEM in bs
	blk, rest := pem.Decode(bs)
	if blk == nil {
		// no more PEMs. return.
		return rest, ErrNoPEMFound
	}

	f, err := CreateOutFile(ctx, prefix, blk.Type, OutputFileExt)
	if err != nil {
		return rest, err
	}

	fmt.Fprintf(os.Stdout, "%s\n", f.Name())

	if verbose {
		fmt.Fprintf(os.Stderr, "writing PEM %q as %q\n", blk.Type, f.Name())
	}

	_, err = f.Write(blk.Bytes)
	err1 := f.Close()
	err = errors.Join(err, err1)

	return rest, err
}

// ExportPEMsAsFiles look for byte blocks encoded as PEM in bs.  Export byte blocks to files.
func ExportPEMsAsFiles(ctx context.Context, verbose bool, prefix string, bs []byte) error {
	var err error
	for len(bs) > 0 && err == nil {
		bs, err = ExportPEMAsFile(ctx, verbose, prefix, bs)
	}

	return err
}
