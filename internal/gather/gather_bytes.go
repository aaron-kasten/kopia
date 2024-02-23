// Package gather implements data structures storing binary data organized
// in a series of byte slices of fixed size that only gathered together by the user.
package gather

import (
	"bytes"
	"io"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

//nolint:gochecknoglobals
var invalidSliceBuf = []byte(uuid.NewString())

// Bytes represents a sequence of bytes split into slices.
type Bytes struct {
	Slices [][]byte

	// for common case where there's one slice, store the slice itself here
	// to avoid allocation
	sliceBuf [1][]byte
}

func (b *Bytes) invalidate() {
	b.sliceBuf[0] = invalidSliceBuf
	b.Slices = nil
}

func (b *Bytes) assertValid() {
	if len(b.sliceBuf[0]) == len(invalidSliceBuf) && bytes.Equal(b.sliceBuf[0], invalidSliceBuf) {
		panic("gather.Bytes is invalid")
	}
}

// AppendSectionTo writes the section of the buffer to the provided writer.
func (b *Bytes) AppendSectionTo(w io.Writer, offset, size int) error {
	b.assertValid()

	if offset < 0 {
		return errors.Errorf("invalid offset")
	}

	// find the index of starting slice
	sliceNdx := -1

	for i, p := range b.Slices {
		if offset < len(p) {
			sliceNdx = i
			break
		}

		offset -= len(p)
	}

	// not found
	if sliceNdx == -1 {
		return nil
	}

	// first slice, possibly with offset zero
	var firstChunkSize int
	if offset+size <= len(b.Slices[sliceNdx]) {
		firstChunkSize = size
	} else {
		// slice shorter
		firstChunkSize = len(b.Slices[sliceNdx]) - offset
	}

	if _, err := w.Write(b.Slices[sliceNdx][offset : offset+firstChunkSize]); err != nil {
		return errors.Wrap(err, "error appending")
	}

	size -= firstChunkSize
	sliceNdx++

	// at this point we're staying at offset 0
	for size > 0 && sliceNdx < len(b.Slices) {
		s := b.Slices[sliceNdx]

		// l is how many bytes we consume out of the current slice
		l := size
		if l > len(s) {
			l = len(s)
		}

		if _, err := w.Write(s[0:l]); err != nil {
			return errors.Wrap(err, "error appending")
		}

		size -= l
		sliceNdx++
	}

	return nil
}

// Length returns the combined length of all slices.
func (b Bytes) Length() int {
	b.assertValid()

	l := 0

	for _, data := range b.Slices {
		l += len(data)
	}

	return l
}

// ReadAt implements io.ReaderAt interface.
func (b Bytes) ReadAt(p []byte, off int64) (n int, err error) {
	b.assertValid()

	return len(p), b.AppendSectionTo(bytes.NewBuffer(p[:0]), int(off), len(p))
}

type bytesReadSeekCloser struct {
	b      Bytes
	offset int
}

func (b *bytesReadSeekCloser) Close() error {
	return nil
}

func (b *bytesReadSeekCloser) Read(buf []byte) (int, error) {
	l := len(buf)
	if b.offset+l > b.b.Length() {
		l = b.b.Length() - b.offset

		if l == 0 {
			return 0, io.EOF
		}
	}

	n, err := b.b.ReadAt(buf[0:l], int64(b.offset))
	b.offset += n

	return n, err
}

func (b *bytesReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	newOffset := b.offset

	switch whence {
	case io.SeekStart:
		newOffset = int(offset)
	case io.SeekCurrent:
		newOffset += int(offset)
	case io.SeekEnd:
		newOffset = b.b.Length() + int(offset)
	}

	if newOffset < 0 || newOffset > b.b.Length() {
		return 0, errors.Errorf("invalid seek")
	}

	b.offset = newOffset

	return int64(newOffset), nil
}

// Reader returns a reader for the data.
func (b Bytes) Reader() io.ReadSeekCloser {
	b.assertValid()

	return &bytesReadSeekCloser{b: b}
}

// AppendToSlice appends the contents to the provided slice.
func (b Bytes) AppendToSlice(output []byte) []byte {
	b.assertValid()

	for _, v := range b.Slices {
		output = append(output, v...)
	}

	return output
}

// ToByteSlice returns contents as a newly-allocated byte slice.
func (b Bytes) ToByteSlice() []byte {
	b.assertValid()

	return b.AppendToSlice(make([]byte, 0, b.Length()))
}

// WriteTo writes contents to the specified writer and returns number of bytes written.
func (b Bytes) WriteTo(w io.Writer) (int64, error) {
	b.assertValid()

	var totalN int64

	for _, v := range b.Slices {
		n, err := w.Write(v)

		totalN += int64(n)

		if err != nil {
			//nolint:wrapcheck
			return totalN, err
		}
	}

	return totalN, nil
}

// FromSlice creates Bytes from the specified slice.
func FromSlice(b []byte) Bytes {
	var r Bytes

	r.sliceBuf[0] = b
	r.Slices = r.sliceBuf[:]

	return r
}

var _ io.ReaderAt = &ReaderWrapper{}
var _ io.WriterAt = &ReaderWrapper{}
var _ io.ReadWriteSeeker = &ReaderWrapper{}

type ReaderWrapper struct {
	Bytes
	i int64
}

func (q *ReaderWrapper) Reader() io.Reader {
	return nil
}

func (q *ReaderWrapper) Read(bs []byte) (int, error) {
	vl0 := 0
	vl1 := 0
	bsi := 0
	for vi := range q.Slices {
		vl0 = vl1
		vl1 += len(q.Slices[vi])
		if int64(vl1) < q.i {
			continue
		}
		j := 0
		if int64(vl0) <= q.i {
			j = int(q.i - int64(vl0))
		}
		bsi += copy(bs[bsi:], q.Slices[vi][j:])
		if bsi == len(bs) {
			break
		}
	}
	q.i += int64(bsi)
	return bsi, nil
}

func (q *ReaderWrapper) Write(bs []byte) (int, error) {
	return 0, nil
}

func (q *ReaderWrapper) WriteAt(bs []byte, off int64) (int, error) {
	return 0, nil
}

func (q *ReaderWrapper) ReadAt(bs []byte, off int64) (int, error) {
	return 0, nil
}

func (q *ReaderWrapper) Seek(i int64, whence int) (int64, error) {
	l := q.Bytes.Length()
	switch whence {
	case io.SeekCurrent:
		q.i += i
	case io.SeekStart:
		q.i = i
	case io.SeekEnd:
		q.i = int64(l) + i
	}
	return q.i, nil
}
