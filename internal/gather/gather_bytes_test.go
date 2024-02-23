package gather

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"testing/iotest"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

var sample1 = []byte("hello! how are you? nice to meet you.")

type failingWriter struct {
	err error
}

func (w failingWriter) Write(buf []byte) (int, error) {
	return 0, w.err
}

func TestGatherBytes(t *testing.T) {
	// split the 'whole' into equivalent Bytes slicings in some interesting ways
	cases := []struct {
		whole  []byte
		sliced Bytes
	}{
		{
			whole:  []byte{},
			sliced: Bytes{},
		},
		{
			whole: []byte{},
			sliced: Bytes{Slices: [][]byte{
				nil,
			}},
		},
		{
			whole: []byte{},
			sliced: Bytes{Slices: [][]byte{
				nil,
				{},
				nil,
			}},
		},
		{
			whole:  sample1,
			sliced: FromSlice(sample1),
		},
		{
			whole: sample1,
			sliced: Bytes{Slices: [][]byte{
				nil,
				sample1,
				nil,
			}},
		},
		{
			whole: sample1,
			sliced: Bytes{Slices: [][]byte{
				sample1[0:20],
				sample1[20:],
			}},
		},
		{
			whole: sample1,
			sliced: Bytes{Slices: [][]byte{
				sample1[0:20],
				nil, // zero-length
				{},  // zero-length
				sample1[20:],
			}},
		},
		{
			whole: sample1,
			sliced: Bytes{Slices: [][]byte{
				sample1[0:10],
				sample1[10:25],
				sample1[25:30],
				sample1[30:31],
				sample1[31:],
			}},
		},
	}

	for _, tc := range cases {
		func() {
			b := tc.sliced

			// length
			if got, want := b.Length(), len(tc.whole); got != want {
				t.Errorf("unexpected length: %v, want %v", got, want)
			}

			// reader
			all, err := io.ReadAll(b.Reader())
			if err != nil {
				t.Errorf("unable to read: %v", err)
			}

			if !bytes.Equal(all, tc.whole) {
				t.Errorf("unexpected data read %v, want %v", string(all), string(tc.whole))
			}

			// GetBytes
			all = b.ToByteSlice()
			if !bytes.Equal(all, tc.whole) {
				t.Errorf("unexpected data from GetBytes() %v, want %v", string(all), string(tc.whole))
			}

			// AppendSectionTo - test exhaustively all combinationf os start, length
			var tmp WriteBuffer
			defer tmp.Close()

			n, err := b.WriteTo(&tmp)

			require.NoError(t, err)
			require.Equal(t, int64(b.Length()), n)

			require.Equal(t, tmp.ToByteSlice(), b.ToByteSlice())

			someError := errors.Errorf("some error")

			// WriteTo propagates error
			if b.Length() > 0 {
				_, err = b.WriteTo(failingWriter{someError})

				require.ErrorIs(t, err, someError)
			}

			require.Error(t, b.AppendSectionTo(&tmp, -3, 3))

			for i := 0; i <= len(tc.whole); i++ {
				for j := i; j <= len(tc.whole); j++ {
					tmp.Reset()

					require.NoError(t, b.AppendSectionTo(&tmp, i, j-i))

					if j > i {
						require.ErrorIs(t, b.AppendSectionTo(failingWriter{someError}, i, j-i), someError)
					}

					require.Equal(t, tmp.ToByteSlice(), tc.whole[i:j])
				}
			}
		}()
	}
}

func TestGatherBytesReadSeeker(t *testing.T) {
	var tmp WriteBuffer
	defer tmp.Close()

	buf := make([]byte, 1234567)

	tmp.Append(buf)

	require.Len(t, buf, tmp.Length())

	reader := tmp.inner.Reader()
	defer reader.Close() //nolint:errcheck

	require.NoError(t, iotest.TestReader(reader, buf))

	_, err := reader.Seek(-3, io.SeekStart)
	require.Error(t, err)

	_, err = reader.Seek(3, io.SeekEnd)
	require.Error(t, err)

	_, err = reader.Seek(10000000, io.SeekCurrent)
	require.Error(t, err)
}

func TestGatherBytesPanicsOnClose(t *testing.T) {
	var tmp WriteBuffer

	tmp.Append([]byte{1, 2, 3})
	tmp.Close()

	require.Panics(t, func() {
		tmp.Bytes().Reader()
	})
}

func TestGatherBytes_File(t *testing.T) {
	td := t.TempDir()
	f, err := os.OpenFile(filepath.Join(td, "test"), os.O_CREATE|os.O_RDWR, 0x666)
	require.NoError(t, err)
	f.WriteString("this is an example")
	n, err := f.Seek(0, -1)
	oserr, ok := err.(*fs.PathError)
	require.True(t, ok)
	require.Equal(t, oserr.Err, syscall.EINVAL)
	n, err = f.Seek(0, 0)
	require.NoError(t, err)
	require.Equal(t, int64(0), n)
	n, err = f.Seek(-2, 0)
	oserr, ok = err.(*fs.PathError)
	require.True(t, ok)
	require.Equal(t, oserr.Err, syscall.EINVAL)
	n, err = f.Seek(-1, 1)
	oserr, ok = err.(*fs.PathError)
	require.True(t, ok)
	require.Equal(t, oserr.Err, syscall.EINVAL)

	n, err = f.Seek(0, 0)
	require.NoError(t, err)
	require.Equal(t, int64(0), n)

	n, err = f.Seek(0, 2)
	require.NoError(t, err)
	require.Equal(t, int64(18), n)

	_, err = f.WriteString("foo")
	n, err = f.Seek(0, 1)
	require.NoError(t, err)
	require.Equal(t, int64(21), n)

	n, err = f.Seek(0, 1)
	require.NoError(t, err)
	require.Equal(t, int64(21), n)

	n0, err := f.WriteString("foo")
	require.NoError(t, err)
	require.Equal(t, 3, n0)

	n, err = f.Seek(0, 1)
	require.NoError(t, err)
	require.Equal(t, int64(3), n)
}

func TestGatherBytes_ReaderWrapper(t *testing.T) {
	tcs := []struct {
		in     []string
		bs     []byte
		n      int
		seek   int64
		whence int
		out    string
	}{
		{
			in:     []string{"this that some", "and something else"},
			bs:     make([]byte, 10),
			seek:   0,
			whence: 0,
			out:    "this that ",
			n:      10,
		},
		{
			in:     []string{"this that some", " and something else"},
			bs:     make([]byte, 10),
			seek:   10,
			whence: 0,
			out:    "some and s",
			n:      10,
		},
		{
			in:     []string{"this that some", " and something else"},
			bs:     make([]byte, 10),
			seek:   13,
			whence: 0,
			out:    "e and some",
			n:      10,
		},
		{
			in:     []string{"this that some", " and something else"},
			bs:     make([]byte, 10),
			seek:   14,
			whence: 0,
			out:    " and somet",
			n:      10,
		},
		{
			in:     []string{"this that some", " and something else"},
			bs:     make([]byte, 10),
			seek:   25,
			whence: 0,
			out:    "ing else\u0000\u0000",
			n:      8,
		},
	}

	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			q := &ReaderWrapper{}
			bss := [][]byte{}
			for _, b := range tc.in {
				bss = append(bss, []byte(b))
			}
			q.Bytes.Slices = bss
			_, err := q.Seek(tc.seek, tc.whence)
			require.NoError(t, err)
			n, err := q.Read(tc.bs)
			require.NoError(t, err)
			require.Equal(t, tc.n, n)
			require.Equal(t, tc.bs, []byte(tc.out))
		})
	}
}
