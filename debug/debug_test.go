package debug

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDebug_parseProfileConfigs(t *testing.T) {
	tcs := []struct {
		in     string
		key    ProfileName
		exists bool
		expect []string
	}{
		{
			in:     "foo=bar",
			key:    "foo",
			exists: true,
			expect: []string{
				"bar",
			},
		},
		{
			in:     "first=one=1",
			key:    "first",
			exists: true,
			expect: []string{
				"one=1",
			},
		},
		{
			in:     "foo=bar:first=one=1",
			key:    "first",
			exists: true,
			expect: []string{
				"one=1",
			},
		},
		{
			in:     "foo=bar:first=one=1,two=2",
			key:    "first",
			exists: true,
			expect: []string{
				"one=1",
				"two=2",
			},
		},
		{
			in:     "foo=bar:first=one=1,two=2:second:third",
			key:    "first",
			exists: true,
			expect: []string{
				"one=1",
				"two=2",
			},
		},
		{
			in:     "foo=bar:first=one=1,two=2:second:third",
			key:    "foo",
			exists: true,
			expect: []string{
				"bar",
			},
		},
		{
			in:     "foo=bar:first=one=1,two=2:second:third",
			key:    "second",
			exists: true,
			expect: nil,
		},
		{
			in:     "foo=bar:first=one=1,two=2:second:third",
			key:    "third",
			exists: true,
			expect: nil,
		},
		{
			in:     "foo=bar:first=one=1,two=2:second:third",
			key:    "fourth",
			exists: false,
		},
	}
	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d %s", i, tc.in), func(t *testing.T) {
			pbs := parseProfileConfigs(1<<10, tc.in)
			pb, ok := pbs[tc.key] // no negative testing for missing keys (see newProfileConfigs)
			require.Equal(t, tc.exists, ok)
			if !tc.exists {
				// don't test the rest if not expecting results
				return
			}
			require.NotNil(t, pb)                 // always not nil
			require.Equal(t, 1<<10, pb.buf.Cap()) // bufsize is always 1024
			require.Equal(t, 0, pb.buf.Len())
			require.Equal(t, tc.expect, pb.flags)
		})
	}
}

func TestDebug_newProfileConfigs(t *testing.T) {
	tcs := []struct {
		in     string
		key    string
		expect string
		ok     bool
	}{
		{
			in:     "foo=bar",
			key:    "foo",
			ok:     true,
			expect: "bar",
		},
		{
			in:     "foo=",
			key:    "foo",
			ok:     true,
			expect: "",
		},
		{
			in:     "",
			key:    "foo",
			ok:     false,
			expect: "",
		},
		{
			in:     "foo=bar",
			key:    "bar",
			ok:     false,
			expect: "",
		},
	}
	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d %s", i, tc.in), func(t *testing.T) {
			pb := newProfileConfig(1<<10, tc.in)
			require.NotNil(t, pb)                 // always not nil
			require.Equal(t, pb.buf.Cap(), 1<<10) // bufsize is always 1024
			v, ok := pb.GetValue(tc.key)
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.expect, v)
		})
	}
}
