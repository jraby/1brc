package brc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseFixedPoint16(t *testing.T) {
	tcs := []struct {
		b           []byte
		expected    int16
		expectedErr bool
	}{
		{[]byte("1"), 10, false},
		{[]byte("3276"), 32760, false},
		{[]byte("3276.7"), 32767, false},
		{[]byte("1.1"), 11, false},
		{[]byte("1.1123123123"), 11, false},
		{[]byte(".1123123123"), 1, false},
		{[]byte("1."), 10, false},
		{[]byte("000001."), 10, false},

		{[]byte("-1"), -10, false},
		{[]byte("-3276"), -32760, false},
		{[]byte("-3276.7"), -32767, false},
		{[]byte("-1.1"), -11, false},
		{[]byte("-1.1123123123"), -11, false},
		{[]byte("-.1123123123"), -1, false},
		{[]byte("-1."), -10, false},
		{[]byte("-000001."), -10, false},

		{[]byte("9000000000000"), 0, true},  // overflow
		{[]byte("-9000000000000"), 0, true}, // underflow
		{[]byte("1..1"), 0, true},           // invalid
		{[]byte("-1..1"), 0, true},          // invalid
		{[]byte("1.1a"), 0, true},           // invalid
		{[]byte("-1.1a"), 0, true},          // invalid
		{[]byte("patate"), 0, true},         // invalid
	}
	for _, tc := range tcs {
		t.Run(string(tc.b), func(t *testing.T) {
			out, err := ParseFixedPoint16(tc.b)
			if tc.expectedErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tc.expected, out)
		})
	}
}

func TestParseFixedPoint16Unsafe(t *testing.T) {
	tcs := []struct {
		b           []byte
		expected    int16
		expectedErr bool
	}{
		//{[]byte("3276.7"), 32767, false},
		{[]byte("1.1"), 11, false},
		{[]byte("1.0"), 10, false},

		{[]byte("-3276.7"), -32767, false},
		{[]byte("-1.1"), -11, false},
		{[]byte("-1.0"), -10, false},

		{[]byte("9000000000000"), 0, true},  // overflow
		{[]byte("-9000000000000"), 0, true}, // underflow
	}
	for _, tc := range tcs {
		t.Run(string(tc.b), func(t *testing.T) {
			out, err := ParseFixedPoint16Unsafe(tc.b)
			if tc.expectedErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tc.expected, out)
		})
	}
}
