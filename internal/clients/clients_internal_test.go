package clients

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSeenForgetsTheOldest(t *testing.T) {
	t.Parallel()
	s := newSeen(1)
	tcs := []struct {
		name string
		id   string
		want bool
	}{
		{"first is new", "a", false},
		{"repeat is seen", "a", true},
		{"second evicts the first", "b", false},
		{"the first is new again", "a", false},
	}
	for _, tc := range tcs {
		assert.Equal(t, tc.want, s.has([]byte(tc.id)), tc.name)
	}
}
