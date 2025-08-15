package query_test

import (
	"testing"

	"github.com/sbchaos/query"
)

func TestPos_String(t *testing.T) {
	if got, want := (query.Pos{}).String(), `-`; got != want {
		t.Fatalf("String()=%q, want %q", got, want)
	}
}
