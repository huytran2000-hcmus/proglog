package log

import (
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func assertEqual(t testing.TB, want, got any) {
	t.Helper()
	if diff := cmp.Diff(want, got); diff != "" {
		t.Error(diff)
	}
}

func assertError(t testing.TB, want, got error) {
	t.Helper()
	if !errors.Is(got, want) {
		t.Errorf("want error %s, got %s", want, got)
	}
}

func assertNoError(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
}
