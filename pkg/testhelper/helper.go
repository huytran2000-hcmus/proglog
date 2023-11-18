package testhelper

import (
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func AssertEqual(t testing.TB, want, got any) {
	t.Helper()
	if diff := cmp.Diff(want, got, cmpopts.IgnoreUnexported()); diff != "" {
		t.Error(diff)
	}
}

func AssertNotEqual(t testing.TB, notWant, got any) {
	t.Helper()
	if cmp.Equal(notWant, got) {
		t.Errorf("not want %v, got %v", got, notWant)
	}
}

func AssertError(t testing.TB, want, got error) {
	t.Helper()
	if !errors.Is(got, want) {
		t.Errorf("want error %s, got %s", want, got)
	}
}

func AssertNoError(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
}

func RequireNoError(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}
