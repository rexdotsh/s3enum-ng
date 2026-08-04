package main

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

func TestProduceCandidatesMatchesS3enum(t *testing.T) {
	var got []string
	err := produceCandidates(context.Background(), []string{"foo"}, strings.NewReader("bar\n"), []string{"baz"},
		func(_ context.Context, candidate string) error {
			got = append(got, candidate)
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}

	want := []string{
		"foo",
		"foo-bar", "bar-foo", "foo-bar-baz", "baz-foo-bar", "bar-foo-baz", "baz-bar-foo",
		"foo_bar", "bar_foo", "foo_bar_baz", "baz_foo_bar", "bar_foo_baz", "baz_bar_foo",
		"foo.bar", "bar.foo", "foo.bar.baz", "baz.foo.bar", "bar.foo.baz", "baz.bar.foo",
		"foobar", "barfoo", "foobarbaz", "bazfoobar", "barfoobaz", "bazbarfoo",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("candidate mismatch\n got: %q\nwant: %q", got, want)
	}
}

func TestProduceCandidatesKeepsDuplicates(t *testing.T) {
	count := 0
	err := produceCandidates(context.Background(), []string{"same"}, strings.NewReader("same\n"), nil,
		func(_ context.Context, _ string) error {
			count++
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if count != 9 {
		t.Fatalf("got %d candidates, want 9", count)
	}
}
