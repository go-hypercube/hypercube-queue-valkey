package valkeyqueue_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/valkey-io/valkey-go"
)

// miniredis does not support Lua shebang metadata. This fixture adapter strips
// only that header; it does not emulate script flags or memory-pressure behavior.
type miniredisClient struct {
	valkey.Client
}

var _ valkey.Client = (*miniredisClient)(nil)

func (c *miniredisClient) Do(ctx context.Context, command valkey.Completed) valkey.ValkeyResult {
	args := command.Commands()
	if len(args) > 1 && (strings.EqualFold(args[0], "EVAL") || strings.EqualFold(args[0], "EVAL_RO")) {
		// The Completed owns this slice. Replace only its script before Do
		// recycles it, preserving the original key routing and command flags.
		args[1] = miniredisLuaBody(args[1])
	}
	return c.Client.Do(ctx, command)
}

func miniredisLuaBody(script string) string {
	if strings.HasPrefix(script, "#!lua ") || strings.HasPrefix(script, "#!lua\n") {
		if _, body, ok := strings.Cut(script, "\n"); ok {
			return body
		}
	}
	return script
}

func TestMiniredisLuaBodyPreservesEverythingExceptLeadingHeader(t *testing.T) {
	body := "\n-- preserve whitespace and opaque bytes: \xff\x00\nreturn ARGV[1]\n"
	for _, tc := range []struct {
		name   string
		script string
		want   string
	}{
		{"plain", body, body},
		{"allow-oom", "#!lua flags=allow-oom\n" + body, body},
		{"other-metadata", "#!lua flags=no-writes,allow-oom\n" + body, body},
		{"bare-header", "#!lua\n" + body, body},
		{"empty-body", "#!lua flags=allow-oom\n", ""},
		{"unterminated-header", "#!lua flags=allow-oom", "#!lua flags=allow-oom"},
		{"not-leading", "\n#!lua flags=allow-oom\n" + body, "\n#!lua flags=allow-oom\n" + body},
		{"other-shebang", "#!/bin/sh\n" + body, "#!/bin/sh\n" + body},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, miniredisLuaBody(tc.script))
		})
	}
}

func TestMiniredisClientFlaggedScriptsPreserveNativeArgumentsAndFlags(t *testing.T) {
	f := newTestQueue(t)
	ctx := context.Background()
	key, arg := "key:{opaque}\xff\x00", "argument:\xfe\x00"
	body := "return {KEYS[1], ARGV[1], ARGV[2]}\n"
	for _, name := range []string{"EVAL", "EVAL_RO"} {
		t.Run(name, func(t *testing.T) {
			observer := &miniredisCommandObserver{Client: f.client}
			client := &miniredisClient{Client: observer}
			builder := f.client.B().Arbitrary(name).Args("#!lua flags=allow-oom\n"+body, "1").Keys(key).Args(arg, "last")
			var command valkey.Completed
			if name == "EVAL_RO" {
				command = builder.ReadOnly()
			} else {
				command = builder.Build()
			}
			readOnly, retryable := command.IsReadOnly(), command.IsRetryable()
			result, err := client.Do(ctx, command).AsStrSlice()
			if name == "EVAL_RO" {
				// This miniredis version lacks EVAL_RO too. The adapter must
				// preserve that command and surface its error, not turn it into
				// EVAL or pretend to emulate readonly-script semantics.
				require.ErrorContains(t, err, "unknown command")
			} else {
				require.NoError(t, err)
				require.Equal(t, []string{key, arg, "last"}, result)
			}
			require.Equal(t, []string{name, body, "1", key, arg, "last"}, observer.args)
			require.Equal(t, readOnly, observer.readOnly)
			require.Equal(t, retryable, observer.retryable)
		})
	}
}

type miniredisCommandObserver struct {
	valkey.Client
	args      []string
	readOnly  bool
	retryable bool
}

func (c *miniredisCommandObserver) Do(ctx context.Context, command valkey.Completed) valkey.ValkeyResult {
	c.args = append([]string(nil), command.Commands()...)
	c.readOnly, c.retryable = command.IsReadOnly(), command.IsRetryable()
	return c.Client.Do(ctx, command)
}
