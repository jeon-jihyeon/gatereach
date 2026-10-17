package secrets_test

import (
	"context"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/jeon-jihyeon/gatereach/internal/secrets"
)

// Not parallel since it sets an environment variable
func TestResolve(t *testing.T) {
	t.Setenv("GATEREACH_TEST_PW", "s3cret")
	// The lookup itself fails on Linux where security doesn't exist
	missing := "no keychain item"
	if _, err := exec.LookPath("security"); err != nil {
		missing = "keychain lookup failed"
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	type args struct {
		ctx context.Context
		ref string
	}
	type want struct {
		value string
		err   string
	}
	tcs := []struct {
		name string
		args args
		want want
	}{
		{"empty means no password", args{context.Background(), ""}, want{}},
		{"environment variable", args{context.Background(), "env:GATEREACH_TEST_PW"}, want{value: "s3cret"}},
		{"missing variable", args{context.Background(), "env:GATEREACH_TEST_MISSING"}, want{err: "no environment variable"}},
		{"missing keychain item", args{context.Background(), "keychain:gatereach-test-missing-item"}, want{err: missing}},
		{"cancelled keychain call", args{cancelled, "keychain:gatereach-test-missing-item"},
			want{err: "keychain lookup failed"}},
		{"keychain without an item never runs security", args{context.Background(), "keychain:"},
			want{err: "secret reference without a name"}},
		{"env without a variable", args{context.Background(), "env:"}, want{err: "secret reference without a name"}},
		{"unknown reference", args{context.Background(), "file:/etc/passwd"},
			want{err: "unsupported secret reference\nuse keychain:<item> or env:GATEREACH_<name>"}},
		{"env outside the prefix is never read", args{context.Background(), "env:HOME"},
			want{err: "env secret must name a GATEREACH_ variable"}},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			got, err := secrets.Resolve(tc.args.ctx, tc.args.ref)
			assert.Equal(t, tc.want.value, got)
			assert.Equal(t, tc.want.err == "", err == nil, "%v", err)
			assert.Contains(t, errText(err), tc.want.err)
		})
	}
}

func TestCheck(t *testing.T) {
	t.Parallel()
	tcs := []struct {
		name string
		ref  string
		err  string
	}{
		{"empty means no password", "", ""},
		{"keychain item", "keychain:acme-db", ""},
		{"prefixed variable", "env:GATEREACH_ACME_PW", ""},
		{"variable outside the prefix", "env:AWS_SECRET_ACCESS_KEY",
			"env secret must name a GATEREACH_ variable\nuse keychain:<item> or env:GATEREACH_<name>"},
		{"keychain without an item", "keychain:",
			"secret reference without a name\nuse keychain:<item> or env:GATEREACH_<name>"},
		{"bare env", "env", "secret reference without a name\nuse keychain:<item> or env:GATEREACH_<name>"},
		{"a password in place of a reference is not repeated", "hunter2",
			"unsupported secret reference\nuse keychain:<item> or env:GATEREACH_<name>"},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.err, errText(secrets.Check(tc.ref)))
		})
	}
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
