package secrets_test

import (
	"context"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/jeon-jihyeon/gatereach/internal/secrets"
)

// 환경변수를 바꾸므로 병렬 실행하지 않음
func TestResolve(t *testing.T) {
	t.Setenv("GATEREACH_TEST_PW", "s3cret")
	// security가 없는 Linux에서는 조회 자체가 실패
	missing := "Keychain 항목 없음"
	if _, err := exec.LookPath("security"); err != nil {
		missing = "Keychain 조회 실패"
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
		{"missing variable", args{context.Background(), "env:GATEREACH_TEST_MISSING"}, want{err: "환경변수 없음"}},
		{"missing keychain item", args{context.Background(), "keychain:gatereach-test-missing-item"}, want{err: missing}},
		{"cancelled keychain call", args{cancelled, "keychain:gatereach-test-missing-item"}, want{err: "Keychain 조회 실패"}},
		{"unknown reference", args{context.Background(), "file:/etc/passwd"}, want{err: "지원하지 않는 secret 참조"}},
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

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
