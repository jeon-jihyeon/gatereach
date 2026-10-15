package cli

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

// 실행 결과를 거치지 않는 종료 코드 규칙은 공개 API로 만들 수 없는 중단 상태가 필요해 내부에서 검증
func TestExitFor(t *testing.T) {
	t.Parallel()
	closeErr := errors.New("디스크 가득 참")
	type args struct {
		follow      bool
		interrupted bool
		err         error
	}
	tcs := []struct {
		name string
		args args
		want error
	}{
		{"an interrupted single read exits 130", args{false, true, nil}, exitCode{code: exitInterrupted, msg: "중단됨"}},
		{"a sink close error after an interrupt exits 1", args{false, true, closeErr}, closeErr},
		{"an interrupted follow ends normally", args{true, true, nil}, nil},
		{"a failed run keeps its error", args{false, false, closeErr}, closeErr},
		{"a finished run exits 0", args{false, false, nil}, nil},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, exitFor(queryOptions{follow: tc.args.follow}, tc.args.interrupted, tc.args.err))
		})
	}
}
