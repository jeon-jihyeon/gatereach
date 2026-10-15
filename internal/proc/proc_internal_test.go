package proc

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// SIGTERM을 무시하는 자식과 그 손자도 유예 뒤 SIGKILL로 끝나 시간 초과가 멈추지 않음
func TestSpawnKillsAChildThatIgnoresSIGTERM(t *testing.T) {
	t.Parallel()
	log := filepath.Join(t.TempDir(), "logs", "a.log")
	start := time.Now()
	err := spawn([]string{"sh", "-c", `trap "" TERM; sleep 30`}, os.Environ(), log, func() bool { return false },
		200*time.Millisecond, 20*time.Millisecond, 200*time.Millisecond)
	assert.ErrorIs(t, err, ErrTimeout)
	assert.Less(t, time.Since(start), 5*time.Second)
}
