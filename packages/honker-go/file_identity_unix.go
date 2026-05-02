//go:build !windows

package honker

import (
	"os"
	"syscall"
)

func statDevIno(info os.FileInfo) (uint64, uint64, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, nil
	}
	return uint64(stat.Dev), uint64(stat.Ino), nil
}
