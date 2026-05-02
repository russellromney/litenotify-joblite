//go:build windows

package honker

import (
	"os"
	"syscall"
)

func statDevIno(info os.FileInfo) (uint64, uint64, error) {
	stat, ok := info.Sys().(*syscall.Win32FileAttributeData)
	if !ok {
		return 0, 0, nil
	}
	// Windows doesn't expose dev/ino directly in Win32FileAttributeData.
	// Use volume serial + file index from the handle, but that's
	// expensive. For now, return zeros — the identity check is a
	// best-effort safety net on Windows.
	_ = stat
	return 0, 0, nil
}
