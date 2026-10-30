//go:build unix

package store

import (
	"io/fs"
	"syscall"
)

// inodeOf reports a file's inode, which is how the checkpoint test tells a
// hardlink from a copy.
func inodeOf(info fs.FileInfo) (uint64, bool) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return uint64(st.Ino), true
}
