//go:build !unix

package store

import "io/fs"

// inodeOf has no portable implementation off unix; the checkpoint test skips.
func inodeOf(fs.FileInfo) (uint64, bool) { return 0, false }
