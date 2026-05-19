//go:build !windows

package run

import "syscall"

// newProcAttrs returns SysProcAttr settings appropriate for child-process
// supervision on POSIX. Setpgid: true puts the child in a new process
// group, so signals sent to that group don't propagate up to the wrapper
// — and the wrapper can kill the entire group cleanly on exit.
func newProcAttrs() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true}
}
