//go:build windows

package run

import "syscall"

// newProcAttrs returns SysProcAttr settings appropriate for child-process
// supervision on Windows. Job-object based group management is a
// follow-up — for the initial cut we run with no special attrs and rely
// on the parent terminating its children explicitly.
func newProcAttrs() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{}
}
