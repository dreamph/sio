//go:build darwin || linux || freebsd || netbsd || openbsd

package fio

import (
	"os"
	"syscall"
)

func tryMmap(f *os.File, size int64) ([]byte, func() error, bool) {
	if f == nil || size <= 0 {
		return nil, nil, false
	}
	maxInt := int64(int(^uint(0) >> 1))
	if size > maxInt {
		return nil, nil, false
	}
	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_PRIVATE)
	if err != nil {
		return nil, nil, false
	}
	cleanup := func() error { return syscall.Munmap(data) }
	return data, cleanup, true
}
