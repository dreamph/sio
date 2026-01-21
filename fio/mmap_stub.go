//go:build !darwin && !linux && !freebsd && !netbsd && !openbsd

package fio

import "os"

func tryMmap(_ *os.File, _ int64) ([]byte, func() error, bool) {
	return nil, nil, false
}
