package sio_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/dreamph/sio"
	"github.com/dreamph/sio/fio"
)

type sourceFactory struct {
	name    string
	makeFio func() fio.Source
	makeSio func() sio.StreamReader
	open    func() (io.ReadCloser, error)
	cleanup func()
}

func newSourceFactory(b *testing.B, kind string, data []byte) sourceFactory {
	b.Helper()

	switch kind {
	case "bytes":
		return sourceFactory{
			name:    "bytes",
			makeFio: func() fio.Source { return fio.BytesSource(data) },
			makeSio: func() sio.StreamReader { return sio.NewBytesReader(data) },
			open:    func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(data)), nil },
			cleanup: func() {
				// no-op
			},
		}
	case "file":
		dir := b.TempDir()
		path := filepath.Join(dir, "input.bin")
		if err := os.WriteFile(path, data, 0o644); err != nil {
			b.Fatalf("WriteFile: %v", err)
		}
		return sourceFactory{
			name:    "file",
			makeFio: func() fio.Source { return fio.PathSource(path) },
			makeSio: func() sio.StreamReader { return sio.NewFileReader(path) },
			open:    func() (io.ReadCloser, error) { return os.Open(path) },
			cleanup: func() {
				// temp dir cleanup handled by testing
			},
		}
	case "url":
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			_, _ = io.Copy(w, bytes.NewReader(data))
		}))
		return sourceFactory{
			name:    "url",
			makeFio: func() fio.Source { return fio.URLSource(srv.URL) },
			makeSio: func() sio.StreamReader { return sio.NewURLReader(srv.URL) },
			open: func() (io.ReadCloser, error) {
				resp, err := http.Get(srv.URL)
				if err != nil {
					return nil, err
				}
				return resp.Body, nil
			},
			cleanup: func() {
				srv.Close()
			},
		}
	default:
		b.Fatalf("unknown source kind: %s", kind)
	}
	return sourceFactory{}
}

type nopWriteCloser struct{ io.Writer }

func (n nopWriteCloser) Close() error { return nil }

func benchNormalCopy(b *testing.B, size int, storage string, src sourceFactory, opsPerSession int) {
	b.Helper()

	b.ReportAllocs()
	b.SetBytes(int64(size))
	b.ResetTimer()

	dir := ""
	if storage == "file" {
		dir = b.TempDir()
	}

	for i := 0; i < b.N; {
		for j := 0; j < opsPerSession && i < b.N; j++ {
			r, err := src.open()
			if err != nil {
				b.Fatalf("open: %v", err)
			}

			var (
				w       io.WriteCloser
				cleanup func()
			)
			switch storage {
			case "memory":
				w = nopWriteCloser{Writer: &bytes.Buffer{}}
			case "file":
				f, err := os.CreateTemp(dir, "normal-*")
				if err != nil {
					_ = r.Close()
					b.Fatalf("CreateTemp: %v", err)
				}
				cleanup = func() { _ = os.Remove(f.Name()) }
				w = f
			default:
				_ = r.Close()
				b.Fatalf("unknown storage: %s", storage)
			}

			_, err = io.Copy(w, r)
			_ = r.Close()
			_ = w.Close()
			if cleanup != nil {
				cleanup()
			}
			if err != nil {
				b.Fatalf("copy: %v", err)
			}
			i++
		}
	}
}

func benchFioDo(b *testing.B, size int, src sourceFactory, mgr fio.IoManager) {
	b.Helper()

	b.ReportAllocs()
	b.SetBytes(int64(size))
	b.ResetTimer()

	// Create session once and reuse
	ses, err := mgr.NewSession()
	if err != nil {
		b.Fatalf("NewSession: %v", err)
	}
	defer func() { _ = ses.Cleanup() }()

	ctx := fio.WithSession(context.Background(), ses)

	for i := 0; i < b.N; i++ {
		out, err := fio.DoOut(ctx, fio.Out(fio.Txt), func(ctx context.Context, s *fio.OutScope, w io.Writer) error {
			r, _ := s.UseSized(src.makeFio())
			_, err := io.Copy(w, r)
			return err
		})
		if err != nil {
			b.Fatalf("DoOut: %v", err)
		}
		_ = out
	}
}

func benchSioDo(b *testing.B, size int, src sourceFactory, mgr sio.IoManager) {
	b.Helper()

	b.ReportAllocs()
	b.SetBytes(int64(size))
	b.ResetTimer()

	// Create session once and reuse
	ses, err := mgr.NewSession()
	if err != nil {
		b.Fatalf("NewSession: %v", err)
	}
	defer func() { _ = ses.Cleanup() }()

	ctx := sio.WithSession(context.Background(), ses)

	for i := 0; i < b.N; i++ {
		out, err := sio.Process(ctx, src.makeSio(), sio.Out(sio.Txt), func(ctx context.Context, r io.Reader, w io.Writer) error {
			_, err := io.Copy(w, r)
			return err
		})
		if err != nil {
			b.Fatalf("Process: %v", err)
		}
		_ = out
	}
}

func BenchmarkCompareFioSio(b *testing.B) {
	opsPerSessionList := []int{1}
	sizes := []int{
		1 << 10,   // 1KB
		1 << 20,   // 1MB
		10 << 20,  // 10MB
		100 << 20, // 100MB
	}
	sourceKinds := []string{"bytes", "file"}
	storages := []struct {
		name string
		fio  fio.StorageType
		sio  sio.StorageType
	}{
		{name: "memory", fio: fio.Memory, sio: sio.Memory},
		{name: "file", fio: fio.File, sio: sio.File},
	}

	// Create fio managers once per storage type
	fioManagers := make(map[fio.StorageType]fio.IoManager)
	for _, storage := range storages {
		mgr, err := fio.NewIoManager(
			"",
			storage.fio,
			fio.WithMaxPreallocate(0),
			fio.WithSpillThreshold(0),
			fio.WithThreshold(0),
		)
		if err != nil {
			b.Fatalf("NewIoManager(fio): %v", err)
		}
		fioManagers[storage.fio] = mgr
		defer func(m fio.IoManager) { _ = m.Cleanup() }(mgr)
	}

	// Create sio managers once per storage type
	sioManagers := make(map[sio.StorageType]sio.IoManager)
	for _, storage := range storages {
		mgr, err := sio.NewIoManager("", storage.sio)
		if err != nil {
			b.Fatalf("NewIoManager(sio): %v", err)
		}
		sioManagers[storage.sio] = mgr
		defer func(m sio.IoManager) { _ = m.Cleanup() }(mgr)
	}

	for _, size := range sizes {
		data := bytes.Repeat([]byte{'a'}, size)
		sizeLabel := strconv.Itoa(size)
		for _, sourceKind := range sourceKinds {
			for _, storage := range storages {
				for _, opsPerSession := range opsPerSessionList {
					label := fmt.Sprintf("ops%d", opsPerSession)
					if sourceKind != "url" {
						b.Run("normal/"+sourceKind+"/storage-"+storage.name+"/"+sizeLabel+"/"+label, func(b *testing.B) {
							src := newSourceFactory(b, sourceKind, data)
							defer src.cleanup()
							benchNormalCopy(b, size, storage.name, src, opsPerSession)
						})
					}
					b.Run("fio/"+sourceKind+"/storage-"+storage.name+"/"+sizeLabel+"/"+label, func(b *testing.B) {
						src := newSourceFactory(b, sourceKind, data)
						defer src.cleanup()
						benchFioDo(b, size, src, fioManagers[storage.fio])
					})
					b.Run("sio/"+sourceKind+"/storage-"+storage.name+"/"+sizeLabel+"/"+label, func(b *testing.B) {
						src := newSourceFactory(b, sourceKind, data)
						defer src.cleanup()
						benchSioDo(b, size, src, sioManagers[storage.sio])
					})
				}
			}
		}
	}
}
