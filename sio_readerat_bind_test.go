package sio

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// helper: create temp dir + register t.Cleanup
func newTempDir(t *testing.T) string {
	t.Helper()

	dir, err := os.MkdirTemp("", "sio-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	t.Cleanup(func() {
		_ = os.RemoveAll(dir)
	})
	return dir
}

// helper: write file with given content, return full path
func writeTempFile(t *testing.T, dir, name string, data []byte) string {
	t.Helper()

	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("failed to mkdir %s: %v", dir, err)
	}

	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("failed to write file %s: %v", path, err)
	}
	return path
}

// Test ToReaderAt + BindProcessResult + AsReaderAt using real files
func TestToReaderAt_WithBindProcessResult_AndFile(t *testing.T) {
	ctx := context.Background()

	// ---------- Prepare manager + session ----------
	baseDir := newTempDir(t)

	mgr, err := NewIoManager(baseDir, Memory)
	if err != nil {
		t.Fatalf("NewIoManager error: %v", err)
	}
	t.Cleanup(func() {
		_ = mgr.Cleanup()
	})

	ses, err := mgr.NewSession()
	if err != nil {
		t.Fatalf("NewSession error: %v", err)
	}
	t.Cleanup(func() {
		_ = ses.Cleanup()
	})

	ctx = WithSession(ctx, ses)

	// ---------- Prepare input file ----------
	inputData := bytes.Repeat([]byte("HELLO-READERAT-"), 1024) // ~14KB
	sr := NewBytesReader(inputData)

	// ---------- BindProcessResult: read from file, write to output ----------
	out, result, err := BindProcessResult[int](ctx, Out(".bin"), func(
		ctx context.Context,
		b *Binder,
		w io.Writer,
	) (*int, error) {
		// 1) open source file via Binder
		r, err := b.Use(sr)
		if err != nil {
			return nil, err
		}

		// 2) use ToReaderAt on input (read-only random access)
		raRes, err := ToReaderAt(ctx, r) // 8MB limit
		if err != nil {
			return nil, err
		}
		defer func() { _ = raRes.Cleanup() }()

		// ✅ CHECK SIZE: input must be known and correct
		if raRes.Size() != int64(len(inputData)) {
			t.Fatalf("unexpected input size, got=%d want=%d", raRes.Size(), len(inputData))
		}

		// small sanity read from input via ReaderAt
		tmp := make([]byte, 5)
		if _, err := raRes.ReaderAt().ReadAt(tmp, 0); err != nil {
			return nil, err
		}
		if string(tmp) != "HELLO" {
			t.Fatalf("unexpected prefix from ReaderAt: %q", string(tmp))
		}

		// 3) write whole input to output
		n, err := io.Copy(w, bytes.NewReader(inputData))
		if err != nil {
			return nil, err
		}
		v := int(n)
		return &v, nil
	})
	if err != nil {
		t.Fatalf("BindProcessResult error: %v", err)
	}

	// result = bytes written
	if result == nil {
		t.Fatalf("result is nil")
	}
	if got, want := *result, len(inputData); got != want {
		t.Fatalf("unexpected result bytes, got=%d want=%d", got, want)
	}

	// ---------- Use AsReaderAt on the output ----------
	raOut, err := out.AsReaderAt(ctx, WithMaxMemoryBytes(4<<10)) // 4KB → force tempFile for this size
	if err != nil {
		t.Fatalf("AsReaderAt error: %v", err)
	}
	defer func() {
		if err := raOut.Cleanup(); err != nil {
			t.Fatalf("cleanup error: %v", err)
		}
	}()

	// ✅ CHECK SIZE: output must be known and correct
	if raOut.Size() != int64(len(inputData)) {
		t.Fatalf("unexpected output size, got=%d want=%d", raOut.Size(), len(inputData))
	}

	if raOut.Source() != "tempFile" && raOut.Source() != "memory" && raOut.Source() != "direct" {
		t.Fatalf("unexpected Source: %q", raOut.Source())
	}

	// check first bytes of output via ReaderAt
	buf := make([]byte, 5)
	if _, err := raOut.ReaderAt().ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt error: %v", err)
	}
	if string(buf) != "HELLO" {
		t.Fatalf("unexpected output prefix: %q", string(buf))
	}
}

// Extra: Validate SizeFromReader works with readerAtReadCloser wrapper
func TestSizeFromReader_UnwrapReaderAtReadCloser(t *testing.T) {
	data := bytes.Repeat([]byte("SIZE-CHECK-"), 1234)
	br := bytes.NewReader(data)

	// Wrap using NopCloser to get readerAtReadCloser (preserves ReaderAt)
	rc := NopCloser(br)

	got := SizeFromReader(rc)
	want := int64(len(data))
	if got != want {
		t.Fatalf("SizeFromReader mismatch, got=%d want=%d", got, want)
	}
}

// Extra: Validate SizeFromReader for *os.File
func TestSizeFromReader_File(t *testing.T) {
	dir := newTempDir(t)
	data := bytes.Repeat([]byte("FILE-SIZE-"), 2048)
	path := writeTempFile(t, dir, "a.bin", data)

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	defer f.Close()

	got := SizeFromReader(f)
	want := int64(len(data))
	if got != want {
		t.Fatalf("SizeFromReader(file) mismatch, got=%d want=%d", got, want)
	}
}
