package sio

import (
	"bytes"
	"context"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStorageTypeString(t *testing.T) {
	tests := []struct {
		st   StorageType
		want string
	}{
		{StorageFile, "file"},
		{StorageMemory, "memory"},
		{File, "file"},
		{Memory, "memory"},
	}

	for _, tt := range tests {
		if got := tt.st.String(); got != tt.want {
			t.Errorf("StorageType(%d).String() = %q, want %q", tt.st, got, tt.want)
		}
	}
}

func TestStorageFunction(t *testing.T) {
	tests := []struct {
		input string
		want  StorageType
	}{
		{"file", StorageFile},
		{"FILE", StorageFile},
		{"memory", StorageMemory},
		{"MEMORY", StorageMemory},
		{"  memory  ", StorageMemory},
		{"unknown", StorageFile}, // default
		{"", StorageFile},        // default
	}

	for _, tt := range tests {
		if got := Storage(tt.input); got != tt.want {
			t.Errorf("Storage(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestOutOption(t *testing.T) {
	t.Run("without storage type", func(t *testing.T) {
		opt := Out(".pdf")
		if opt.Ext != ".pdf" {
			t.Errorf("Ext = %q, want %q", opt.Ext, ".pdf")
		}
		if opt.StorageType != nil {
			t.Error("StorageType should be nil")
		}
		// Should use session default
		if got := opt.getStorageType(StorageFile); got != StorageFile {
			t.Errorf("getStorageType(File) = %v, want File", got)
		}
		if got := opt.getStorageType(StorageMemory); got != StorageMemory {
			t.Errorf("getStorageType(Bytes) = %v, want Bytes", got)
		}
	})

	t.Run("with memory storage", func(t *testing.T) {
		opt := Out(".pdf", Memory)
		if opt.Ext != ".pdf" {
			t.Errorf("Ext = %q, want %q", opt.Ext, ".pdf")
		}
		if opt.StorageType == nil {
			t.Fatal("StorageType should not be nil")
		}
		if *opt.StorageType != StorageMemory {
			t.Errorf("StorageType = %v, want Bytes", *opt.StorageType)
		}
		// Should override session default
		if got := opt.getStorageType(StorageFile); got != StorageMemory {
			t.Errorf("getStorageType should return Bytes, got %v", got)
		}
	})

	t.Run("with file storage", func(t *testing.T) {
		opt := Out(".txt", File)
		if *opt.StorageType != StorageFile {
			t.Errorf("StorageType = %v, want File", *opt.StorageType)
		}
	})

	t.Run("with Storage() string conversion", func(t *testing.T) {
		opt := Out(".pdf", Storage("memory"))
		if *opt.StorageType != StorageMemory {
			t.Errorf("StorageType = %v, want Bytes", *opt.StorageType)
		}
	})
}

func TestManagerWithStorageFile(t *testing.T) {
	tmpDir := t.TempDir()

	mgr, err := NewIoManager(tmpDir, StorageFile)
	if err != nil {
		t.Fatalf("NewIoManager: %v", err)
	}
	defer mgr.Cleanup()

	ses, err := mgr.NewSession()
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer ses.Cleanup()

	ctx := WithSession(context.Background(), ses)

	// Process with default (file) storage
	src := NewBytesReader([]byte("hello world"))
	out, err := Process(ctx, src, Out(".txt"), func(ctx context.Context, r io.Reader, w io.Writer) error {
		_, err := io.Copy(w, r)
		return err
	})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	if out.StorageType() != StorageFile {
		t.Errorf("StorageType = %v, want File", out.StorageType())
	}

	if out.Path() == "" {
		t.Error("Path should not be empty for file storage")
	}

	// Verify file exists
	if _, err := os.Stat(out.Path()); err != nil {
		t.Errorf("output file should exist: %v", err)
	}

	// Read back
	data, err := out.Bytes()
	if err != nil {
		t.Fatalf("Bytes: %v", err)
	}
	if string(data) != "hello world" {
		t.Errorf("got %q, want %q", string(data), "hello world")
	}
}

func TestManagerWithStorageBytes(t *testing.T) {
	mgr, err := NewIoManager("", StorageMemory)
	if err != nil {
		t.Fatalf("NewIoManager: %v", err)
	}
	defer mgr.Cleanup()

	ses, err := mgr.NewSession()
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer ses.Cleanup()

	ctx := WithSession(context.Background(), ses)

	// Process with default (bytes) storage
	src := NewBytesReader([]byte("hello memory"))
	out, err := Process(ctx, src, Out(".txt"), func(ctx context.Context, r io.Reader, w io.Writer) error {
		_, err := io.Copy(w, r)
		return err
	})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	if out.StorageType() != StorageMemory {
		t.Errorf("StorageType = %v, want Bytes", out.StorageType())
	}

	if out.Path() != "" {
		t.Errorf("Path should be empty for bytes storage, got %q", out.Path())
	}

	// Use Data() for zero-copy access
	data := out.Data()
	if string(data) != "hello memory" {
		t.Errorf("Data() = %q, want %q", string(data), "hello memory")
	}

	// Also test Bytes()
	data2, err := out.Bytes()
	if err != nil {
		t.Fatalf("Bytes: %v", err)
	}
	if string(data2) != "hello memory" {
		t.Errorf("Bytes() = %q, want %q", string(data2), "hello memory")
	}
}

func TestMixedStorageInSession(t *testing.T) {
	tmpDir := t.TempDir()

	// Create manager with file storage (has directory)
	mgr, err := NewIoManager(tmpDir, StorageFile)
	if err != nil {
		t.Fatalf("NewIoManager: %v", err)
	}
	defer mgr.Cleanup()

	ses, err := mgr.NewSession()
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer ses.Cleanup()

	ctx := WithSession(context.Background(), ses)

	// First output: use memory
	src1 := NewBytesReader([]byte("memory data"))
	out1, err := Process(ctx, src1, Out(".txt", Memory), func(ctx context.Context, r io.Reader, w io.Writer) error {
		_, err := io.Copy(w, r)
		return err
	})
	if err != nil {
		t.Fatalf("Process (mem): %v", err)
	}
	if out1.StorageType() != StorageMemory {
		t.Errorf("out1 StorageType = %v, want Bytes", out1.StorageType())
	}

	// Second output: use file (explicit)
	src2 := NewBytesReader([]byte("file data"))
	out2, err := Process(ctx, src2, Out(".txt", File), func(ctx context.Context, r io.Reader, w io.Writer) error {
		_, err := io.Copy(w, r)
		return err
	})
	if err != nil {
		t.Fatalf("Process (file): %v", err)
	}
	if out2.StorageType() != StorageFile {
		t.Errorf("out2 StorageType = %v, want File", out2.StorageType())
	}

	// Third output: use session default (file)
	src3 := NewBytesReader([]byte("default data"))
	out3, err := Process(ctx, src3, Out(".txt"), func(ctx context.Context, r io.Reader, w io.Writer) error {
		_, err := io.Copy(w, r)
		return err
	})
	if err != nil {
		t.Fatalf("Process (default): %v", err)
	}
	if out3.StorageType() != StorageFile {
		t.Errorf("out3 StorageType = %v, want File", out3.StorageType())
	}

	// Verify all outputs
	d1, _ := out1.Bytes()
	d2, _ := out2.Bytes()
	d3, _ := out3.Bytes()

	if string(d1) != "memory data" {
		t.Errorf("out1 = %q", string(d1))
	}
	if string(d2) != "file data" {
		t.Errorf("out2 = %q", string(d2))
	}
	if string(d3) != "default data" {
		t.Errorf("out3 = %q", string(d3))
	}
}

func TestFileStorageUnavailableError(t *testing.T) {
	// Create manager with bytes-only storage (no directory)
	mgr, err := NewIoManager("", StorageMemory)
	if err != nil {
		t.Fatalf("NewIoManager: %v", err)
	}
	defer mgr.Cleanup()

	ses, err := mgr.NewSession()
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer ses.Cleanup()

	ctx := WithSession(context.Background(), ses)

	// Try to force file storage - should fail
	src := NewBytesReader([]byte("test"))
	_, err = Process(ctx, src, Out(".txt", File), func(ctx context.Context, r io.Reader, w io.Writer) error {
		_, err := io.Copy(w, r)
		return err
	})

	if !errors.Is(err, ErrFileStorageUnavailable) {
		t.Errorf("expected ErrFileStorageUnavailable, got %v", err)
	}
}

func TestOutputReader(t *testing.T) {
	t.Run("bytes storage returns BytesReader", func(t *testing.T) {
		mgr, _ := NewIoManager("", StorageMemory)
		defer mgr.Cleanup()
		ses, _ := mgr.NewSession()
		defer ses.Cleanup()
		ctx := WithSession(context.Background(), ses)

		src := NewBytesReader([]byte("test"))
		out, err := Process(ctx, src, Out(".txt"), func(ctx context.Context, r io.Reader, w io.Writer) error {
			_, err := io.Copy(w, r)
			return err
		})
		if err != nil {
			t.Fatalf("Process: %v", err)
		}

		reader := out.Reader()
		if _, ok := reader.(*BytesReader); !ok {
			t.Errorf("Reader() returned %T, want *BytesReader", reader)
		}
	})

	t.Run("file storage returns FileReader", func(t *testing.T) {
		tmpDir := t.TempDir()
		mgr, _ := NewIoManager(tmpDir, StorageFile)
		defer mgr.Cleanup()
		ses, _ := mgr.NewSession()
		defer ses.Cleanup()
		ctx := WithSession(context.Background(), ses)

		src := NewBytesReader([]byte("test"))
		out, err := Process(ctx, src, Out(".txt"), func(ctx context.Context, r io.Reader, w io.Writer) error {
			_, err := io.Copy(w, r)
			return err
		})
		if err != nil {
			t.Fatalf("Process: %v", err)
		}

		reader := out.Reader()
		if _, ok := reader.(*FileReader); !ok {
			t.Errorf("Reader() returned %T, want *FileReader", reader)
		}
	})
}

func TestChainedProcessing(t *testing.T) {
	mgr, _ := NewIoManager("", StorageMemory)
	defer mgr.Cleanup()
	ses, _ := mgr.NewSession()
	defer ses.Cleanup()
	ctx := WithSession(context.Background(), ses)

	// First process: write "hello"
	src := NewBytesReader([]byte("hello"))
	out1, err := Process(ctx, src, Out(".txt"), func(ctx context.Context, r io.Reader, w io.Writer) error {
		_, err := io.Copy(w, r)
		return err
	})
	if err != nil {
		t.Fatalf("Process 1: %v", err)
	}

	// Chain: transform to uppercase
	out2, err := Process(ctx, out1.Reader(), Out(".txt"), func(ctx context.Context, r io.Reader, w io.Writer) error {
		data, _ := io.ReadAll(r)
		_, err := w.Write([]byte(strings.ToUpper(string(data))))
		return err
	})
	if err != nil {
		t.Fatalf("Process 2: %v", err)
	}

	data, _ := out2.Bytes()
	if string(data) != "HELLO" {
		t.Errorf("got %q, want %q", string(data), "HELLO")
	}
}

func TestKeepFlag(t *testing.T) {
	t.Run("bytes storage respects keep", func(t *testing.T) {
		mgr, _ := NewIoManager("", StorageMemory)
		defer mgr.Cleanup()
		ses, _ := mgr.NewSession()
		ctx := WithSession(context.Background(), ses)

		src := NewBytesReader([]byte("keep me"))
		out, _ := Process(ctx, src, Out(".txt"), func(ctx context.Context, r io.Reader, w io.Writer) error {
			_, err := io.Copy(w, r)
			return err
		})

		out.Keep()
		ses.Cleanup()

		// Data should still be accessible
		data := out.Data()
		if string(data) != "keep me" {
			t.Errorf("Data after cleanup = %q, want %q", string(data), "keep me")
		}
	})

	t.Run("file storage respects keep", func(t *testing.T) {
		tmpDir := t.TempDir()
		mgr, _ := NewIoManager(tmpDir, StorageFile)
		defer mgr.Cleanup()
		ses, _ := mgr.NewSession()
		ctx := WithSession(context.Background(), ses)

		src := NewBytesReader([]byte("keep me"))
		out, _ := Process(ctx, src, Out(".txt"), func(ctx context.Context, r io.Reader, w io.Writer) error {
			_, err := io.Copy(w, r)
			return err
		})

		path := out.Path()
		out.Keep()
		ses.Cleanup()

		// File should still exist
		if _, err := os.Stat(path); err != nil {
			t.Errorf("kept file should exist: %v", err)
		}
	})
}

func TestSaveAs(t *testing.T) {
	mgr, _ := NewIoManager("", StorageMemory)
	defer mgr.Cleanup()
	ses, _ := mgr.NewSession()
	defer ses.Cleanup()
	ctx := WithSession(context.Background(), ses)

	src := NewBytesReader([]byte("save me"))
	out, _ := Process(ctx, src, Out(".txt"), func(ctx context.Context, r io.Reader, w io.Writer) error {
		_, err := io.Copy(w, r)
		return err
	})

	savePath := filepath.Join(t.TempDir(), "saved.txt")
	if err := out.SaveAs(savePath); err != nil {
		t.Fatalf("SaveAs: %v", err)
	}

	data, err := os.ReadFile(savePath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(data) != "save me" {
		t.Errorf("saved data = %q, want %q", string(data), "save me")
	}
}

func TestReadAndReadList(t *testing.T) {
	mgr, _ := NewIoManager("", StorageMemory)
	defer mgr.Cleanup()
	ses, _ := mgr.NewSession()
	defer ses.Cleanup()
	ctx := WithSession(context.Background(), ses)

	t.Run("Read", func(t *testing.T) {
		src := NewBytesReader([]byte("read test"))
		var result string
		err := Read(ctx, src, func(ctx context.Context, r io.Reader) error {
			data, _ := io.ReadAll(r)
			result = string(data)
			return nil
		})
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if result != "read test" {
			t.Errorf("result = %q, want %q", result, "read test")
		}
	})

	t.Run("ReadList", func(t *testing.T) {
		sources := []StreamReader{
			NewBytesReader([]byte("one")),
			NewBytesReader([]byte("two")),
			NewBytesReader([]byte("three")),
		}
		var results []string
		err := ReadList(ctx, sources, func(ctx context.Context, readers []io.Reader) error {
			for _, r := range readers {
				data, _ := io.ReadAll(r)
				results = append(results, string(data))
			}
			return nil
		})
		if err != nil {
			t.Fatalf("ReadList: %v", err)
		}
		if len(results) != 3 {
			t.Errorf("got %d results, want 3", len(results))
		}
	})
}

func TestProcessList(t *testing.T) {
	mgr, _ := NewIoManager("", StorageMemory)
	defer mgr.Cleanup()
	ses, _ := mgr.NewSession()
	defer ses.Cleanup()
	ctx := WithSession(context.Background(), ses)

	sources := []StreamReader{
		NewBytesReader([]byte("hello ")),
		NewBytesReader([]byte("world")),
	}

	out, err := ProcessList(ctx, sources, Out(".txt", Memory), func(ctx context.Context, readers []io.Reader, w io.Writer) error {
		for _, r := range readers {
			if _, err := io.Copy(w, r); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ProcessList: %v", err)
	}

	data, _ := out.Bytes()
	if string(data) != "hello world" {
		t.Errorf("got %q, want %q", string(data), "hello world")
	}
}

func TestNoSessionError(t *testing.T) {
	ctx := context.Background() // no session

	src := NewBytesReader([]byte("test"))
	_, err := Process(ctx, src, Out(".txt"), func(ctx context.Context, r io.Reader, w io.Writer) error {
		return nil
	})

	if !errors.Is(err, ErrNoSession) {
		t.Errorf("expected ErrNoSession, got %v", err)
	}
}

func TestNilSourceError(t *testing.T) {
	mgr, _ := NewIoManager("", StorageMemory)
	defer mgr.Cleanup()
	ses, _ := mgr.NewSession()
	defer ses.Cleanup()
	ctx := WithSession(context.Background(), ses)

	_, err := Process(ctx, nil, Out(".txt"), func(ctx context.Context, r io.Reader, w io.Writer) error {
		return nil
	})

	if !errors.Is(err, ErrNilSource) {
		t.Errorf("expected ErrNilSource, got %v", err)
	}
}

func TestReadWithoutSession(t *testing.T) {
	// Read should work without session (falls back to direct streaming)
	ctx := context.Background()

	src := NewBytesReader([]byte("direct read"))
	var result string
	err := Read(ctx, src, func(ctx context.Context, r io.Reader) error {
		data, _ := io.ReadAll(r)
		result = string(data)
		return nil
	})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if result != "direct read" {
		t.Errorf("result = %q, want %q", result, "direct read")
	}
}

func TestMultipleOutputs(t *testing.T) {
	mgr, _ := NewIoManager("", StorageMemory)
	defer mgr.Cleanup()
	ses, _ := mgr.NewSession()
	defer ses.Cleanup()
	ctx := WithSession(context.Background(), ses)

	var outputs []*Output
	for i := 0; i < 5; i++ {
		src := NewBytesReader([]byte("output"))
		out, err := Process(ctx, src, Out(".txt"), func(ctx context.Context, r io.Reader, w io.Writer) error {
			_, err := io.Copy(w, r)
			return err
		})
		if err != nil {
			t.Fatalf("Process %d: %v", i, err)
		}
		outputs = append(outputs, out)
	}

	// Verify all outputs
	for i, out := range outputs {
		data, _ := out.Bytes()
		if string(data) != "output" {
			t.Errorf("output %d = %q, want %q", i, string(data), "output")
		}
	}
}

func TestLargeData(t *testing.T) {
	mgr, _ := NewIoManager("", StorageMemory)
	defer mgr.Cleanup()
	ses, _ := mgr.NewSession()
	defer ses.Cleanup()
	ctx := WithSession(context.Background(), ses)

	// 1MB of data
	largeData := bytes.Repeat([]byte("x"), 1024*1024)
	src := NewBytesReader(largeData)

	out, err := Process(ctx, src, Out(".bin"), func(ctx context.Context, r io.Reader, w io.Writer) error {
		_, err := io.Copy(w, r)
		return err
	})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	data, _ := out.Bytes()
	if len(data) != len(largeData) {
		t.Errorf("len = %d, want %d", len(data), len(largeData))
	}
}

func TestReadResult(t *testing.T) {
	ctx := context.Background()

	src := NewBytesReader([]byte("42"))
	result, err := ReadResult[int](ctx, src, func(ctx context.Context, r io.Reader) (*int, error) {
		data, _ := io.ReadAll(r)
		var n int
		_, err := io.ReadFull(bytes.NewReader(data), []byte{})
		if err != nil && err != io.EOF {
			return nil, err
		}
		n = 42 // simplified
		return &n, nil
	})
	if err != nil {
		t.Fatalf("ReadResult: %v", err)
	}
	if *result != 42 {
		t.Errorf("result = %d, want 42", *result)
	}
}

func TestProcessResult(t *testing.T) {
	mgr, _ := NewIoManager("", StorageMemory)
	defer mgr.Cleanup()
	ses, _ := mgr.NewSession()
	defer ses.Cleanup()
	ctx := WithSession(context.Background(), ses)

	src := NewBytesReader([]byte("test data"))
	out, result, err := ProcessResult[int](ctx, src, Out(".txt"), func(ctx context.Context, r io.Reader, w io.Writer) (*int, error) {
		data, _ := io.ReadAll(r)
		w.Write(data)
		n := len(data)
		return &n, nil
	})
	if err != nil {
		t.Fatalf("ProcessResult: %v", err)
	}
	if *result != 9 {
		t.Errorf("result = %d, want 9", *result)
	}

	outData, _ := out.Bytes()
	if string(outData) != "test data" {
		t.Errorf("output = %q, want %q", string(outData), "test data")
	}
}

func TestToOutput(t *testing.T) {
	mgr, _ := NewIoManager("", StorageMemory)
	defer mgr.Cleanup()
	ses, _ := mgr.NewSession()
	defer ses.Cleanup()
	ctx := WithSession(context.Background(), ses)

	src := NewBytesReader([]byte("copy me"))
	out, err := ToOutput(ctx, src, Out(".txt"))
	if err != nil {
		t.Fatalf("ToOutput: %v", err)
	}

	data, _ := out.Bytes()
	if string(data) != "copy me" {
		t.Errorf("got %q, want %q", string(data), "copy me")
	}
}

func TestCopyOutputTo(t *testing.T) {
	mgr, _ := NewIoManager("", StorageMemory)
	defer mgr.Cleanup()
	ses, _ := mgr.NewSession()
	defer ses.Cleanup()
	ctx := WithSession(context.Background(), ses)

	src := NewBytesReader([]byte("copy to writer"))
	out, _ := Process(ctx, src, Out(".txt"), func(ctx context.Context, r io.Reader, w io.Writer) error {
		_, err := io.Copy(w, r)
		return err
	})

	var buf bytes.Buffer
	n, err := out.WriteTo(&buf)
	if err != nil {
		t.Fatalf("CopyOutputTo: %v", err)
	}
	if n != 14 {
		t.Errorf("n = %d, want 14", n)
	}
	if buf.String() != "copy to writer" {
		t.Errorf("got %q, want %q", buf.String(), "copy to writer")
	}
}

func TestWriteFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.txt")

	r := strings.NewReader("write to file")
	n, err := WriteFile(r, path)
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if n != 13 {
		t.Errorf("n = %d, want 13", n)
	}

	data, _ := os.ReadFile(path)
	if string(data) != "write to file" {
		t.Errorf("got %q, want %q", string(data), "write to file")
	}
}

func TestWriteStreamToFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stream.txt")

	src := NewBytesReader([]byte("stream to file"))
	n, err := WriteStreamToFile(src, path)
	if err != nil {
		t.Fatalf("WriteStreamToFile: %v", err)
	}
	if n != 14 {
		t.Errorf("n = %d, want 14", n)
	}

	data, _ := os.ReadFile(path)
	if string(data) != "stream to file" {
		t.Errorf("got %q, want %q", string(data), "stream to file")
	}
}

func TestReadLines(t *testing.T) {
	ctx := context.Background()

	content := "line1\nline2\nline3"
	src := NewBytesReader([]byte(content))

	var lines []string
	err := ReadLines(ctx, src, func(line string) error {
		lines = append(lines, line)
		return nil
	})
	if err != nil {
		t.Fatalf("ReadLines: %v", err)
	}

	if len(lines) != 3 {
		t.Errorf("got %d lines, want 3", len(lines))
	}
	if lines[0] != "line1" || lines[1] != "line2" || lines[2] != "line3" {
		t.Errorf("lines = %v", lines)
	}
}

func TestSessionCleanup(t *testing.T) {
	t.Run("cleans up bytes storage", func(t *testing.T) {
		mgr, _ := NewIoManager("", StorageMemory)
		defer mgr.Cleanup()
		ses, _ := mgr.NewSession()
		ctx := WithSession(context.Background(), ses)

		src := NewBytesReader([]byte("cleanup test"))
		out, _ := Process(ctx, src, Out(".txt"), func(ctx context.Context, r io.Reader, w io.Writer) error {
			_, err := io.Copy(w, r)
			return err
		})

		// Cleanup
		ses.Cleanup()

		// Data should be nil after cleanup
		if out.Data() != nil {
			t.Error("data should be nil after cleanup")
		}
	})

	t.Run("cleans up file storage", func(t *testing.T) {
		tmpDir := t.TempDir()
		mgr, _ := NewIoManager(tmpDir, StorageFile)
		defer mgr.Cleanup()
		ses, _ := mgr.NewSession()
		ctx := WithSession(context.Background(), ses)

		src := NewBytesReader([]byte("cleanup test"))
		out, _ := Process(ctx, src, Out(".txt"), func(ctx context.Context, r io.Reader, w io.Writer) error {
			_, err := io.Copy(w, r)
			return err
		})

		path := out.Path()

		// Cleanup
		ses.Cleanup()

		// File should not exist after cleanup
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Error("file should not exist after cleanup")
		}
	})
}

func TestFileReaderCleanup(t *testing.T) {
	// Create a temp file
	tmpFile := filepath.Join(t.TempDir(), "test.txt")
	os.WriteFile(tmpFile, []byte("test"), 0644)

	fr := NewFileReader(tmpFile)

	// FileReader.Cleanup does NOT delete the file
	fr.Cleanup()

	if _, err := os.Stat(tmpFile); err != nil {
		t.Error("FileReader.Cleanup should not delete the file")
	}
}

func TestBytesReaderCleanup(t *testing.T) {
	data := []byte("test data")
	br := NewBytesReader(data)

	br.Cleanup()

	if br.Data != nil {
		t.Error("BytesReader.Cleanup should set Data to nil")
	}
}

func TestIOReader(t *testing.T) {
	t.Run("wraps io.Reader", func(t *testing.T) {
		r := strings.NewReader("io reader test")
		ior := NewGenericReader(r)

		rc, err := ior.Open()
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer rc.Close()

		data, _ := io.ReadAll(rc)
		if string(data) != "io reader test" {
			t.Errorf("got %q, want %q", string(data), "io reader test")
		}
	})

	t.Run("wraps io.ReadCloser", func(t *testing.T) {
		rc := io.NopCloser(strings.NewReader("read closer test"))
		ior := NewGenericReader(rc)

		rc2, err := ior.Open()
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer rc2.Close()

		data, _ := io.ReadAll(rc2)
		if string(data) != "read closer test" {
			t.Errorf("got %q, want %q", string(data), "read closer test")
		}
	})

	t.Run("nil reader error", func(t *testing.T) {
		ior := NewGenericReader(nil)
		_, err := ior.Open()
		if err == nil {
			t.Error("expected error for nil reader")
		}
	})
}

func TestManagerClosed(t *testing.T) {
	mgr, _ := NewIoManager("", StorageMemory)
	mgr.Cleanup()

	_, err := mgr.NewSession()
	if !errors.Is(err, ErrIoManagerClosed) {
		t.Errorf("expected ErrIoManagerClosed, got %v", err)
	}
}

func TestSessionClosed(t *testing.T) {
	mgr, _ := NewIoManager("", StorageMemory)
	defer mgr.Cleanup()
	ses, _ := mgr.NewSession()
	ses.Cleanup()

	ctx := WithSession(context.Background(), ses)
	src := NewBytesReader([]byte("test"))

	_, err := Process(ctx, src, Out(".txt"), func(ctx context.Context, r io.Reader, w io.Writer) error {
		return nil
	})
	if !errors.Is(err, ErrIoSessionClosed) {
		t.Errorf("expected ErrIoSessionClosed, got %v", err)
	}
}

func TestGetFileSizeByReader(t *testing.T) {
	t.Run("bytes reader with Len()", func(t *testing.T) {
		data := []byte("hello")
		r := bytes.NewReader(data)
		size := SizeFromReader(r)
		if size != 5 {
			t.Errorf("size = %d, want 5", size)
		}
	})

	t.Run("unknown reader", func(t *testing.T) {
		// io.NopCloser wraps a reader without Len()
		r := io.NopCloser(strings.NewReader("test"))
		size := SizeFromReader(r)
		if size != -1 {
			t.Errorf("size = %d, want -1", size)
		}
	})
}

func BenchmarkStorageBytesProcess(b *testing.B) {
	mgr, _ := NewIoManager("", StorageMemory)
	defer mgr.Cleanup()

	data := bytes.Repeat([]byte("x"), 1024) // 1KB

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ses, _ := mgr.NewSession()
		ctx := WithSession(context.Background(), ses)

		src := NewBytesReader(data)
		out, _ := Process(ctx, src, Out(".bin"), func(ctx context.Context, r io.Reader, w io.Writer) error {
			_, err := io.Copy(w, r)
			return err
		})
		_ = out.Data()

		ses.Cleanup()
	}
}

func BenchmarkStorageFileProcess(b *testing.B) {
	tmpDir := b.TempDir()
	mgr, _ := NewIoManager(tmpDir, StorageFile)
	defer mgr.Cleanup()

	data := bytes.Repeat([]byte("x"), 1024) // 1KB

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ses, _ := mgr.NewSession()
		ctx := WithSession(context.Background(), ses)

		src := NewBytesReader(data)
		out, _ := Process(ctx, src, Out(".bin"), func(ctx context.Context, r io.Reader, w io.Writer) error {
			_, err := io.Copy(w, r)
			return err
		})
		_, _ = out.Bytes()

		ses.Cleanup()
	}
}

type stubStreamReader struct {
	openFn    func() (io.ReadCloser, error)
	cleanupFn func() error
}

func (s stubStreamReader) Open() (io.ReadCloser, error) {
	if s.openFn == nil {
		return nil, errors.New("open not implemented")
	}
	return s.openFn()
}

func (s stubStreamReader) Cleanup() error {
	if s.cleanupFn != nil {
		return s.cleanupFn()
	}
	return nil
}

type errReadCloser struct{}

func (e *errReadCloser) Read(p []byte) (int, error) {
	return 0, io.EOF
}

func (e *errReadCloser) Close() error {
	return errors.New("close failed")
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (r roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return r(req)
}

type seekOnlyReader struct {
	data []byte
	pos  int64
}

func (s *seekOnlyReader) Read(p []byte) (int, error) {
	if s.pos >= int64(len(s.data)) {
		return 0, io.EOF
	}
	n := copy(p, s.data[s.pos:])
	s.pos += int64(n)
	return n, nil
}

func (s *seekOnlyReader) Seek(offset int64, whence int) (int64, error) {
	var next int64
	switch whence {
	case io.SeekStart:
		next = offset
	case io.SeekCurrent:
		next = s.pos + offset
	case io.SeekEnd:
		next = int64(len(s.data)) + offset
	default:
		return 0, errors.New("invalid whence")
	}
	if next < 0 {
		return 0, errors.New("negative position")
	}
	s.pos = next
	return s.pos, nil
}

type dummySession struct{}

func (d dummySession) Read(ctx context.Context, source StreamReader, fn ReadFunc) error {
	return nil
}
func (d dummySession) ReadList(ctx context.Context, sources []StreamReader, fn ReadListFunc) error {
	return nil
}
func (d dummySession) Process(ctx context.Context, source StreamReader, out OutConfig, fn ProcessFunc) (*Output, error) {
	return nil, nil
}
func (d dummySession) ProcessList(ctx context.Context, sources []StreamReader, out OutConfig, fn ProcessListFunc) (*Output, error) {
	return nil, nil
}
func (d dummySession) Cleanup() error { return nil }

func TestToExt(t *testing.T) {
	if got := ToExt("pdf"); got != ".pdf" {
		t.Fatalf("ToExt = %q, want %q", got, ".pdf")
	}
}

func TestConfigure(t *testing.T) {
	old := httpClient
	t.Cleanup(func() { httpClient = old })

	custom := &http.Client{Timeout: 12 * time.Second}
	if err := Configure(Config{Client: custom}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	if httpClient != custom {
		t.Fatalf("httpClient not updated")
	}

	if err := Configure(Config{}); err != nil {
		t.Fatalf("Configure (nil): %v", err)
	}
	if httpClient != custom {
		t.Fatalf("httpClient should remain unchanged")
	}
}

func TestFileReaderSizeAndOpenError(t *testing.T) {
	fr := NewFileReader(filepath.Join(t.TempDir(), "missing.txt"))
	if fr.Size() != -1 {
		t.Fatalf("Size should be -1 for missing file")
	}
	if _, err := fr.Open(); err == nil {
		t.Fatalf("Open should fail for missing file")
	}
}

func TestMultipartReaderAndPartReader(t *testing.T) {
	buf := &bytes.Buffer{}
	w := multipart.NewWriter(buf)
	fw, err := w.CreateFormFile("file", "test.txt")
	if err != nil {
		t.Fatalf("CreateFormFile: %v", err)
	}
	if _, err := fw.Write([]byte("hello")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	body := append([]byte(nil), buf.Bytes()...)

	req, err := http.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	if err := req.ParseMultipartForm(1024); err != nil {
		t.Fatalf("ParseMultipartForm: %v", err)
	}

	fh := req.MultipartForm.File["file"][0]
	mr := NewMultipartReader(fh)
	rc, err := mr.Open()
	if err != nil {
		t.Fatalf("MultipartReader.Open: %v", err)
	}
	data, _ := io.ReadAll(rc)
	rc.Close()
	if string(data) != "hello" {
		t.Fatalf("MultipartReader read %q", string(data))
	}

	partReader := multipart.NewReader(bytes.NewReader(body), w.Boundary())
	part, err := partReader.NextPart()
	if err != nil {
		t.Fatalf("NextPart: %v", err)
	}
	generic := NewPartReader(part)
	prc, err := generic.Open()
	if err != nil {
		t.Fatalf("PartReader.Open: %v", err)
	}
	data, _ = io.ReadAll(prc)
	prc.Close()
	if string(data) != "hello" {
		t.Fatalf("PartReader read %q", string(data))
	}
}

func TestURLReader(t *testing.T) {
	t.Run("invalid url", func(t *testing.T) {
		r := NewURLReader("://bad")
		_, err := r.Open()
		if !errors.Is(err, ErrInvalidURL) {
			t.Fatalf("expected ErrInvalidURL, got %v", err)
		}
	})

	t.Run("invalid scheme", func(t *testing.T) {
		r := NewURLReader("ftp://example.com")
		_, err := r.Open()
		if !errors.Is(err, ErrInvalidURL) {
			t.Fatalf("expected ErrInvalidURL, got %v", err)
		}
	})

	t.Run("non-2xx", func(t *testing.T) {
		client := &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusInternalServerError,
					Body:       io.NopCloser(strings.NewReader("fail")),
					Header:     make(http.Header),
				}, nil
			}),
		}
		r := NewURLReader("http://example.com", URLReaderOptions{Client: client})
		_, err := r.Open()
		if !errors.Is(err, ErrDownloadFailed) {
			t.Fatalf("expected ErrDownloadFailed, got %v", err)
		}
	})

	t.Run("ok", func(t *testing.T) {
		client := &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader("ok")),
					Header:     make(http.Header),
				}, nil
			}),
		}
		r := NewURLReader("http://example.com", URLReaderOptions{Client: client})
		rc, err := r.Open()
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		data, _ := io.ReadAll(rc)
		rc.Close()
		if string(data) != "ok" {
			t.Fatalf("got %q", string(data))
		}
	})

	t.Run("client error", func(t *testing.T) {
		client := &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return nil, errors.New("boom")
			}),
		}
		r := NewURLReader("http://example.com", URLReaderOptions{Client: client})
		_, err := r.Open()
		if !errors.Is(err, ErrDownloadFailed) {
			t.Fatalf("expected ErrDownloadFailed, got %v", err)
		}
	})

	t.Run("custom client used", func(t *testing.T) {
		custom := &http.Client{Timeout: 2 * time.Second}
		r := NewURLReader("http://example.com", URLReaderOptions{Client: custom})
		if r.client != custom {
			t.Fatalf("expected custom client to be used")
		}
	})

	t.Run("insecure tls option sets transport", func(t *testing.T) {
		r := NewURLReader("https://example.com", URLReaderOptions{InsecureTLS: true})
		tr, ok := r.client.Transport.(*http.Transport)
		if !ok || tr.TLSClientConfig == nil || !tr.TLSClientConfig.InsecureSkipVerify {
			t.Fatalf("expected InsecureTLS transport")
		}
	})
}

func TestManagerBaseDirAndSessionDir(t *testing.T) {
	mgr, err := NewIoManager("", StorageMemory)
	if err != nil {
		t.Fatalf("NewIoManager: %v", err)
	}
	defer mgr.Cleanup()

	if mgr.(*manager).BaseDir() != "" {
		t.Fatalf("BaseDir should be empty for memory storage")
	}

	fileMgr, err := NewIoManager("", StorageFile)
	if err != nil {
		t.Fatalf("NewIoManager: %v", err)
	}
	defer fileMgr.Cleanup()

	base := fileMgr.(*manager).BaseDir()
	if base == "" {
		t.Fatalf("BaseDir should be set for file storage")
	}

	ses, err := fileMgr.NewSession()
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer ses.Cleanup()

	if ses.(*ioSession).Dir() == "" {
		t.Fatalf("Dir should be set for file storage")
	}
}

func TestNopCloser(t *testing.T) {
	if NopCloser(nil) != nil {
		t.Fatalf("NopCloser(nil) should return nil")
	}

	rc := io.NopCloser(strings.NewReader("x"))
	if got := NopCloser(rc); got != rc {
		t.Fatalf("NopCloser should return original ReadCloser")
	}

	br := bytes.NewReader([]byte("readerat"))
	nc := NopCloser(br)
	if _, ok := nc.(io.ReaderAt); !ok {
		t.Fatalf("NopCloser should preserve ReaderAt")
	}
}

func TestOutputOpenReaderWriterErrors(t *testing.T) {
	mgr, _ := NewIoManager("", StorageMemory)
	defer mgr.Cleanup()
	ses, _ := mgr.NewSession()
	iSes := ses.(*ioSession)

	out, err := iSes.newOutputWithStorage(".txt", StorageMemory)
	if err != nil {
		t.Fatalf("newOutputWithStorage: %v", err)
	}
	_ = out.cleanup()

	if _, err := out.OpenReader(); err == nil {
		t.Fatalf("OpenReader should fail after cleanup")
	}
	if _, err := out.OpenWriter(); err == nil {
		t.Fatalf("OpenWriter should fail after cleanup")
	}
}

func TestOutputDataFileStorageNil(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, _ := NewIoManager(tmpDir, StorageFile)
	defer mgr.Cleanup()
	ses, _ := mgr.NewSession()
	defer ses.Cleanup()
	ctx := WithSession(context.Background(), ses)

	out, err := Process(ctx, NewBytesReader([]byte("data")), Out(".txt"), func(ctx context.Context, r io.Reader, w io.Writer) error {
		_, err := io.Copy(w, r)
		return err
	})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if out.Data() != nil {
		t.Fatalf("Data should be nil for file storage")
	}
}

func TestDeleteAfterUse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "delete.txt")
	if err := os.WriteFile(path, []byte("remove"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	src := In(NewFileReader(path), DeleteAfterUse())
	if err := Read(context.Background(), src, func(ctx context.Context, r io.Reader) error {
		_, err := io.ReadAll(r)
		return err
	}); err != nil {
		t.Fatalf("Read: %v", err)
	}

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected file to be deleted")
	}
}

func TestNewDownloadReaderCloser(t *testing.T) {
	if _, err := NewDownloadReaderCloser(nil); err == nil {
		t.Fatalf("expected error for nil streamReader")
	}

	cleanupCalled := false
	src := stubStreamReader{
		openFn: func() (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader("data")), nil
		},
		cleanupFn: func() error {
			cleanupCalled = true
			return nil
		},
	}

	drc, err := NewDownloadReaderCloser(src, func() { cleanupCalled = true })
	if err != nil {
		t.Fatalf("NewDownloadReaderCloser: %v", err)
	}

	buf := make([]byte, 4)
	if _, err := drc.Read(buf); err != nil && err != io.EOF {
		t.Fatalf("Read: %v", err)
	}

	if err := drc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !cleanupCalled {
		t.Fatalf("cleanup not called")
	}
	if _, err := drc.Read(buf); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("expected ErrClosedPipe after close, got %v", err)
	}
}

func TestReaderListCloseAndOpenReaderListErrors(t *testing.T) {
	rl, err := OpenReaderList(nil)
	if err != nil {
		t.Fatalf("OpenReaderList empty: %v", err)
	}
	if len(rl.Readers) != 0 {
		t.Fatalf("expected empty ReaderList")
	}

	if _, err := OpenReaderList([]StreamReader{nil}); !errors.Is(err, ErrNilSource) {
		t.Fatalf("expected ErrNilSource, got %v", err)
	}

	_, err = OpenReaderList([]StreamReader{
		stubStreamReader{openFn: func() (io.ReadCloser, error) {
			return nil, errors.New("open failed")
		}},
	})
	if !errors.Is(err, ErrOpenFailed) {
		t.Fatalf("expected ErrOpenFailed, got %v", err)
	}

	rl, err = OpenReaderList([]StreamReader{
		stubStreamReader{
			openFn: func() (io.ReadCloser, error) {
				return &errReadCloser{}, nil
			},
			cleanupFn: func() error { return errors.New("cleanup failed") },
		},
	})
	if err != nil {
		t.Fatalf("OpenReaderList: %v", err)
	}
	if err := rl.Close(); err == nil {
		t.Fatalf("expected error from ReaderList.Close")
	}
}

func TestReadListAndProcessListErrors(t *testing.T) {
	if err := ReadList(context.Background(), nil, func(ctx context.Context, readers []io.Reader) error { return nil }); !errors.Is(err, ErrNilSource) {
		t.Fatalf("expected ErrNilSource, got %v", err)
	}
	if err := ReadList(context.Background(), []StreamReader{NewBytesReader([]byte("x"))}, nil); err == nil {
		t.Fatalf("expected error for nil ReadList func")
	}

	mgr, _ := NewIoManager("", StorageMemory)
	defer mgr.Cleanup()
	ses, _ := mgr.NewSession()
	defer ses.Cleanup()
	ctx := WithSession(context.Background(), ses)

	if _, err := ProcessList(ctx, nil, Out(".txt"), func(ctx context.Context, readers []io.Reader, w io.Writer) error { return nil }); err == nil {
		t.Fatalf("expected error for empty sources")
	}
	if _, err := ProcessList(ctx, []StreamReader{NewBytesReader([]byte("x"))}, Out(".txt"), nil); err == nil {
		t.Fatalf("expected error for nil ProcessList func")
	}
}

func TestReadLinesAndReadFileLines(t *testing.T) {
	if err := ReadLines(context.Background(), NewBytesReader([]byte("a\nb")), nil); err != nil {
		t.Fatalf("ReadLines nil fn: %v", err)
	}

	stopErr := errors.New("stop")
	err := ReadLines(context.Background(), NewBytesReader([]byte("a\nb")), func(line string) error {
		return stopErr
	})
	if !errors.Is(err, stopErr) {
		t.Fatalf("expected stop error, got %v", err)
	}

	path := filepath.Join(t.TempDir(), "lines.txt")
	if err := os.WriteFile(path, []byte("l1\nl2"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	var lines []string
	if err := ReadFileLines(context.Background(), path, func(line string) error {
		lines = append(lines, line)
		return nil
	}); err != nil {
		t.Fatalf("ReadFileLines: %v", err)
	}
	if len(lines) != 2 || lines[0] != "l1" || lines[1] != "l2" {
		t.Fatalf("unexpected lines: %v", lines)
	}
}

func TestSizeAndSizeHelpers(t *testing.T) {
	if _, err := Size(context.Background(), nil); !errors.Is(err, ErrNilSource) {
		t.Fatalf("expected ErrNilSource, got %v", err)
	}

	size, err := Size(context.Background(), NewBytesReader([]byte("abc")))
	if err != nil {
		t.Fatalf("Size: %v", err)
	}
	if size != 3 {
		t.Fatalf("Size = %d", size)
	}

	if got := SizeFromStream(NewBytesReader([]byte("x"))); got != 1 {
		t.Fatalf("SizeFromStream = %d", got)
	}
	if got := SizeFromStream(NewGenericReader(strings.NewReader("x"))); got != -1 {
		t.Fatalf("SizeFromStream for non-sizer = %d", got)
	}

	if got := SizeFromReader(bytes.NewBufferString("buf")); got != 3 {
		t.Fatalf("SizeFromReader buffer = %d", got)
	}
	if got := SizeFromReader(strings.NewReader("str")); got != 3 {
		t.Fatalf("SizeFromReader strings.Reader = %d", got)
	}
	seek := &seekOnlyReader{data: []byte("seek")}
	if got := SizeFromReader(seek); got != 4 {
		t.Fatalf("SizeFromReader seek = %d", got)
	}

	if _, err := SizeFromPath(filepath.Join(t.TempDir(), "missing.txt")); err == nil {
		t.Fatalf("expected error for missing path")
	}
}

func TestWriteFileAndWriteStreamToFileErrors(t *testing.T) {
	if _, err := WriteFile(nil, "x"); !errors.Is(err, ErrNilSource) {
		t.Fatalf("expected ErrNilSource, got %v", err)
	}

	if _, err := WriteStreamToFile(nil, "x"); !errors.Is(err, ErrNilSource) {
		t.Fatalf("expected ErrNilSource, got %v", err)
	}

	_, err := WriteStreamToFile(stubStreamReader{
		openFn: func() (io.ReadCloser, error) {
			return nil, errors.New("open failed")
		},
	}, filepath.Join(t.TempDir(), "out.txt"))
	if err == nil {
		t.Fatalf("expected error for open failure")
	}
}

func TestCopyToFileError(t *testing.T) {
	dir := t.TempDir()
	guard := filepath.Join(dir, "file")
	if err := os.WriteFile(guard, []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	dst := filepath.Join(guard, "child.txt")
	if _, err := copyToFile(strings.NewReader("data"), dst); err == nil {
		t.Fatalf("expected error when parent is file")
	}
}

func TestToReaderAtVariants(t *testing.T) {
	ctx := context.Background()

	if _, err := ToReaderAt(ctx, nil); err == nil {
		t.Fatalf("expected error for nil reader")
	}

	direct, err := ToReaderAt(ctx, bytes.NewReader([]byte("abc")))
	if err != nil {
		t.Fatalf("ToReaderAt direct: %v", err)
	}
	if direct.Source != "direct" || direct.Size != 3 {
		t.Fatalf("unexpected direct result: %#v", direct)
	}
	_ = direct.Cleanup()

	mem, err := ToReaderAt(ctx, io.LimitReader(strings.NewReader("hello"), 5), WithMaxMemoryBytes(10))
	if err != nil {
		t.Fatalf("ToReaderAt memory: %v", err)
	}
	if mem.Source != "memory" || mem.Size != 5 {
		t.Fatalf("unexpected memory result: %#v", mem)
	}

	tmpDir := t.TempDir()
	temp, err := ToReaderAt(ctx, io.NopCloser(strings.NewReader("file")), WithMaxMemoryBytes(0), WithTempDir(tmpDir), WithTempPattern("sio-*"))
	if err != nil {
		t.Fatalf("ToReaderAt temp: %v", err)
	}
	if temp.Source != "tempFile" || temp.Size != 4 {
		t.Fatalf("unexpected temp result: %#v", temp)
	}
	if f, ok := temp.ReaderAt.(*os.File); ok {
		name := f.Name()
		if err := temp.Cleanup(); err != nil {
			t.Fatalf("Cleanup: %v", err)
		}
		if _, err := os.Stat(name); !os.IsNotExist(err) {
			t.Fatalf("expected temp file removed")
		}
	}

	spill, err := ToReaderAt(ctx, io.NopCloser(strings.NewReader("0123456789")), WithMaxMemoryBytes(4))
	if err != nil {
		t.Fatalf("ToReaderAt spill: %v", err)
	}
	if spill.Source != "tempFile" || spill.Size != 10 {
		t.Fatalf("unexpected spill result: %#v", spill)
	}
	_ = spill.Cleanup()

	if _, err := ToReaderAt(ctx, io.NopCloser(strings.NewReader("bad")), WithMaxMemoryBytes(0), WithTempDir(filepath.Join(t.TempDir(), "missing"))); err == nil {
		t.Fatalf("expected error for invalid temp dir")
	}
}

func TestMinInt64(t *testing.T) {
	if got := minInt64(1, 2); got != 1 {
		t.Fatalf("minInt64 = %d", got)
	}
}

func TestUseReaderErrors(t *testing.T) {
	var ur *UseReader
	if _, err := ur.Read(make([]byte, 1)); err == nil {
		t.Fatalf("expected error for nil UseReader")
	}
	if err := ur.Err(); err == nil {
		t.Fatalf("expected error from Err on nil UseReader")
	}

	ur = &UseReader{err: errors.New("boom")}
	if _, err := ur.Read(make([]byte, 1)); err == nil {
		t.Fatalf("expected error for UseReader with err")
	}
	if err := ur.Err(); err == nil {
		t.Fatalf("expected Err to return error")
	}

	ur = &UseReader{}
	if _, err := ur.Read(make([]byte, 1)); err == nil {
		t.Fatalf("expected error for nil reader")
	}
}

func TestBinderAndBindErrors(t *testing.T) {
	var b *Binder
	ur := b.openReader(NewBytesReader([]byte("x")))
	if ur.Err() == nil {
		t.Fatalf("expected error for nil binder")
	}

	b = &Binder{}
	if _, err := b.Use(nil); err == nil {
		t.Fatalf("expected error for nil source")
	}
	if b.Err() == nil {
		t.Fatalf("expected binder error after nil source")
	}

	if err := BindRead(context.Background(), nil); err == nil {
		t.Fatalf("expected error for nil BindRead fn")
	}
	if _, err := BindReadResult[int](context.Background(), nil); err == nil {
		t.Fatalf("expected error for nil BindReadResult fn")
	}

	if _, err := BindProcess(context.Background(), Out(".txt"), nil); err == nil {
		t.Fatalf("expected error for nil BindProcess fn")
	}
	if _, err := BindProcess(context.Background(), Out(".txt"), func(ctx context.Context, b *Binder, w io.Writer) error { return nil }); !errors.Is(err, ErrNoSession) {
		t.Fatalf("expected ErrNoSession, got %v", err)
	}

	ctx := WithSession(context.Background(), dummySession{})
	if _, err := BindProcess(ctx, Out(".txt"), func(ctx context.Context, b *Binder, w io.Writer) error { return nil }); !errors.Is(err, ErrInvalidSessionType) {
		t.Fatalf("expected ErrInvalidSessionType, got %v", err)
	}

	if _, _, err := BindProcessResult[int](context.Background(), Out(".txt"), nil); err == nil {
		t.Fatalf("expected error for nil BindProcessResult fn")
	}
	if _, _, err := BindProcessResult[int](context.Background(), Out(".txt"), func(ctx context.Context, b *Binder, w io.Writer) (*int, error) { return nil, nil }); !errors.Is(err, ErrNoSession) {
		t.Fatalf("expected ErrNoSession, got %v", err)
	}
	if _, _, err := BindProcessResult[int](ctx, Out(".txt"), func(ctx context.Context, b *Binder, w io.Writer) (*int, error) { return nil, nil }); !errors.Is(err, ErrInvalidSessionType) {
		t.Fatalf("expected ErrInvalidSessionType, got %v", err)
	}
}

func TestReadResultAndProcessResultErrors(t *testing.T) {
	if _, err := ReadResult[int](context.Background(), NewBytesReader([]byte("x")), nil); err == nil {
		t.Fatalf("expected error for nil ReadResult fn")
	}
	if _, err := ReadListResult[int](context.Background(), nil, func(ctx context.Context, readers []io.Reader) (*int, error) { return nil, nil }); err == nil {
		t.Fatalf("expected error for empty ReadListResult sources")
	}

	if _, _, err := ProcessResult[int](context.Background(), NewBytesReader([]byte("x")), Out(".txt"), nil); err == nil {
		t.Fatalf("expected error for nil ProcessResult fn")
	}
	if _, _, err := ProcessResult[int](context.Background(), NewBytesReader([]byte("x")), Out(".txt"), func(ctx context.Context, r io.Reader, w io.Writer) (*int, error) { return nil, nil }); !errors.Is(err, ErrNoSession) {
		t.Fatalf("expected ErrNoSession, got %v", err)
	}

	if _, _, err := ProcessListResult[int](context.Background(), nil, Out(".txt"), func(ctx context.Context, readers []io.Reader, w io.Writer) (*int, error) { return nil, nil }); err == nil {
		t.Fatalf("expected error for empty ProcessListResult sources")
	}
}
