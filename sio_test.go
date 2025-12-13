package sio

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
	n, err := CopyOutputTo(out, &buf)
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
