package sio

import (
	"context"
	"io"
	"strings"
	"testing"
)

func TestStorageFile(t *testing.T) {
	// Create IoManager with StorageFile (default)
	mgr, err := NewIoManager("./test-temp", StorageFile)
	if err != nil {
		t.Fatalf("Failed to create IoManager: %v", err)
	}
	defer mgr.Cleanup()

	// Create session
	ses, err := mgr.NewSession()
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}
	defer ses.Cleanup()

	// Test Process with file storage
	ctx := context.Background()
	input := NewBytesReader([]byte("Hello, File Storage!"))

	out, err := ses.Process(ctx, input, Text, func(ctx context.Context, r io.Reader, w io.Writer) error {
		_, err := io.Copy(w, r)
		return err
	})
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	// Verify output
	data, err := out.Bytes()
	if err != nil {
		t.Fatalf("Failed to read output: %v", err)
	}

	if string(data) != "Hello, File Storage!" {
		t.Errorf("Expected 'Hello, File Storage!', got '%s'", string(data))
	}

	// Verify that file was created
	if out.Path() == "" {
		t.Error("Expected non-empty file path for StorageFile mode")
	}
}

func TestStorageBytes(t *testing.T) {
	// Create IoManager with StorageBytes
	mgr, err := NewIoManager("", StorageBytes)
	if err != nil {
		t.Fatalf("Failed to create IoManager: %v", err)
	}
	defer mgr.Cleanup()

	// Create session
	ses, err := mgr.NewSession()
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}
	defer ses.Cleanup()

	// Test Process with memory storage
	ctx := context.Background()
	input := NewBytesReader([]byte("Hello, Memory Storage!"))

	out, err := ses.Process(ctx, input, Text, func(ctx context.Context, r io.Reader, w io.Writer) error {
		_, err := io.Copy(w, r)
		return err
	})
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	// Verify output
	data, err := out.Bytes()
	if err != nil {
		t.Fatalf("Failed to read output: %v", err)
	}

	if string(data) != "Hello, Memory Storage!" {
		t.Errorf("Expected 'Hello, Memory Storage!', got '%s'", string(data))
	}

	// Verify that no file path exists for memory storage
	if out.Path() != "" {
		t.Error("Expected empty file path for StorageBytes mode")
	}
}

func TestStorageBytesMultipleOutputs(t *testing.T) {
	// Create IoManager with StorageBytes
	mgr, err := NewIoManager("", StorageBytes)
	if err != nil {
		t.Fatalf("Failed to create IoManager: %v", err)
	}
	defer mgr.Cleanup()

	// Create session
	ses, err := mgr.NewSession()
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}
	defer ses.Cleanup()

	ctx := context.Background()

	// Create first output
	out1, err := ses.Process(ctx, NewBytesReader([]byte("Output 1")), Text, func(ctx context.Context, r io.Reader, w io.Writer) error {
		_, err := io.Copy(w, r)
		return err
	})
	if err != nil {
		t.Fatalf("Process 1 failed: %v", err)
	}

	// Create second output
	out2, err := ses.Process(ctx, NewBytesReader([]byte("Output 2")), Text, func(ctx context.Context, r io.Reader, w io.Writer) error {
		_, err := io.Copy(w, r)
		return err
	})
	if err != nil {
		t.Fatalf("Process 2 failed: %v", err)
	}

	// Verify both outputs
	data1, err := out1.Bytes()
	if err != nil {
		t.Fatalf("Failed to read output 1: %v", err)
	}
	if string(data1) != "Output 1" {
		t.Errorf("Expected 'Output 1', got '%s'", string(data1))
	}

	data2, err := out2.Bytes()
	if err != nil {
		t.Fatalf("Failed to read output 2: %v", err)
	}
	if string(data2) != "Output 2" {
		t.Errorf("Expected 'Output 2', got '%s'", string(data2))
	}
}

func TestStorageBytesRead(t *testing.T) {
	// Create IoManager with StorageBytes
	mgr, err := NewIoManager("", StorageBytes)
	if err != nil {
		t.Fatalf("Failed to create IoManager: %v", err)
	}
	defer mgr.Cleanup()

	// Create session
	ses, err := mgr.NewSession()
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}
	defer ses.Cleanup()

	ctx := context.Background()
	input := NewBytesReader([]byte("Test Read"))

	var result string
	err = ses.Read(ctx, input, func(ctx context.Context, r io.Reader) error {
		data, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		result = string(data)
		return nil
	})
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if result != "Test Read" {
		t.Errorf("Expected 'Test Read', got '%s'", result)
	}
}

func TestStorageBytesReadList(t *testing.T) {
	// Create IoManager with StorageBytes
	mgr, err := NewIoManager("", StorageBytes)
	if err != nil {
		t.Fatalf("Failed to create IoManager: %v", err)
	}
	defer mgr.Cleanup()

	// Create session
	ses, err := mgr.NewSession()
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}
	defer ses.Cleanup()

	ctx := context.Background()
	sources := []StreamReader{
		NewBytesReader([]byte("Part 1")),
		NewBytesReader([]byte("Part 2")),
		NewBytesReader([]byte("Part 3")),
	}

	var results []string
	err = ses.ReadList(ctx, sources, func(ctx context.Context, readers []io.Reader) error {
		for _, r := range readers {
			data, err := io.ReadAll(r)
			if err != nil {
				return err
			}
			results = append(results, string(data))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ReadList failed: %v", err)
	}

	expected := []string{"Part 1", "Part 2", "Part 3"}
	if len(results) != len(expected) {
		t.Errorf("Expected %d results, got %d", len(expected), len(results))
	}

	for i, exp := range expected {
		if results[i] != exp {
			t.Errorf("Expected '%s' at index %d, got '%s'", exp, i, results[i])
		}
	}
}

func TestStorageBytesProcessList(t *testing.T) {
	// Create IoManager with StorageBytes
	mgr, err := NewIoManager("", StorageBytes)
	if err != nil {
		t.Fatalf("Failed to create IoManager: %v", err)
	}
	defer mgr.Cleanup()

	// Create session
	ses, err := mgr.NewSession()
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}
	defer ses.Cleanup()

	ctx := context.Background()
	sources := []StreamReader{
		NewBytesReader([]byte("Part 1 ")),
		NewBytesReader([]byte("Part 2 ")),
		NewBytesReader([]byte("Part 3")),
	}

	out, err := ses.ProcessList(ctx, sources, Text, func(ctx context.Context, readers []io.Reader, w io.Writer) error {
		for _, r := range readers {
			_, err := io.Copy(w, r)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ProcessList failed: %v", err)
	}

	// Verify output
	data, err := out.Bytes()
	if err != nil {
		t.Fatalf("Failed to read output: %v", err)
	}

	expected := "Part 1 Part 2 Part 3"
	if string(data) != expected {
		t.Errorf("Expected '%s', got '%s'", expected, string(data))
	}
}

func TestDefaultStorageType(t *testing.T) {
	// Create IoManager without specifying storage type (should default to StorageFile)
	mgr, err := NewIoManager("./test-temp-default")
	if err != nil {
		t.Fatalf("Failed to create IoManager: %v", err)
	}
	defer mgr.Cleanup()

	// Create session
	ses, err := mgr.NewSession()
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}
	defer ses.Cleanup()

	ctx := context.Background()
	input := NewBytesReader([]byte("Default storage test"))

	out, err := ses.Process(ctx, input, Text, func(ctx context.Context, r io.Reader, w io.Writer) error {
		_, err := io.Copy(w, r)
		return err
	})
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	// Should have a file path (StorageFile mode)
	if out.Path() == "" {
		t.Error("Expected non-empty file path for default StorageFile mode")
	}

	// Verify content
	data, err := out.Bytes()
	if err != nil {
		t.Fatalf("Failed to read output: %v", err)
	}

	if string(data) != "Default storage test" {
		t.Errorf("Expected 'Default storage test', got '%s'", string(data))
	}
}

func TestStorageBytesTransformation(t *testing.T) {
	// Create IoManager with StorageBytes
	mgr, err := NewIoManager("", StorageBytes)
	if err != nil {
		t.Fatalf("Failed to create IoManager: %v", err)
	}
	defer mgr.Cleanup()

	// Create session
	ses, err := mgr.NewSession()
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}
	defer ses.Cleanup()

	ctx := context.Background()
	input := NewBytesReader([]byte("hello world"))

	// Transform to uppercase
	out, err := ses.Process(ctx, input, Text, func(ctx context.Context, r io.Reader, w io.Writer) error {
		data, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		_, err = w.Write([]byte(strings.ToUpper(string(data))))
		return err
	})
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	// Verify output
	data, err := out.Bytes()
	if err != nil {
		t.Fatalf("Failed to read output: %v", err)
	}

	expected := "HELLO WORLD"
	if string(data) != expected {
		t.Errorf("Expected '%s', got '%s'", expected, string(data))
	}
}
