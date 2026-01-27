package fio

import (
	"bytes"
	"context"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type noSizeReader struct{}

func (noSizeReader) Read(p []byte) (int, error) { return 0, io.EOF }

func newTestSession(t *testing.T, storage StorageType) (context.Context, IoSession) {
	t.Helper()

	mgr, err := NewIoManager(t.TempDir(), storage)
	if err != nil {
		t.Fatalf("NewIoManager: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Cleanup() })

	ses, err := mgr.NewSession()
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	t.Cleanup(func() { _ = ses.Cleanup() })

	return WithSession(context.Background(), ses), ses
}

func TestStorageTypeString(t *testing.T) {
	tests := []struct {
		st   StorageType
		want string
	}{
		{File, "file"},
		{Memory, "memory"},
	}

	for _, tt := range tests {
		if got := tt.st.String(); got != tt.want {
			t.Errorf("StorageType(%d).String() = %q, want %q", tt.st, got, tt.want)
		}
	}
}

func TestToExtAndMB(t *testing.T) {
	if got := ToExt("pdf"); got != ".pdf" {
		t.Fatalf("ToExt = %q, want %q", got, ".pdf")
	}
	if got := MB(2); got != 2*1024*1024 {
		t.Fatalf("MB(2) = %d", got)
	}
}

func TestOutOption(t *testing.T) {
	opt := Out(".pdf")
	if opt.Ext() != ".pdf" {
		t.Fatalf("Ext = %q, want %q", opt.Ext(), ".pdf")
	}
	if opt.StorageTypeVal() != nil {
		t.Fatalf("StorageTypeVal should be nil")
	}

	opt = Out(".pdf", Memory)
	if opt.StorageTypeVal() == nil || *opt.StorageTypeVal() != Memory {
		t.Fatalf("StorageTypeVal = %v, want Memory", opt.StorageTypeVal())
	}

	opt = Out(".txt", File)
	if opt.StorageTypeVal() == nil || *opt.StorageTypeVal() != File {
		t.Fatalf("StorageTypeVal = %v, want File", opt.StorageTypeVal())
	}
}

func TestConfigure(t *testing.T) {
	old := httpClient
	t.Cleanup(func() { httpClient = old })

	custom := &http.Client{}
	if err := Configure(NewConfig(custom)); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	if httpClient != custom {
		t.Fatalf("httpClient not updated")
	}
}

func TestNewOutWithoutSession(t *testing.T) {
	if _, err := NewOut(context.Background(), Out(".txt")); !errors.Is(err, ErrNoSession) {
		t.Fatalf("expected ErrNoSession, got %v", err)
	}
}

func TestAutoThresholdManager(t *testing.T) {
	mgr, err := NewIoManager(t.TempDir(), Memory, WithThreshold(4))
	if err != nil {
		t.Fatalf("NewIoManager: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Cleanup() })

	ses, err := mgr.NewSession()
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	t.Cleanup(func() { _ = ses.Cleanup() })

	out, err := ses.NewOut(Out(".txt"), 2)
	if err != nil {
		t.Fatalf("NewOut: %v", err)
	}
	if out.StorageType() != Memory {
		t.Fatalf("StorageType = %v, want Memory", out.StorageType())
	}

	out, err = ses.NewOut(Out(".txt"), 8)
	if err != nil {
		t.Fatalf("NewOut: %v", err)
	}
	if out.StorageType() != File {
		t.Fatalf("StorageType = %v, want File", out.StorageType())
	}
}

func TestOutputWriteRead(t *testing.T) {
	ctx, _ := newTestSession(t, Memory)

	out, err := DoOut(ctx, Out(Txt), func(ctx context.Context, s *OutScope, w io.Writer) error {
		_, err := w.Write([]byte("hello"))
		return err
	})
	if err != nil {
		t.Fatalf("DoOut: %v", err)
	}

	got, err := out.Bytes()
	if err != nil {
		t.Fatalf("Bytes: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("Bytes = %q, want %q", string(got), "hello")
	}
}

func TestCopyAndProcess(t *testing.T) {
	ctx, _ := newTestSession(t, Memory)

	out, err := Copy(ctx, BytesSource([]byte("abc")), Out(Txt))
	if err != nil {
		t.Fatalf("Copy: %v", err)
	}
	got, _ := out.Bytes()
	if string(got) != "abc" {
		t.Fatalf("Copy bytes = %q", string(got))
	}

	out, err = Process(ctx, BytesSource([]byte("xyz")), Out(Txt), func(r io.Reader, w io.Writer) error {
		_, err := io.Copy(w, r)
		return err
	})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	got, _ = out.Bytes()
	if string(got) != "xyz" {
		t.Fatalf("Process bytes = %q", string(got))
	}
}

func TestReadAndResults(t *testing.T) {
	ctx, _ := newTestSession(t, Memory)

	var n int
	err := Read(ctx, BytesSource([]byte("abc")), func(r io.Reader) error {
		b, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		n = len(b)
		return nil
	})
	if err != nil || n != 3 {
		t.Fatalf("Read = %d, %v", n, err)
	}

	res, err := ReadResult(ctx, BytesSource([]byte("abc")), func(r io.Reader) (*string, error) {
		b, err := io.ReadAll(r)
		s := string(b)
		return &s, err
	})
	if err != nil || res == nil || *res != "abc" {
		t.Fatalf("ReadResult = %v, %v", res, err)
	}

	out, got, err := ProcessResult(ctx, BytesSource([]byte("hi")), Out(Txt), func(r io.Reader, w io.Writer) (*string, error) {
		_, err := io.Copy(w, r)
		s := "ok"
		return &s, err
	})
	if err != nil || got == nil || *got != "ok" {
		t.Fatalf("ProcessResult = %v, %v", got, err)
	}
	if out == nil {
		t.Fatalf("ProcessResult output is nil")
	}
}

func TestReadAtAndProcessAt(t *testing.T) {
	ctx, _ := newTestSession(t, Memory)

	got, err := ReadAtResult(ctx, BytesSource([]byte("hello")), func(ra io.ReaderAt, size int64) (*string, error) {
		buf := make([]byte, 2)
		if _, err := ra.ReadAt(buf, 1); err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		if size != 5 {
			return nil, errors.New("bad size")
		}
		s := string(buf)
		return &s, nil
	})
	if err != nil || got == nil || *got != "el" {
		t.Fatalf("ReadAt = %v, %v", got, err)
	}

	out, err := ProcessAt(ctx, BytesSource([]byte("hello")), Out(Txt), func(ra io.ReaderAt, size int64, w io.Writer) error {
		buf := make([]byte, size)
		if _, err := ra.ReadAt(buf, 0); err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		_, err := w.Write(buf)
		return err
	})
	if err != nil {
		t.Fatalf("ProcessAt: %v", err)
	}
	b, _ := out.Bytes()
	if string(b) != "hello" {
		t.Fatalf("ProcessAt bytes = %q", string(b))
	}
}

func TestOutReuseMemory(t *testing.T) {
	ctx, _ := newTestSession(t, Memory)

	var out *Output
	first, err := Copy(ctx, BytesSource([]byte("one")), Out(Txt, Memory, OutReuse(&out)))
	if err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if out == nil || out != first {
		t.Fatalf("expected reuse output pointer")
	}

	second, err := Copy(ctx, BytesSource([]byte("two")), Out(Txt, Memory, OutReuse(&out)))
	if err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if second != out {
		t.Fatalf("expected same output pointer")
	}
	b, _ := out.Bytes()
	if string(b) != "two" {
		t.Fatalf("reuse bytes = %q", string(b))
	}
}

func TestReusableFileInput(t *testing.T) {
	ctx, _ := newTestSession(t, Memory)

	dir := t.TempDir()
	path := filepath.Join(dir, "in.txt")
	if err := os.WriteFile(path, []byte("hello"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	in, err := OpenIn(ctx, PathSource(path), Reusable())
	if err != nil {
		t.Fatalf("OpenIn: %v", err)
	}
	t.Cleanup(func() { _ = in.Close() })

	for i := 0; i < 2; i++ {
		out, err := Copy(ctx, InputSource(in), Out(Txt, Memory))
		if err != nil {
			t.Fatalf("Copy %d: %v", i, err)
		}
		b, _ := out.Bytes()
		if string(b) != "hello" {
			t.Fatalf("Copy %d bytes = %q", i, string(b))
		}
	}
}

func TestSizeHelpers(t *testing.T) {
	ctx, _ := newTestSession(t, Memory)

	size, err := Size(ctx, BytesSource([]byte("abc")))
	if err != nil || size != 3 {
		t.Fatalf("Size = %d, %v", size, err)
	}
	if got := SizeFromStream(BytesSource([]byte("x"))); got != 1 {
		t.Fatalf("SizeFromStream = %d", got)
	}
	if got := SizeFromStream(ReaderSource(noSizeReader{})); got != -1 {
		t.Fatalf("SizeFromStream non-sizer = %d", got)
	}
	if got := SizeFromStreamList([]Source{BytesSource([]byte("a")), BytesSource([]byte("b"))}); got != 2 {
		t.Fatalf("SizeFromStreamList = %d", got)
	}
}

func TestWriteFileHelpers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.txt")

	n, err := WriteFile(strings.NewReader("abc"), path)
	if err != nil || n != 3 {
		t.Fatalf("WriteFile = %d, %v", n, err)
	}

	n, err = WriteStreamToFile(BytesSource([]byte("xyz")), filepath.Join(dir, "out2.txt"))
	if err != nil || n != 3 {
		t.Fatalf("WriteStreamToFile = %d, %v", n, err)
	}
}

func TestReadLinesHelpers(t *testing.T) {
	ctx, _ := newTestSession(t, Memory)
	path := filepath.Join(t.TempDir(), "lines.txt")
	if err := os.WriteFile(path, []byte("l1\nl2"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	var lines []string
	if err := ReadLines(ctx, BytesSource([]byte("a\nb")), func(line string) error {
		lines = append(lines, line)
		return nil
	}); err != nil {
		t.Fatalf("ReadLines: %v", err)
	}
	if len(lines) != 2 || lines[0] != "a" || lines[1] != "b" {
		t.Fatalf("ReadLines = %v", lines)
	}

	lines = nil
	if err := ReadFileLines(ctx, path, func(line string) error {
		lines = append(lines, line)
		return nil
	}); err != nil {
		t.Fatalf("ReadFileLines: %v", err)
	}
	if len(lines) != 2 || lines[0] != "l1" || lines[1] != "l2" {
		t.Fatalf("ReadFileLines = %v", lines)
	}
}

func TestOpenInReusable(t *testing.T) {
	ctx, _ := newTestSession(t, Memory)

	in, err := OpenIn(ctx, BytesSource([]byte("abc")), Reusable())
	if err != nil {
		t.Fatalf("OpenIn: %v", err)
	}
	t.Cleanup(func() { _ = in.Close() })

	readOnce := func() string {
		got, err := ReadResult(ctx, InputSource(in), func(r io.Reader) (*string, error) {
			b, err := io.ReadAll(r)
			if err != nil {
				return nil, err
			}
			s := string(b)
			return &s, nil
		})
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if got == nil {
			t.Fatalf("Read: nil result")
		}
		return *got
	}
	if got := readOnce(); got != "abc" {
		t.Fatalf("Read once = %q", got)
	}
	if got := readOnce(); got != "abc" {
		t.Fatalf("Read twice = %q", got)
	}
}

func TestDownloadReaderCloser(t *testing.T) {
	called := 0
	rc, err := NewDownloadReaderCloser(BytesSource([]byte("abc")), func() { called++ })
	if err != nil {
		t.Fatalf("NewDownloadReaderCloser: %v", err)
	}
	b, err := io.ReadAll(rc)
	if err != nil || string(b) != "abc" {
		t.Fatalf("ReadAll = %q, %v", string(b), err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if called != 1 {
		t.Fatalf("cleanup called %d", called)
	}
}

func TestToReaderAtVariants(t *testing.T) {
	ctx, _ := newTestSession(t, Memory)

	res, err := ToReaderAt(ctx, bytes.NewReader([]byte("hello")))
	if err != nil {
		t.Fatalf("ToReaderAt: %v", err)
	}
	if res.Source() != readerAtSourceDirect {
		t.Fatalf("source = %s", res.Source())
	}
	if res.Size() != 5 {
		t.Fatalf("size = %d", res.Size())
	}
	_ = res.Cleanup()

	res, err = ToReaderAt(ctx, bytes.NewBufferString("abc"), WithMaxMemoryBytes(1))
	if err != nil {
		t.Fatalf("ToReaderAt: %v", err)
	}
	if res.Source() != readerAtSourceTempFile {
		t.Fatalf("expected temp file source, got %s", res.Source())
	}
	_ = res.Cleanup()
}

func TestURLSource(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(srv.Close)

	ctx, _ := newTestSession(t, Memory)
	got, err := ReadResult(ctx, URLSource(srv.URL), func(r io.Reader) (*string, error) {
		b, err := io.ReadAll(r)
		if err != nil {
			return nil, err
		}
		s := string(b)
		return &s, nil
	})
	if err != nil || got == nil || *got != "ok" {
		t.Fatalf("URLSource = %v, %v", got, err)
	}
}

func TestMultipartSource(t *testing.T) {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("file", "t.txt")
	if err != nil {
		t.Fatalf("CreateFormFile: %v", err)
	}
	if _, err := part.Write([]byte("hello")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/upload", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	if err := req.ParseMultipartForm(1 << 20); err != nil {
		t.Fatalf("ParseMultipartForm: %v", err)
	}
	files := req.MultipartForm.File["file"]
	if len(files) != 1 {
		t.Fatalf("expected 1 file")
	}

	ctx, _ := newTestSession(t, Memory)
	got, err := ReadResult(ctx, MultipartSource(files[0]), func(r io.Reader) (*string, error) {
		b, err := io.ReadAll(r)
		if err != nil {
			return nil, err
		}
		s := string(b)
		return &s, nil
	})
	if err != nil || got == nil || *got != "hello" {
		t.Fatalf("MultipartSource = %v, %v", got, err)
	}
}

func TestErrorPaths(t *testing.T) {
	ctx, _ := newTestSession(t, Memory)

	if err := Read(ctx, BytesSource([]byte("x")), nil); !errors.Is(err, ErrNilFunc) {
		t.Fatalf("Read nil fn: %v", err)
	}
	if _, err := Copy(ctx, nil, Out(".txt")); !errors.Is(err, ErrNilSource) {
		t.Fatalf("Copy nil source: %v", err)
	}
	if _, err := Size(context.Background(), nil); !errors.Is(err, ErrNilSource) {
		t.Fatalf("Size nil source: %v", err)
	}
	if _, err := NewDownloadReaderCloser(nil); err == nil {
		t.Fatalf("expected error for nil source")
	}
}
