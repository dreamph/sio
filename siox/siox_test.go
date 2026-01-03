package siox

import (
	"bytes"
	"context"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dreamph/sio"
)

func newSessionCtx(t *testing.T) context.Context {
	t.Helper()

	mgr, err := sio.NewIoManager("", sio.StorageMemory)
	if err != nil {
		t.Fatalf("NewIoManager: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Cleanup() })

	ses, err := mgr.NewSession()
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	t.Cleanup(func() { _ = ses.Cleanup() })

	return sio.WithSession(context.Background(), ses)
}

func newMultipartHeader(t *testing.T, content string) *multipart.FileHeader {
	t.Helper()

	buf := &bytes.Buffer{}
	w := multipart.NewWriter(buf)
	fw, err := w.CreateFormFile("file", "test.txt")
	if err != nil {
		t.Fatalf("CreateFormFile: %v", err)
	}
	if _, err := fw.Write([]byte(content)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/", buf)
	req.Header.Set("Content-Type", w.FormDataContentType())
	if err := req.ParseMultipartForm(1024); err != nil {
		t.Fatalf("ParseMultipartForm: %v", err)
	}

	return req.MultipartForm.File["file"][0]
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (r roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return r(req)
}

func TestDoAndDoBytes(t *testing.T) {
	ctx := newSessionCtx(t)

	out, err := Do(ctx, bytes.NewBufferString("hello"), sio.Out(sio.Txt), func(ctx context.Context, r io.Reader, w io.Writer) error {
		_, err := io.Copy(w, r)
		return err
	})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	data, _ := out.Bytes()
	if string(data) != "hello" {
		t.Fatalf("Do got %q", string(data))
	}

	out, err = DoBytes(ctx, []byte("bytes"), sio.Out(sio.Txt), func(ctx context.Context, r io.Reader, w io.Writer) error {
		_, err := io.Copy(w, r)
		return err
	})
	if err != nil {
		t.Fatalf("DoBytes: %v", err)
	}
	data, _ = out.Bytes()
	if string(data) != "bytes" {
		t.Fatalf("DoBytes got %q", string(data))
	}
}

func TestDoFileDoMultipartDoURL(t *testing.T) {
	ctx := newSessionCtx(t)

	path := filepath.Join(t.TempDir(), "file.txt")
	if err := os.WriteFile(path, []byte("file"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	out, err := DoFile(ctx, path, sio.Out(sio.Txt), func(ctx context.Context, r io.Reader, w io.Writer) error {
		_, err := io.Copy(w, r)
		return err
	})
	if err != nil {
		t.Fatalf("DoFile: %v", err)
	}
	data, _ := out.Bytes()
	if string(data) != "file" {
		t.Fatalf("DoFile got %q", string(data))
	}

	fh := newMultipartHeader(t, "multi")
	out, err = DoMultipart(ctx, fh, sio.Out(sio.Txt), func(ctx context.Context, r io.Reader, w io.Writer) error {
		_, err := io.Copy(w, r)
		return err
	})
	if err != nil {
		t.Fatalf("DoMultipart: %v", err)
	}
	data, _ = out.Bytes()
	if string(data) != "multi" {
		t.Fatalf("DoMultipart got %q", string(data))
	}

	custom := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("url")),
				Header:     make(http.Header),
			}, nil
		}),
	}
	if err := sio.Configure(sio.NewConfig(custom)); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() {
		_ = sio.Configure(sio.NewConfig(&http.Client{Timeout: 30 * time.Second}))
	})

	out, err = DoURL(ctx, "http://example.com", sio.Out(sio.Txt), func(ctx context.Context, r io.Reader, w io.Writer) error {
		_, err := io.Copy(w, r)
		return err
	})
	if err != nil {
		t.Fatalf("DoURL: %v", err)
	}
	data, _ = out.Bytes()
	if string(data) != "url" {
		t.Fatalf("DoURL got %q", string(data))
	}
}

func TestStreamOutput(t *testing.T) {
	ctx := newSessionCtx(t)

	out, err := DoBytes(ctx, []byte("stream"), sio.Out(sio.Txt), func(ctx context.Context, r io.Reader, w io.Writer) error {
		_, err := io.Copy(w, r)
		return err
	})
	if err != nil {
		t.Fatalf("DoBytes: %v", err)
	}

	var buf bytes.Buffer
	if _, err := StreamOutput(out, &buf); err != nil {
		t.Fatalf("StreamOutput: %v", err)
	}
	if buf.String() != "stream" {
		t.Fatalf("StreamOutput got %q", buf.String())
	}
}

func TestDoNoSession(t *testing.T) {
	if _, err := Do(context.Background(), bytes.NewBufferString("x"), sio.Out(sio.Txt), func(ctx context.Context, r io.Reader, w io.Writer) error {
		_, err := io.Copy(w, r)
		return err
	}); err == nil {
		t.Fatalf("expected error without session")
	}
}
