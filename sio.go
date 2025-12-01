package sio

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

/* ---------- Errors ---------- */

var (
	ErrNilSource       = errors.New("sio: nil source")
	ErrOpenFailed      = errors.New("sio: cannot open reader")
	ErrIoManagerClosed = errors.New("sio: manager is closed")
	ErrIoSessionClosed = errors.New("sio: session is closed")
	ErrInvalidURL      = errors.New("sio: invalid URL")
	ErrDownloadFailed  = errors.New("sio: download failed")
	ErrNoSession       = errors.New("sio: session is nil")
)

var httpClient = &http.Client{Timeout: 30 * time.Second,
	Transport: &http.Transport{
		TLSClientConfig: &tls.Config{
			//InsecureSkipVerify: true,
		},
	},
}

func SetHttpClient(c *http.Client) {
	httpClient = c
}

/* -------------------------------------------------------------------------- */
/*                           StreamReader Abstraction                          */
/* -------------------------------------------------------------------------- */

// StreamReader represents any input source that can be opened as an io.ReadCloser,
// and is responsible for cleaning up its own underlying resources (temporary files,
// buffers, etc.).
type StreamReader interface {
	Open() (io.ReadCloser, error)
	Cleanup() error
}

/* -------------------------------------------------------------------------- */
/*                               FileReader                                    */
/* -------------------------------------------------------------------------- */

// FileReader reads from a file path on disk.
// Cleanup() does NOT remove the original file.
type FileReader struct {
	Path string
}

func NewFileReader(path string) *FileReader { return &FileReader{Path: path} }

func (f *FileReader) Open() (io.ReadCloser, error) { return os.Open(f.Path) }
func (f *FileReader) Cleanup() error {
	return nil
}

/* -------------------------------------------------------------------------- */
/*                             MultipartReader                                 */
/* -------------------------------------------------------------------------- */

// MultipartReader wraps an uploaded multipart.FileHeader.
// Cleanup() does nothing because frameworks handle their own temp files.
type MultipartReader struct {
	File *multipart.FileHeader
}

func NewMultipartReader(fh *multipart.FileHeader) *MultipartReader {
	return &MultipartReader{File: fh}
}

func (m *MultipartReader) Open() (io.ReadCloser, error) { return m.File.Open() }
func (m *MultipartReader) Cleanup() error               { return nil }

/* -------------------------------------------------------------------------- */
/*                                 IOReader                                    */
/* -------------------------------------------------------------------------- */

// IOReader wraps an uploaded io.Reader.
// Cleanup() does nothing because frameworks handle their own temp files.
type IOReader struct {
	File io.Reader
}

func NewIOReader(r io.Reader) *IOReader {
	return &IOReader{File: r}
}

func (m *IOReader) Open() (io.ReadCloser, error) {
	if m.File == nil {
		return nil, fmt.Errorf("sio: IOReader: underlying reader is nil")
	}

	if rc, ok := m.File.(io.ReadCloser); ok {
		return rc, nil
	}

	return io.NopCloser(m.File), nil
}

func (m *IOReader) Cleanup() error { return nil }

/* -------------------------------------------------------------------------- */
/*                                BytesReader                                  */
/* -------------------------------------------------------------------------- */

// BytesReader exposes an in-memory []byte as a StreamReader.
// Cleanup() optionally clears the underlying buffer.
type BytesReader struct {
	Data []byte
}

func NewBytesReader(data []byte) *BytesReader {
	return &BytesReader{Data: data}
}

func (b *BytesReader) Open() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(b.Data)), nil
}

func (b *BytesReader) Cleanup() error {
	b.Data = nil
	return nil
}

/* -------------------------------------------------------------------------- */
/*                                 URLReader                                   */
/* -------------------------------------------------------------------------- */

// URLReader streams content directly from a URL.
// Each Open() call creates a new HTTP request.
// Cleanup() is a no-op since the response body is closed via the returned ReadCloser.
type URLReader struct {
	URL string
}

func NewURLReader(urlStr string) *URLReader { return &URLReader{URL: urlStr} }

func (u *URLReader) Open() (io.ReadCloser, error) {
	parsed, err := url.Parse(u.URL)
	if err != nil {
		return nil, ErrInvalidURL
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, ErrInvalidURL
	}

	resp, err := httpClient.Get(u.URL)
	if err != nil {
		return nil, ErrDownloadFailed
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		resp.Body.Close()
		return nil, ErrDownloadFailed
	}

	return resp.Body, nil
}

func (u *URLReader) Cleanup() error { return nil }

/* -------------------------------------------------------------------------- */
/*                               Manager                                       */
/* -------------------------------------------------------------------------- */

type IoManager interface {
	NewSession() (IoSession, error)
	Cleanup() error
}

// IoManager manages a root directory under which multiple IoSessions
// create their own isolated working directories.
type manager struct {
	mu      sync.Mutex
	baseDir string
	closed  bool
}

// NewIoManager :
//   - baseDir == "" → create a temp folder using os.MkdirTemp("", "sio-")
//   - baseDir != "" → create/use the provided directory
func NewIoManager(baseDir string) (IoManager, error) {
	if strings.TrimSpace(baseDir) == "" {
		dir, err := os.MkdirTemp("", "sio-")
		if err != nil {
			return nil, err
		}
		return &manager{baseDir: dir}, nil
	}

	baseDir = filepath.Clean(baseDir)
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, err
	}

	return &manager{baseDir: baseDir}, nil
}

// BaseDir returns the underlying root directory managed by this IoManager.
func (m *manager) BaseDir() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.baseDir
}

// NewSession creates a new isolated IoSession under baseDir.
func (m *manager) NewSession() (IoSession, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil, ErrIoManagerClosed
	}

	id := uuid.New().String()
	dir := filepath.Join(m.baseDir, id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	return &ioSession{manager: m, dir: dir}, nil
}

// Cleanup removes the entire baseDir. Use only during app shutdown.
func (m *manager) Cleanup() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil
	}
	m.closed = true

	err := os.RemoveAll(m.baseDir)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

/* -------------------------------------------------------------------------- */
/*                                IoSession                                      */
/* -------------------------------------------------------------------------- */

type ReadFunc func(ctx context.Context, r io.Reader) error
type ReadListFunc func(ctx context.Context, readers []io.Reader) error
type ProcessFunc func(ctx context.Context, r io.Reader, w io.Writer) error
type ProcessListFunc func(ctx context.Context, readers []io.Reader, w io.Writer) error

type IoSession interface {
	Read(ctx context.Context, source StreamReader, fn ReadFunc) error
	ReadList(ctx context.Context, sources []StreamReader, fn ReadListFunc) error

	Process(ctx context.Context, source StreamReader, outExt string, fn ProcessFunc) (*Output, error)
	ProcessList(ctx context.Context, sources []StreamReader, outExt string, fn ProcessListFunc) (*Output, error)

	Cleanup() error
}

// session represents an isolated working directory where all temporary files
// and transformation outputs are stored during a single job or request.
type ioSession struct {
	mu      sync.Mutex
	manager IoManager
	dir     string
	closed  bool
	outputs []*Output // tracked outputs inside this session
}

// Dir returns the directory assigned to this IoIoSession.
func (s *ioSession) Dir() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dir
}

func (s *ioSession) ensureOpen() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrIoSessionClosed
	}
	return nil
}

// newOutput creates a new temporary file inside the session directory and tracks it.
func (s *ioSession) newOutput(ext string) (*Output, error) {
	if err := s.ensureOpen(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	pattern := "*"
	if ext != "" {
		pattern += ext
	}

	f, err := os.CreateTemp(s.dir, pattern)
	if err != nil {
		return nil, err
	}
	f.Close()

	out := &Output{path: f.Name(), ses: s}
	s.outputs = append(s.outputs, out)
	return out, nil
}

// Process executes a transformation:
//  1. Open source (r)
//  2. Create Output (w)
//  3. Run fn(ctx, r, w)
//
// Returns an Output on success.
func (s *ioSession) Process(
	ctx context.Context,
	source StreamReader,
	outExt string,
	fn ProcessFunc,
) (*Output, error) {

	if source == nil {
		return nil, ErrNilSource
	}
	if fn == nil {
		return nil, fmt.Errorf("sio: IoSession: fn is nil")
	}
	if err := s.ensureOpen(); err != nil {
		return nil, err
	}

	r, err := source.Open()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrOpenFailed, err)
	}
	defer r.Close()
	defer source.Cleanup()

	out, err := s.newOutput(outExt)
	if err != nil {
		return nil, err
	}

	w, err := out.OpenWriter()
	if err != nil {
		_ = out.cleanup()
		return nil, err
	}
	defer w.Close()

	if err := fn(ctx, r, w); err != nil {
		_ = out.cleanup()
		return nil, err
	}

	return out, nil
}

// ProcessList opens all sources, passes the readers to fn along with a writer,
// and returns an Output on success.
func (s *ioSession) ProcessList(
	ctx context.Context,
	sources []StreamReader,
	outExt string,
	fn ProcessListFunc,
) (*Output, error) {

	if len(sources) == 0 {
		return nil, fmt.Errorf("sio: empty source list")
	}
	if fn == nil {
		return nil, fmt.Errorf("sio: ProcessReaderList: fn is nil")
	}
	if err := s.ensureOpen(); err != nil {
		return nil, err
	}

	rl, err := OpenReaderList(sources)
	if err != nil {
		return nil, err
	}
	defer rl.Close()

	out, err := s.newOutput(outExt)
	if err != nil {
		return nil, err
	}

	w, err := out.OpenWriter()
	if err != nil {
		_ = out.cleanup()
		return nil, err
	}
	defer w.Close()

	if err := fn(ctx, rl.Readers, w); err != nil {
		_ = out.cleanup()
		return nil, err
	}

	return out, nil
}

// Read simply opens the StreamReader and passes it to fn.
// No Output file is created.
func (s *ioSession) Read(
	ctx context.Context,
	source StreamReader,
	fn ReadFunc,
) error {

	if source == nil {
		return ErrNilSource
	}
	if fn == nil {
		return fmt.Errorf("sio: Read: fn is nil")
	}
	if err := s.ensureOpen(); err != nil {
		return err
	}

	r, err := source.Open()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrOpenFailed, err)
	}
	defer r.Close()
	defer source.Cleanup()

	return fn(ctx, r)
}

func (s *ioSession) ReadList(
	ctx context.Context,
	sources []StreamReader,
	fn ReadListFunc,
) error {
	if len(sources) == 0 {
		return ErrNilSource
	}
	if fn == nil {
		return fmt.Errorf("sio: ReadList: fn is nil")
	}

	rl, err := OpenReaderList(sources)
	if err != nil {
		return err
	}
	defer rl.Close()

	return fn(ctx, rl.Readers)
}

/* ---------- IoSession Cleanup Handling ---------- */

// isKeptPath returns true if the given path belongs to an Output marked as kept.
func (s *ioSession) isKeptPath(path string) bool {
	for _, o := range s.outputs {
		o.mu.Lock()
		kept := o.keep && !o.closed && o.path == path
		o.mu.Unlock()
		if kept {
			return true
		}
	}
	return false
}

// Cleanup removes all non-kept files inside the session directory.
// Kept files survive IoSession.Cleanup().
// The directory itself is removed if empty.
func (s *ioSession) Cleanup() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	entries, err := os.ReadDir(s.dir)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	var firstErr error

	for _, e := range entries {
		full := filepath.Join(s.dir, e.Name())

		if s.isKeptPath(full) {
			continue
		}

		if err := os.RemoveAll(full); err != nil && !os.IsNotExist(err) && firstErr == nil {
			firstErr = err
		}
	}

	_ = os.Remove(s.dir)
	return firstErr
}

/* -------------------------------------------------------------------------- */
/*                                  Output                                     */
/* -------------------------------------------------------------------------- */

// Output represents a file produced by a IoSession.Process call.
// It can be kept beyond session cleanup via Output.Keep().
type Output struct {
	mu           sync.Mutex
	path         string
	ses          IoSession
	closed       bool
	keep         bool
	detachedPath string // file copied outside the session dir (for detached use)
}

func (o *Output) Path() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.path
}

func (o *Output) OpenReader() (io.ReadCloser, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return nil, fmt.Errorf("sio: output is cleaned up")
	}
	return os.Open(o.path)
}

func (o *Output) OpenWriter() (io.WriteCloser, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return nil, fmt.Errorf("sio: output is cleaned up")
	}
	return os.Create(o.path)
}

func (o *Output) Reader(opts ...InOption) StreamReader {
	sr := NewFileReader(o.Path())
	if len(opts) > 0 {
		return In(sr, opts...)
	}
	return sr
}

// cleanup is an internal helper that removes the output file and any detached file.
func (o *Output) cleanup() error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return nil
	}
	o.closed = true

	var errs error

	// Remove the file inside the session.
	if err := os.Remove(o.path); err != nil && !os.IsNotExist(err) {
		errs = errors.Join(errs, err)
	}

	// Remove the detached file (if any).
	if o.detachedPath != "" {
		if err := os.Remove(o.detachedPath); err != nil && !os.IsNotExist(err) {
			errs = errors.Join(errs, err)
		}
	}

	return errs
}

// Keep marks the output file as persistent.
// IoSession.Cleanup() will NOT delete the file.
// This file will survive until process exit or manual deletion.
func (o *Output) Keep() *Output {
	o.mu.Lock()
	o.keep = true
	o.mu.Unlock()
	return o
}

// SaveAs copies the output file to a persistent path outside of the session dir.
func (o *Output) SaveAs(path string) error {
	r, err := o.OpenReader()
	if err != nil {
		return err
	}
	defer r.Close()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = io.Copy(f, r)
	return err
}

// Bytes loads the entire file into memory.
// Use carefully for very large files.
func (o *Output) Bytes() ([]byte, error) {
	r, err := o.OpenReader()
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return io.ReadAll(r)
}

/* -------------------------------------------------------------------------- */
/*                                In helpers                                   */
/* -------------------------------------------------------------------------- */

// inConfig holds options passed via In(...).
type inConfig struct {
	deleteAfterProcess bool
}

// InOption is a functional option for In(...).
type InOption func(*inConfig)

// DeleteAfterUse tells In(...) to delete the underlying file after
// the StreamReader is cleaned up (only works for *FileReader).
func DeleteAfterUse() InOption {
	return func(c *inConfig) {
		c.deleteAfterProcess = true
	}
}

// streamReaderWithCleanup wraps an existing StreamReader and injects
// extra cleanup behavior (e.g. deleting a temp file) before delegating
// to the inner StreamReader's Cleanup().
type streamReaderWithCleanup struct {
	inner        StreamReader
	extraCleanup func() error
}

func (w *streamReaderWithCleanup) Open() (io.ReadCloser, error) {
	return w.inner.Open()
}

func (w *streamReaderWithCleanup) Cleanup() error {
	var errs error

	if w.extraCleanup != nil {
		if err := w.extraCleanup(); err != nil {
			errs = errors.Join(errs, err)
		}
	}

	if w.inner != nil {
		if err := w.inner.Cleanup(); err != nil {
			errs = errors.Join(errs, err)
		}
	}

	return errs
}

// In wraps a StreamReader with additional behavior, such as DeleteAfterUse.
//
// Example:
//
//	sr, _ := out.Reader()
//	out2, _ := ses.Process(ctx, sio2.In(sr, sio2.DeleteAfterUse()), ".pdf", step2)
func In(sr StreamReader, opts ...InOption) StreamReader {
	if sr == nil {
		return nil
	}

	cfg := &inConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	// No extra behavior requested → return original.
	if !cfg.deleteAfterProcess {
		return sr
	}

	// Only *FileReader has a concrete path we can delete.
	fr, ok := sr.(*FileReader)
	if !ok {
		return sr
	}

	path := fr.Path

	extra := func() error {
		if path == "" {
			return nil
		}
		err := os.Remove(path)
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	return &streamReaderWithCleanup{
		inner:        sr,
		extraCleanup: extra,
	}
}

/* -------------------------------------------------------------------------- */
/*                   Utility: Wrap io.Reader as StreamReader                   */
/* -------------------------------------------------------------------------- */

// NewPartReader wraps multipart.Part as StreamReader (streaming, no buffering).
func NewPartReader(p *multipart.Part) *IOReader {
	// *multipart.Part implements io.ReadCloser
	// ด้วย IOReader.Open() เวอร์ชันใหม่ จะคืน *multipart.Part โดยตรง
	return NewIOReader(p)
}

/* -------------------------------------------------------------------------- */
/*                       DownloadReaderCloser helper                           */
/* -------------------------------------------------------------------------- */

// DownloadReaderCloser is a convenience interface for a read-only stream
// that also supports Close, typically used for HTTP response bodies or
// file download handlers.
type DownloadReaderCloser interface {
	io.Reader
	io.Closer
}

type downloadReaderCloser struct {
	reader       io.ReadCloser
	streamReader StreamReader
	Cleanup      func()
}

func (d *downloadReaderCloser) Read(p []byte) (int, error) {
	if d.reader == nil {
		return 0, io.ErrClosedPipe
	}
	return d.reader.Read(p)
}

func (d *downloadReaderCloser) Close() error {
	var errs error

	// 1) Close the underlying reader
	if d.reader != nil {
		readerCloseErr := d.reader.Close()
		if readerCloseErr != nil {
			errs = errors.Join(errs, readerCloseErr)
		}
		d.reader = nil
	}

	// 2) Cleanup the StreamReader (e.g., remove temp files)
	if d.streamReader != nil {
		streamReaderCleanupErr := d.streamReader.Cleanup()
		if streamReaderCleanupErr != nil {
			errs = errors.Join(errs, streamReaderCleanupErr)
		}
		d.streamReader = nil
	}

	// 3) Run the optional extra cleanup callback
	if d.Cleanup != nil {
		d.Cleanup()
		d.Cleanup = nil
	}

	if errs != nil {
		fmt.Println("downloadReaderCloser.Close, Err:", errs)
	} else {
		fmt.Println("downloadReaderCloser.Close")
	}

	return errs
}

// NewDownloadReaderCloser wraps a StreamReader as a DownloadReaderCloser.
// When Close() is called, it closes the underlying reader and then calls
// StreamReader.Cleanup() and the optional extra cleanup function.
func NewDownloadReaderCloser(streamReader StreamReader, cleanup ...func()) (DownloadReaderCloser, error) {
	if streamReader == nil {
		return nil, fmt.Errorf("nil streamReader")
	}

	reader, err := streamReader.Open()
	if err != nil {
		return nil, err
	}

	closer := &downloadReaderCloser{
		streamReader: streamReader,
		reader:       reader,
	}

	if len(cleanup) > 0 {
		closer.Cleanup = cleanup[0]
	}

	return closer, nil
}

/* -------------------------------------------------------------------------- */
/*                         StreamReader List Utilities                         */
/* -------------------------------------------------------------------------- */

// ReaderList holds opened readers and provides cleanup functionality.
type ReaderList struct {
	Readers []io.Reader
	closers []io.Closer
	sources []StreamReader
}

// Close closes all opened readers and cleans up all source StreamReaders.
func (rl *ReaderList) Close() error {
	var errs error

	for _, c := range rl.closers {
		if err := c.Close(); err != nil {
			errs = errors.Join(errs, err)
		}
	}

	for _, source := range rl.sources {
		if err := source.Cleanup(); err != nil {
			errs = errors.Join(errs, err)
		}
	}

	rl.Readers = nil
	rl.closers = nil
	rl.sources = nil

	return errs
}

// OpenReaderList opens all StreamReaders and returns a ReaderList.
// The caller must call Close() on the returned ReaderList to release resources.
// If any Open() fails, all previously opened readers are closed and cleaned up.
func OpenReaderList(sources []StreamReader) (*ReaderList, error) {
	if len(sources) == 0 {
		return &ReaderList{}, nil
	}

	rl := &ReaderList{
		Readers: make([]io.Reader, 0, len(sources)),
		closers: make([]io.Closer, 0, len(sources)),
		sources: make([]StreamReader, 0, len(sources)),
	}

	for _, source := range sources {
		if source == nil {
			rl.Close()
			return nil, ErrNilSource
		}

		rc, err := source.Open()
		if err != nil {
			rl.Close()
			return nil, fmt.Errorf("%w: %v", ErrOpenFailed, err)
		}

		rl.Readers = append(rl.Readers, rc)
		rl.closers = append(rl.closers, rc)
		rl.sources = append(rl.sources, source)
	}

	return rl, nil
}

type ctxKey struct{}

var sessionKey = ctxKey{}

// WithSession attaches a IoSession into context.
func WithSession(ctx context.Context, ses IoSession) context.Context {
	return context.WithValue(ctx, sessionKey, ses)
}

// Session extracts IoSession from context.
// Returns nil if not found.
func Session(ctx context.Context) IoSession {
	ses, _ := ctx.Value(sessionKey).(IoSession)
	return ses
}

// Read is a convenience wrapper that gets IoSession from context
// and calls ses.Read(...).
func Read(ctx context.Context, source StreamReader, fn ReadFunc) error {
	if source == nil {
		return ErrNilSource
	}
	if fn == nil {
		return fmt.Errorf("sio: Read: fn is nil")
	}

	ses := Session(ctx)
	if ses == nil {
		return ErrNoSession
	}

	return ses.Read(ctx, source, fn)
}

// ReadList is a convenience wrapper for IoSession.ReadList.
func ReadList(ctx context.Context, sources []StreamReader, fn ReadListFunc) error {
	if len(sources) == 0 {
		return ErrNilSource
	}
	if fn == nil {
		return fmt.Errorf("sio: ReadList: fn is nil")
	}

	ses := Session(ctx)
	if ses == nil {
		return ErrNoSession
	}

	return ses.ReadList(ctx, sources, fn)
}

// Process is a convenience wrapper for IoSession.Process.
func Process(ctx context.Context, source StreamReader, outExt string, fn ProcessFunc) (*Output, error) {
	if source == nil {
		return nil, ErrNilSource
	}
	if fn == nil {
		return nil, fmt.Errorf("sio: Process: fn is nil")
	}

	ses := Session(ctx)
	if ses == nil {
		return nil, ErrNoSession
	}

	return ses.Process(ctx, source, outExt, fn)
}

// ProcessList is a convenience wrapper for IoSession.ProcessList.
func ProcessList(ctx context.Context, sources []StreamReader, outExt string, fn ProcessListFunc) (*Output, error) {
	if len(sources) == 0 {
		return nil, ErrNilSource
	}
	if fn == nil {
		return nil, fmt.Errorf("sio: ProcessList: fn is nil")
	}

	ses := Session(ctx)
	if ses == nil {
		return nil, ErrNoSession
	}

	return ses.ProcessList(ctx, sources, outExt, fn)
}

func ReadResult[T any](ctx context.Context, source StreamReader, fn func(ctx context.Context, r io.Reader) (*T, error)) (*T, error) {
	if source == nil {
		return nil, ErrNilSource
	}
	if fn == nil {
		return nil, fmt.Errorf("sio: ReadWithResult: fn is nil")
	}

	ses := Session(ctx)
	if ses == nil {
		return nil, ErrNoSession
	}

	var result *T
	err := ses.Read(ctx, source, func(ctx context.Context, r io.Reader) error {
		res, err := fn(ctx, r)
		if err != nil {
			return err
		}

		result = res
		return nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

func ReadListResult[T any](
	ctx context.Context,
	sources []StreamReader,
	fn func(ctx context.Context, readers []io.Reader) (*T, error),
) (*T, error) {
	if len(sources) == 0 {
		return nil, fmt.Errorf("sio: ReadListResult: empty sources")
	}
	if fn == nil {
		return nil, fmt.Errorf("sio: ReadListResult: fn is nil")
	}

	ses := Session(ctx)
	if ses == nil {
		return nil, ErrNoSession
	}

	var result *T
	err := ses.ReadList(ctx, sources, func(ctx context.Context, readers []io.Reader) error {
		res, err := fn(ctx, readers)
		if err != nil {
			return err
		}
		result = res
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func ProcessResult[T any](
	ctx context.Context,
	source StreamReader,
	outExt string,
	fn func(ctx context.Context, r io.Reader, w io.Writer) (*T, error),
) (*Output, *T, error) {
	if source == nil {
		return nil, nil, ErrNilSource
	}
	if fn == nil {
		return nil, nil, fmt.Errorf("sio: ProcessResult: fn is nil")
	}

	ses := Session(ctx)
	if ses == nil {
		return nil, nil, ErrNoSession
	}

	var result *T
	out, err := ses.Process(ctx, source, outExt, func(ctx context.Context, r io.Reader, w io.Writer) error {
		res, err := fn(ctx, r, w)
		if err != nil {
			return err
		}
		result = res
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return out, result, nil
}

func ProcessListResult[T any](
	ctx context.Context,
	sources []StreamReader,
	outExt string,
	fn func(ctx context.Context, readers []io.Reader, w io.Writer) (*T, error),
) (*Output, *T, error) {
	if len(sources) == 0 {
		return nil, nil, fmt.Errorf("sio: ProcessListResult: empty sources")
	}
	if fn == nil {
		return nil, nil, fmt.Errorf("sio: ProcessListResult: fn is nil")
	}

	ses := Session(ctx)
	if ses == nil {
		return nil, nil, ErrNoSession
	}

	var result *T
	out, err := ses.ProcessList(ctx, sources, outExt, func(ctx context.Context, readers []io.Reader, w io.Writer) error {
		res, err := fn(ctx, readers, w)
		if err != nil {
			return err
		}
		result = res
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return out, result, nil
}

func ToOutput(ctx context.Context, src StreamReader, ext string) (*Output, error) {
	return Process(ctx, src, ext, func(ctx context.Context, r io.Reader, w io.Writer) error {
		_, err := io.Copy(w, r)
		return err
	})
}

func CopyOutputTo(out *Output, w io.Writer) (int64, error) {
	sr := out.Reader()
	rc, err := sr.Open()
	if err != nil {
		return 0, err
	}
	defer rc.Close()
	defer sr.Cleanup()

	return io.Copy(w, rc)
}
