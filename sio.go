package sio

import (
	"bytes"
	"context"
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
	ErrNilSource      = errors.New("sio: nil source")
	ErrOpenFailed     = errors.New("sio: cannot open reader")
	ErrManagerClosed  = errors.New("sio: manager is closed")
	ErrSessionClosed  = errors.New("sio: session is closed")
	ErrInvalidURL     = errors.New("sio: invalid URL")
	ErrDownloadFailed = errors.New("sio: download failed")
)

var httpClient = &http.Client{Timeout: 30 * time.Second}

/* -------------------------------------------------------------------------- */
/*                           StreamReader Abstraction                           */
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
func (f *FileReader) Cleanup() error               { return nil }

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

// URLReader lazily downloads the content from a URL into a temporary file.
// The temp file is deleted by Cleanup().
type URLReader struct {
	URL string
	tmp string
}

func NewURLReader(urlStr string) *URLReader { return &URLReader{URL: urlStr} }

func (u *URLReader) Open() (io.ReadCloser, error) {
	if u.tmp == "" {
		if err := u.download(); err != nil {
			return nil, err
		}
	}
	return os.Open(u.tmp)
}

func (u *URLReader) download() error {
	parsed, err := url.Parse(u.URL)
	if err != nil {
		return ErrInvalidURL
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return ErrInvalidURL
	}

	resp, err := httpClient.Get(u.URL)
	if err != nil {
		return ErrDownloadFailed
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ErrDownloadFailed
	}

	tmpFile, err := os.CreateTemp("", "sio-url-*")
	if err != nil {
		return err
	}
	defer tmpFile.Close()

	if _, err := io.Copy(tmpFile, resp.Body); err != nil {
		_ = os.Remove(tmpFile.Name())
		return err
	}

	u.tmp = tmpFile.Name()
	return nil
}

func (u *URLReader) Cleanup() error {
	if u.tmp == "" {
		return nil
	}
	err := os.Remove(u.tmp)
	u.tmp = ""
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

/* -------------------------------------------------------------------------- */
/*                               Manager                                       */
/* -------------------------------------------------------------------------- */

type Manager interface {
	NewSession() (Session, error)
	Cleanup() error
}

// Manager manages a root directory under which multiple Sessions
// create their own isolated working directories.
type manager struct {
	mu      sync.Mutex
	baseDir string
	closed  bool
}

// NewManager :
//   - baseDir == "" → create a temp folder using os.MkdirTemp("", "sio-")
//   - baseDir != "" → create/use the provided directory
func NewManager(baseDir string) (Manager, error) {
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

// BaseDir returns the underlying root directory managed by this Manager.
func (m *manager) BaseDir() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.baseDir
}

// NewSession creates a new isolated Session under baseDir.
func (m *manager) NewSession() (Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil, ErrManagerClosed
	}

	id := uuid.New().String()
	dir := filepath.Join(m.baseDir, id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	return &session{manager: m, dir: dir}, nil
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
/*                                Session                                      */
/* -------------------------------------------------------------------------- */

type Session interface {
	Process(ctx context.Context, src StreamReader, outExt string, fn func(ctx context.Context, r io.Reader, w io.Writer) error) (*Output, error)
	Read(ctx context.Context, src StreamReader, fn func(ctx context.Context, r io.Reader) error) error
	Cleanup() error
}

// session represents an isolated working directory where all temporary files
// and transformation outputs are stored during a single job or request.
type session struct {
	mu      sync.Mutex
	manager Manager
	dir     string
	closed  bool
	outputs []*Output // tracked outputs inside this session
}

// Dir returns the directory assigned to this Session.
func (s *session) Dir() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dir
}

func (s *session) ensureOpen() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrSessionClosed
	}
	return nil
}

// newOutput creates a new temporary file inside the session directory and tracks it.
func (s *session) newOutput(ext string) (*Output, error) {
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
//  1. Open src (r)
//  2. Create Output (w)
//  3. Run fn(ctx, r, w)
//
// Returns the Output on success.
func (s *session) Process(
	ctx context.Context,
	src StreamReader,
	outExt string,
	fn func(ctx context.Context, r io.Reader, w io.Writer) error,
) (*Output, error) {

	if src == nil {
		return nil, ErrNilSource
	}
	if fn == nil {
		return nil, fmt.Errorf("sio: Session: fn is nil")
	}
	if err := s.ensureOpen(); err != nil {
		return nil, err
	}

	r, err := src.Open()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrOpenFailed, err)
	}
	defer r.Close()
	defer src.Cleanup()

	out, err := s.newOutput(outExt)
	if err != nil {
		return nil, err
	}

	w, err := out.OpenWriter()
	if err != nil {
		out.Cleanup()
		return nil, err
	}
	defer w.Close()

	if err := fn(ctx, r, w); err != nil {
		out.Cleanup()
		return nil, err
	}

	return out, nil
}

// Read simply opens the StreamReader and passes it to fn.
// No Output file is created.
func (s *session) Read(
	ctx context.Context,
	src StreamReader,
	fn func(ctx context.Context, r io.Reader) error,
) error {

	if src == nil {
		return ErrNilSource
	}
	if fn == nil {
		return fmt.Errorf("sio: Read: fn is nil")
	}
	if err := s.ensureOpen(); err != nil {
		return err
	}

	r, err := src.Open()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrOpenFailed, err)
	}
	defer r.Close()
	defer src.Cleanup()

	return fn(ctx, r)
}

/* ---------- Session Cleanup Handling ---------- */

// isKeptPath returns true if the given path belongs to an Output marked as kept.
func (s *session) isKeptPath(path string) bool {
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
// Kept files survive Session.Cleanup().
// The directory itself is removed if empty.
func (s *session) Cleanup() error {
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

// Output represents a file produced by a Session.Process call.
// It can be kept beyond session cleanup via Output.Keep().
type Output struct {
	mu     sync.Mutex
	path   string
	ses    Session
	closed bool
	keep   bool
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

func (o *Output) AsStreamReader() StreamReader {
	return NewFileReader(o.Path())
}

func (o *Output) Cleanup() error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return nil
	}
	o.closed = true

	err := os.Remove(o.path)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

// Keep marks the output file as persistent.
// Session.Cleanup() will NOT delete the file.
// This file will survive until Output.Cleanup() or Manager.Cleanup().
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
/*                       Global Default Manager Support                        */
/* -------------------------------------------------------------------------- */

var (
	defaultMgrMu sync.Mutex
	defaultMgr   Manager
)

func useDefaultManager() (Manager, error) {
	defaultMgrMu.Lock()
	defer defaultMgrMu.Unlock()

	if defaultMgr != nil {
		return defaultMgr, nil
	}

	m, err := NewManager("")
	if err != nil {
		return nil, err
	}

	defaultMgr = m
	return m, nil
}

// InitManager configures the global default Manager with the given baseDir.
// Usually called once at application startup.
func InitManager(baseDir string) (Manager, error) {
	defaultMgrMu.Lock()
	defer defaultMgrMu.Unlock()

	if defaultMgr != nil {
		return defaultMgr, nil
	}

	m, err := NewManager(baseDir)
	if err != nil {
		return nil, err
	}

	defaultMgr = m
	return m, nil
}

// ShutdownDefault removes the entire baseDir of the global Manager.
func ShutdownDefault() error {
	defaultMgrMu.Lock()
	defer defaultMgrMu.Unlock()

	if defaultMgr == nil {
		return nil
	}

	err := defaultMgr.Cleanup()
	defaultMgr = nil
	return err
}

/* -------------------------------------------------------------------------- */
/*                         Global Read / Process Helpers                       */
/* -------------------------------------------------------------------------- */

// Read creates a temporary Session, executes Session.Read,
// then automatically cleans up the Session.
func Read(
	ctx context.Context,
	src StreamReader,
	fn func(ctx context.Context, r io.Reader) error,
) error {

	m, err := useDefaultManager()
	if err != nil {
		return err
	}

	s, err := m.NewSession()
	if err != nil {
		return err
	}
	defer s.Cleanup()

	return s.Read(ctx, src, fn)
}

// Process creates a temporary Session, executes Session.Process,
// marks the Output as kept, cleans up the Session (removing non-kept files),
// and returns the final kept Output.
func Process(ctx context.Context, src StreamReader, outExt string, fn func(ctx context.Context, r io.Reader, w io.Writer) error) (*Output, error) {
	m, err := useDefaultManager()
	if err != nil {
		return nil, err
	}

	s, err := m.NewSession()
	if err != nil {
		return nil, err
	}

	out, err := s.Process(ctx, src, outExt, fn)
	if err != nil {
		s.Cleanup()
		return nil, err
	}

	out.Keep()
	s.Cleanup()
	return out, nil
}

/* -------------------------------------------------------------------------- */
/*                   Utility: Wrap io.Reader as StreamReader                   */
/* -------------------------------------------------------------------------- */

// NewStreamReaderFromReader loads an io.Reader into memory and returns
// a BytesReader. This is useful when integrating APIs that expose only
// io.Reader without access to the underlying file.
//
// NOTE: This loads the full content into memory and is not suitable
// for extremely large streams.
func NewStreamReaderFromReader(r io.Reader) StreamReader {
	buf, _ := io.ReadAll(r)
	return NewBytesReader(buf)
}

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
		if readerCloseErr := d.reader.Close(); readerCloseErr != nil {
			errs = errors.Join(errs, readerCloseErr)
		}
		d.reader = nil
	}

	// 2) Cleanup the StreamReader (e.g., remove temp files)
	if d.streamReader != nil {
		if streamReaderCleanupErr := d.streamReader.Cleanup(); streamReaderCleanupErr != nil {
			errs = errors.Join(errs, streamReaderCleanupErr)
		}
		d.streamReader = nil
	}

	// 3) Run the optional extra cleanup callback
	if d.Cleanup != nil {
		d.Cleanup()
		d.Cleanup = nil
	}

	return errs
}

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
