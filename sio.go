package sio

import (
	"bufio"
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

const (
	Pdf   = ".pdf"
	Text  = ".txt"
	Csv   = ".csv"
	Jpg   = ".jpg"
	Png   = ".png"
	Excel = ".xlsx"
	Docx  = ".docx"
	Pptx  = ".pptx"
	Zip   = ".zip"
)

func ToExt(format string) string {
	return "." + format
}

/* ---------- Errors ---------- */

var (
	ErrNilSource              = errors.New("sio: nil source")
	ErrOpenFailed             = errors.New("sio: cannot open reader")
	ErrIoManagerClosed        = errors.New("sio: manager is closed")
	ErrIoSessionClosed        = errors.New("sio: session is closed")
	ErrInvalidURL             = errors.New("sio: invalid URL")
	ErrDownloadFailed         = errors.New("sio: download failed")
	ErrNoSession              = errors.New("sio: session is nil")
	ErrFileStorageUnavailable = errors.New("sio: file storage requires directory (manager created with StorageMemory only)")
	ErrInvalidSessionType     = errors.New("sio: invalid session type")
)

const (
	DefaultBaseTempDir = "./temp"
)

/* -------------------------------------------------------------------------- */
/*                              Storage Types                                  */
/* -------------------------------------------------------------------------- */

// StorageType defines how the IoManager stores data
type StorageType int

const (
	// StorageFile stores data as temporary files on disk (default)
	StorageFile StorageType = iota
	// StorageMemory stores data in memory as byte slices
	StorageMemory
)

// Shorthand aliases
const (
	Memory = StorageMemory
	File   = StorageFile
)

// Storage converts string to StorageType.
// Supports: "file", "memory"
func Storage(s string) StorageType {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "memory":
		return StorageMemory
	default: // "file" or unknown
		return StorageFile
	}
}

// String returns string representation of StorageType
func (s StorageType) String() string {
	switch s {
	case StorageMemory:
		return "memory"
	case StorageFile:
		return "file"
	default:
		return "file"
	}
}

/* -------------------------------------------------------------------------- */
/*                             Output Options                                  */
/* -------------------------------------------------------------------------- */

// OutConfig configures output behavior
type OutConfig struct {
	ext         string
	storageType *StorageType // nil = use session default
}

// Out creates output configuration.
//
// Usage:
//
//	sio.Out(".pdf")                        // use session default
//	sio.Out(".pdf", sio.Memory)            // force memory storage
//	sio.Out(".pdf", sio.File)              // force file storage
//	sio.Out(".pdf", sio.Storage("memory")) // from string config
func Out(ext string, opts ...StorageType) OutConfig {
	o := OutConfig{ext: ext}
	if len(opts) > 0 {
		o.storageType = &opts[0]
	}
	return o
}

// getStorageType returns the storage type, using session default if not specified
func (o OutConfig) getStorageType(sessionDefault StorageType) StorageType {
	if o.storageType != nil {
		return *o.storageType
	}
	return sessionDefault
}

// Ext returns the output extension.
func (o OutConfig) Ext() string {
	return o.ext
}

// StorageType returns the configured storage type, or nil when unset.
func (o OutConfig) StorageType() *StorageType {
	return o.storageType
}

/* -------------------------------------------------------------------------- */
/*                              HTTP Client                                    */
/* -------------------------------------------------------------------------- */

var httpClient = &http.Client{Timeout: 30 * time.Second}

type Config struct {
	client *http.Client
}

// NewConfig constructs a Config with a custom HTTP client.
func NewConfig(client *http.Client) Config {
	return Config{client: client}
}

// WithClient sets the HTTP client on a Config.
func (c Config) WithClient(client *http.Client) Config {
	c.client = client
	return c
}

func Configure(config Config) error {
	if config.client != nil {
		httpClient = config.client
	}
	return nil
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
	path string
}

func NewFileReader(path string) *FileReader { return &FileReader{path: path} }

func (f *FileReader) Open() (io.ReadCloser, error) { return os.Open(f.path) }

func (f *FileReader) Size() int64 {
	fi, err := os.Stat(f.path)
	if err != nil {
		return -1
	}
	return fi.Size()
}

func (f *FileReader) Cleanup() error {
	return nil
}

// Path returns the underlying file path.
func (f *FileReader) Path() string {
	return f.path
}

/* -------------------------------------------------------------------------- */
/*                             MultipartReader                                 */
/* -------------------------------------------------------------------------- */

// MultipartReader wraps an uploaded multipart.FileHeader.
// Cleanup() does nothing because frameworks handle their own temp files.
type MultipartReader struct {
	file *multipart.FileHeader
}

func NewMultipartReader(fh *multipart.FileHeader) *MultipartReader {
	return &MultipartReader{file: fh}
}

func (m *MultipartReader) Open() (io.ReadCloser, error) { return m.file.Open() }
func (m *MultipartReader) Cleanup() error               { return nil }

/* -------------------------------------------------------------------------- */
/*                                 GenericReader                               */
/* -------------------------------------------------------------------------- */

// GenericReader wraps an uploaded io.Reader.
// Cleanup() does nothing because frameworks handle their own temp files.
type GenericReader struct {
	file io.Reader
}

func NewGenericReader(r io.Reader) *GenericReader {
	return &GenericReader{file: r}
}

func (m *GenericReader) Open() (io.ReadCloser, error) {
	if m.file == nil {
		return nil, fmt.Errorf("sio: GenericReader: underlying reader is nil")
	}

	return NopCloser(m.file), nil
}

func (m *GenericReader) Cleanup() error { return nil }

/* -------------------------------------------------------------------------- */
/*                                BytesReader                                  */
/* -------------------------------------------------------------------------- */

// BytesReader exposes an in-memory []byte as a StreamReader.
// Cleanup() optionally clears the underlying buffer.
type BytesReader struct {
	data []byte
}

func NewBytesReader(data []byte) *BytesReader {
	return &BytesReader{data: data}
}

func (b *BytesReader) Open() (io.ReadCloser, error) {
	return NopCloser(bytes.NewReader(b.data)), nil
}

func (b *BytesReader) Size() int64 {
	return int64(len(b.data))
}

func (b *BytesReader) Cleanup() error {
	b.data = nil
	return nil
}

// Data returns the underlying byte slice.
func (b *BytesReader) Data() []byte {
	return b.data
}

/* -------------------------------------------------------------------------- */
/*                                 URLReader                                   */
/* -------------------------------------------------------------------------- */

type URLReaderOptions struct {
	client      *http.Client
	insecureTLS bool
	timeout     time.Duration
}

// WithClient sets a custom HTTP client for URLReaderOptions.
func (o URLReaderOptions) WithClient(client *http.Client) URLReaderOptions {
	o.client = client
	return o
}

// WithInsecureTLS configures TLS verification for URLReaderOptions.
func (o URLReaderOptions) WithInsecureTLS(insecure bool) URLReaderOptions {
	o.insecureTLS = insecure
	return o
}

// WithTimeout sets the timeout for URLReaderOptions.
func (o URLReaderOptions) WithTimeout(timeout time.Duration) URLReaderOptions {
	o.timeout = timeout
	return o
}

// URLReader streams content directly from a URL.
// Each Open() call creates a new HTTP request.
// Cleanup() is a no-op since the response body is closed via the returned ReadCloser.
type URLReader struct {
	url    string
	client *http.Client
}

func NewURLReader(urlStr string, opts ...URLReaderOptions) *URLReader {
	r := &URLReader{url: urlStr}

	baseTimeout := httpClient.Timeout
	if baseTimeout == 0 {
		baseTimeout = 30 * time.Second
	}

	if len(opts) == 0 {
		r.client = httpClient
		return r
	}

	opt := opts[0]

	if opt.client != nil {
		r.client = opt.client
		return r
	}

	c := &http.Client{
		Timeout: baseTimeout,
	}

	if opt.timeout > 0 {
		c.Timeout = opt.timeout
	}

	if opt.insecureTLS {
		c.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true, //nolint:gosec
			},
		}
	}

	r.client = c
	return r
}

func (u *URLReader) Open() (io.ReadCloser, error) {
	parsed, err := url.Parse(u.url)
	if err != nil {
		return nil, ErrInvalidURL
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, ErrInvalidURL
	}

	resp, err := u.client.Get(u.url)
	if err != nil {
		return nil, ErrDownloadFailed
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_ = resp.Body.Close()
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

// manager manages a root directory under which multiple IoSessions
// create their own isolated working directories.
type manager struct {
	mu          sync.Mutex
	baseDir     string
	closed      bool
	storageType StorageType
}

// NewIoManager creates a new IoManager.
//   - baseDir == "" → create a temp folder using os.MkdirTemp("", "sio-")
//   - baseDir != "" → create/use the provided directory
//   - storageType: optional parameter to specify storage strategy (default: StorageFile)
func NewIoManager(baseDir string, storageType ...StorageType) (IoManager, error) {
	st := StorageFile // default
	if len(storageType) > 0 {
		st = storageType[0]
	}

	// For StorageMemory, we don't need a directory
	if st == StorageMemory {
		return &manager{
			baseDir:     "",
			storageType: st,
		}, nil
	}

	// For StorageFile, create/use the directory
	if strings.TrimSpace(baseDir) == "" {
		dir, err := os.MkdirTemp("", "sio-")
		if err != nil {
			return nil, err
		}
		return &manager{
			baseDir:     dir,
			storageType: st,
		}, nil
	}

	baseDir = filepath.Clean(baseDir)
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, err
	}

	return &manager{
		baseDir:     baseDir,
		storageType: st,
	}, nil
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

	// For StorageMemory mode, no directory is needed
	if m.storageType == StorageMemory {
		return &ioSession{
			manager:     m,
			dir:         "",
			storageType: m.storageType,
		}, nil
	}

	// For StorageFile mode, create session directory
	id := uuid.New().String()
	dir := filepath.Join(m.baseDir, id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	return &ioSession{
		manager:     m,
		dir:         dir,
		storageType: m.storageType,
	}, nil
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
/*                                IoSession                                    */
/* -------------------------------------------------------------------------- */

type ReadFunc func(ctx context.Context, r io.Reader) error
type ReadListFunc func(ctx context.Context, readers []io.Reader) error
type ProcessFunc func(ctx context.Context, r io.Reader, w io.Writer) error
type ProcessListFunc func(ctx context.Context, readers []io.Reader, w io.Writer) error

type IoSession interface {
	Read(ctx context.Context, source StreamReader, fn ReadFunc) error
	ReadList(ctx context.Context, sources []StreamReader, fn ReadListFunc) error

	Process(ctx context.Context, source StreamReader, out OutConfig, fn ProcessFunc) (*Output, error)
	ProcessList(ctx context.Context, sources []StreamReader, out OutConfig, fn ProcessListFunc) (*Output, error)

	Cleanup() error
}

// ioSession represents an isolated working directory where all temporary files
// and transformation outputs are stored during a single job or request.
type ioSession struct {
	mu          sync.Mutex
	manager     IoManager
	dir         string
	closed      bool
	outputs     []*Output // tracked outputs inside this session
	storageType StorageType
}

// Dir returns the directory assigned to this IoSession.
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

// newOutputWithStorage creates output with specified storage type.
func (s *ioSession) newOutputWithStorage(ext string, storageType StorageType) (*Output, error) {
	if err := s.ensureOpen(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// For StorageMemory mode, create an in-memory output
	if storageType == StorageMemory {
		out := &Output{
			path:        "",
			ses:         s,
			data:        nil,
			storageType: StorageMemory,
		}
		s.outputs = append(s.outputs, out)
		return out, nil
	}

	// StorageFile requires directory
	if s.dir == "" {
		return nil, ErrFileStorageUnavailable
	}

	// For StorageFile mode, create a temporary file
	pattern := "*"
	if ext != "" {
		pattern += ext
	}

	f, err := os.CreateTemp(s.dir, pattern)
	if err != nil {
		return nil, err
	}
	_ = f.Close()

	out := &Output{
		path:        f.Name(),
		ses:         s,
		storageType: StorageFile,
	}
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
	out OutConfig,
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

	storageType := out.getStorageType(s.storageType)
	output, err := s.newOutputWithStorage(out.ext, storageType)
	if err != nil {
		return nil, err
	}

	w, err := output.OpenWriter()
	if err != nil {
		_ = output.cleanup()
		return nil, err
	}
	defer w.Close()

	if err := fn(ctx, r, w); err != nil {
		_ = output.cleanup()
		return nil, err
	}

	return output, nil
}

// ProcessList opens all sources, passes the readers to fn along with a writer,
// and returns an Output on success.
func (s *ioSession) ProcessList(
	ctx context.Context,
	sources []StreamReader,
	out OutConfig,
	fn ProcessListFunc,
) (*Output, error) {
	if len(sources) == 0 {
		return nil, fmt.Errorf("sio: empty source list")
	}
	if fn == nil {
		return nil, fmt.Errorf("sio: ProcessList: fn is nil")
	}
	if err := s.ensureOpen(); err != nil {
		return nil, err
	}

	rl, err := OpenReaderList(sources)
	if err != nil {
		return nil, err
	}
	defer rl.Close()

	storageType := out.getStorageType(s.storageType)
	output, err := s.newOutputWithStorage(out.ext, storageType)
	if err != nil {
		return nil, err
	}

	w, err := output.OpenWriter()
	if err != nil {
		_ = output.cleanup()
		return nil, err
	}
	defer w.Close()

	if err := fn(ctx, rl.readers, w); err != nil {
		_ = output.cleanup()
		return nil, err
	}

	return output, nil
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

	if err := s.ensureOpen(); err != nil {
		return err
	}

	rl, err := OpenReaderList(sources)
	if err != nil {
		return err
	}
	defer rl.Close()

	return fn(ctx, rl.readers)
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

	// For StorageMemory mode, just clean up in-memory outputs
	if s.storageType == StorageMemory {
		var firstErr error
		for _, out := range s.outputs {
			out.mu.Lock()
			shouldSkip := out.keep && !out.closed
			out.mu.Unlock()
			if shouldSkip {
				continue
			}
			if err := out.cleanup(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		return firstErr
	}

	// For StorageFile mode, clean up files
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
/*                           Storage Helper Types                              */
/* -------------------------------------------------------------------------- */

// bytesWriteCloser wraps a bytes.Buffer to implement io.WriteCloser
type bytesWriteCloser struct {
	buf    *bytes.Buffer
	output *Output
}

func (b *bytesWriteCloser) Write(p []byte) (int, error) {
	return b.buf.Write(p)
}

func (b *bytesWriteCloser) Close() error {
	if b.output != nil {
		b.output.mu.Lock()
		// Zero-copy: give buffer's backing array to Output
		b.output.data = b.buf.Bytes()
		b.output.mu.Unlock()
	}
	// Let GC reclaim b.buf when Output is cleaned
	return nil
}

// readerAtReader is any type that implements both io.Reader and io.ReaderAt.
type readerAtReader struct {
	io.Reader
	io.ReaderAt
}

// readerAtReadCloser preserves ReaderAt while adding a no-op Close().
type readerAtReadCloser struct {
	*readerAtReader
}

func (r readerAtReadCloser) Close() error { return nil }

func NopCloser(r io.Reader) io.ReadCloser {
	if r == nil {
		return nil
	}

	// Already a ReadCloser - return as-is
	if rc, ok := r.(io.ReadCloser); ok {
		return rc
	}

	if rc, ok := r.(readerAtReadCloser); ok {
		return rc
	}

	// Preserve ReaderAt capability if present
	if ra, ok := r.(io.ReaderAt); ok {
		return readerAtReadCloser{
			&readerAtReader{
				Reader:   r,
				ReaderAt: ra,
			},
		}
	}

	// Simple case - just wrap with no-op closer
	return io.NopCloser(r)
}

/* -------------------------------------------------------------------------- */
/*                                  Output                                     */
/* -------------------------------------------------------------------------- */

// Output represents a file produced by a IoSession.Process call.
// It can be kept beyond session cleanup via Output.Keep().
type Output struct {
	mu     sync.Mutex
	path   string
	ses    IoSession
	closed bool
	keep   bool
	// For StorageMemory mode
	data        []byte
	storageType StorageType
}

func (o *Output) Path() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.path
}

// StorageType returns the storage type of this output.
func (o *Output) StorageType() StorageType {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.storageType
}

func (o *Output) OpenReader() (io.ReadCloser, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return nil, fmt.Errorf("sio: output is cleaned up")
	}

	// For StorageMemory mode, return reader from memory
	if o.storageType == StorageMemory {
		if o.data == nil {
			return NopCloser(bytes.NewReader([]byte{})), nil
		}
		return NopCloser(bytes.NewReader(o.data)), nil
	}

	// For StorageFile mode, return file reader
	return os.Open(o.path)
}

func (o *Output) OpenWriter() (io.WriteCloser, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return nil, fmt.Errorf("sio: output is cleaned up")
	}

	if o.storageType == StorageMemory {
		return &bytesWriteCloser{
			buf:    &bytes.Buffer{},
			output: o,
		}, nil
	}

	return os.Create(o.path)
}

func (o *Output) Reader() StreamReader {
	o.mu.Lock()
	defer o.mu.Unlock()

	var sr StreamReader
	if o.storageType == StorageMemory {
		sr = NewBytesReader(o.data)
	} else {
		sr = NewFileReader(o.path)
	}

	return sr
}

// Data returns the raw bytes for StorageMemory mode.
// Returns nil for StorageFile mode.
// The returned slice is owned by Output - do not modify.
func (o *Output) Data() []byte {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.storageType == StorageMemory {
		return o.data
	}
	return nil
}

// cleanup is an internal helper that removes the output file and any detached file.
func (o *Output) cleanup() error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return nil
	}
	o.closed = true

	if o.storageType == StorageMemory {
		o.data = nil
		return nil
	}

	err := os.Remove(o.path)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

// Keep marks the output file as persistent.
// IoSession.Cleanup() will NOT delete the file.
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

	_, err = copyToFile(r, path)
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

func (o *Output) WriteTo(w io.Writer) (int64, error) {
	r, err := o.OpenReader()
	if err != nil {
		return 0, err
	}
	defer r.Close()
	return Copy(w, r)
}

func (o *Output) AsReaderAt(ctx context.Context, opts ...ToReaderAtOption) (*ReaderAtResult, error) {
	r, err := o.OpenReader()
	if err != nil {
		return nil, err
	}
	return ToReaderAt(ctx, r, opts...)
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
func In(sr StreamReader, opts ...InOption) StreamReader {
	if sr == nil {
		return nil
	}

	cfg := &inConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	if !cfg.deleteAfterProcess {
		return sr
	}

	fr, ok := sr.(*FileReader)
	if !ok {
		return sr
	}

	filePath := fr.Path()

	extra := func() error {
		if filePath == "" {
			return nil
		}
		err := os.Remove(filePath)
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
func NewPartReader(p *multipart.Part) *GenericReader {
	return NewGenericReader(p)
}

/* -------------------------------------------------------------------------- */
/*                       DownloadReaderCloser helper                           */
/* -------------------------------------------------------------------------- */

// DownloadReaderCloser is a convenience interface for a read-only stream
// that also supports Close.
type DownloadReaderCloser interface {
	io.Reader
	io.Closer
}

type downloadReaderCloser struct {
	reader       io.ReadCloser
	streamReader StreamReader
	cleanup      func()
}

func (d *downloadReaderCloser) Read(p []byte) (int, error) {
	if d.reader == nil {
		return 0, io.ErrClosedPipe
	}
	return d.reader.Read(p)
}

func (d *downloadReaderCloser) Close() error {
	var errs error

	if d.reader != nil {
		if err := d.reader.Close(); err != nil {
			errs = errors.Join(errs, err)
		}
		d.reader = nil
	}

	if d.streamReader != nil {
		if err := d.streamReader.Cleanup(); err != nil {
			errs = errors.Join(errs, err)
		}
		d.streamReader = nil
	}

	if d.cleanup != nil {
		d.cleanup()
		d.cleanup = nil
	}

	return errs
}

// NewDownloadReaderCloser wraps a StreamReader as a DownloadReaderCloser.
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
		closer.cleanup = cleanup[0]
	}

	return closer, nil
}

/* -------------------------------------------------------------------------- */
/*                         StreamReader List Utilities                         */
/* -------------------------------------------------------------------------- */

// ReaderList holds opened readers and provides cleanup functionality.
type ReaderList struct {
	readers []io.Reader
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

	rl.readers = nil
	rl.closers = nil
	rl.sources = nil

	return errs
}

// OpenReaderList opens all StreamReaders and returns a ReaderList.
func OpenReaderList(sources []StreamReader) (*ReaderList, error) {
	if len(sources) == 0 {
		return &ReaderList{}, nil
	}

	rl := &ReaderList{
		readers: make([]io.Reader, 0, len(sources)),
		closers: make([]io.Closer, 0, len(sources)),
		sources: make([]StreamReader, 0, len(sources)),
	}

	for _, source := range sources {
		if source == nil {
			_ = rl.Close()
			return nil, ErrNilSource
		}

		rc, err := source.Open()
		if err != nil {
			_ = rl.Close()
			return nil, fmt.Errorf("%w: %v", ErrOpenFailed, err)
		}

		rl.readers = append(rl.readers, rc)
		rl.closers = append(rl.closers, rc)
		rl.sources = append(rl.sources, source)
	}

	return rl, nil
}

// Readers returns the list of opened readers.
func (rl *ReaderList) Readers() []io.Reader {
	if rl == nil {
		return nil
	}
	return rl.readers
}

/* -------------------------------------------------------------------------- */
/*                            Context Helpers                                  */
/* -------------------------------------------------------------------------- */

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

/* -------------------------------------------------------------------------- */
/*                         Package-level Functions                             */
/* -------------------------------------------------------------------------- */

// Read is a convenience wrapper that gets IoSession from context
// and calls ses.Read(...). If there is no session, it falls back
// to direct streaming from the StreamReader.
func Read(ctx context.Context, source StreamReader, fn ReadFunc) error {
	if source == nil {
		return ErrNilSource
	}
	if fn == nil {
		return fmt.Errorf("sio: Read: fn is nil")
	}

	if ses := Session(ctx); ses != nil {
		return ses.Read(ctx, source, fn)
	}

	// direct read
	r, err := source.Open()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrOpenFailed, err)
	}
	defer r.Close()
	defer source.Cleanup()

	return fn(ctx, r)
}

// ReadList is a convenience wrapper for IoSession.ReadList.
func ReadList(ctx context.Context, sources []StreamReader, fn ReadListFunc) error {
	if len(sources) == 0 {
		return ErrNilSource
	}
	if fn == nil {
		return fmt.Errorf("sio: ReadList: fn is nil")
	}

	if ses := Session(ctx); ses != nil {
		return ses.ReadList(ctx, sources, fn)
	}

	// direct read
	rl, err := OpenReaderList(sources)
	if err != nil {
		return err
	}
	defer rl.Close()

	return fn(ctx, rl.readers)
}

// Process is a convenience wrapper for IoSession.Process.
func Process(ctx context.Context, source StreamReader, out OutConfig, fn ProcessFunc) (*Output, error) {
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

	return ses.Process(ctx, source, out, fn)
}

// ProcessList is a convenience wrapper for IoSession.ProcessList.
func ProcessList(ctx context.Context, sources []StreamReader, out OutConfig, fn ProcessListFunc) (*Output, error) {
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

	return ses.ProcessList(ctx, sources, out, fn)
}

func ReadResult[T any](ctx context.Context, source StreamReader, fn func(ctx context.Context, r io.Reader) (*T, error)) (*T, error) {
	if source == nil {
		return nil, ErrNilSource
	}
	if fn == nil {
		return nil, fmt.Errorf("sio: ReadResult: fn is nil")
	}

	var result *T
	err := Read(ctx, source, func(ctx context.Context, r io.Reader) error {
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

func ReadListResult[T any](ctx context.Context, sources []StreamReader, fn func(ctx context.Context, readers []io.Reader) (*T, error)) (*T, error) {
	if len(sources) == 0 {
		return nil, fmt.Errorf("sio: ReadListResult: empty sources")
	}
	if fn == nil {
		return nil, fmt.Errorf("sio: ReadListResult: fn is nil")
	}

	var result *T
	err := ReadList(ctx, sources, func(ctx context.Context, readers []io.Reader) error {
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

func ProcessResult[T any](ctx context.Context, source StreamReader, out OutConfig, fn func(ctx context.Context, r io.Reader, w io.Writer) (*T, error)) (*Output, *T, error) {
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
	output, err := ses.Process(ctx, source, out, func(ctx context.Context, r io.Reader, w io.Writer) error {
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
	return output, result, nil
}

func ProcessListResult[T any](ctx context.Context, sources []StreamReader, out OutConfig, fn func(ctx context.Context, readers []io.Reader, w io.Writer) (*T, error)) (*Output, *T, error) {
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
	output, err := ses.ProcessList(ctx, sources, out, func(ctx context.Context, readers []io.Reader, w io.Writer) error {
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
	return output, result, nil
}

func ToOutput(ctx context.Context, src StreamReader, out OutConfig) (*Output, error) {
	return Process(ctx, src, out, func(ctx context.Context, r io.Reader, w io.Writer) error {
		_, err := Copy(w, r)
		return err
	})
}

func WriteFile(r io.Reader, path string) (int64, error) {
	if r == nil {
		return 0, ErrNilSource
	}
	return copyToFile(r, path)
}

func WriteStreamToFile(src StreamReader, path string) (int64, error) {
	if src == nil {
		return 0, ErrNilSource
	}

	rc, err := src.Open()
	if err != nil {
		return 0, err
	}
	defer rc.Close()
	defer src.Cleanup()

	return WriteFile(rc, path)
}

type LineFunc func(line string) error

func ReadLines(ctx context.Context, src StreamReader, fn LineFunc) error {
	if src == nil {
		return ErrNilSource
	}
	if fn == nil {
		return nil
	}

	return Read(ctx, src, func(ctx context.Context, r io.Reader) error {
		scanner := bufio.NewScanner(r)

		buf := make([]byte, 0, 64*1024) // 64KB
		scanner.Buffer(buf, 1024*1024)  // max 1MB

		for scanner.Scan() {
			line := scanner.Text()
			if err := fn(line); err != nil {
				return err
			}
		}
		return scanner.Err()
	})
}

func ReadFileLines(ctx context.Context, path string, fn LineFunc) error {
	src := NewFileReader(path)
	return ReadLines(ctx, src, fn)
}

func Size(ctx context.Context, source StreamReader) (int64, error) {
	fileSize, err := ReadResult[int64](ctx, source, func(ctx context.Context, r io.Reader) (*int64, error) {
		fileSize := SizeFromReader(r)
		return &fileSize, nil
	})
	if err != nil {
		return 0, err
	}
	return *fileSize, nil
}

func SizeFromStream(sr StreamReader) int64 {
	if s, ok := sr.(sizer); ok {
		return s.Size()
	}
	return -1
}

func SizeFromReader(r io.Reader) int64 {
	switch v := r.(type) {
	case readerAtReadCloser:
		if v.readerAtReader != nil && v.readerAtReader.Reader != nil {
			return SizeFromReader(v.readerAtReader.Reader)
		}
	case *readerAtReadCloser:
		if v != nil && v.readerAtReader != nil && v.readerAtReader.Reader != nil {
			return SizeFromReader(v.readerAtReader.Reader)
		}

	case *os.File:
		if fi, err := v.Stat(); err == nil {
			return fi.Size()
		}

	case *bytes.Reader:
		return v.Size()

	case *strings.Reader:
		return int64(v.Size())

	case *bytes.Buffer:
		return int64(v.Len())

	case interface{ Size() int64 }:
		if n := v.Size(); n >= 0 {
			return n
		}

	case interface{ Len() int }:
		return int64(v.Len())

	case io.ReadSeeker:
		cur, err := v.Seek(0, io.SeekCurrent)
		if err != nil {
			return -1
		}
		end, err := v.Seek(0, io.SeekEnd)
		if err != nil {
			return -1
		}
		if _, err := v.Seek(cur, io.SeekStart); err != nil {
			return -1
		}
		return end
	}
	return -1
}

func SizeFromPath(path string) (int64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

/* -------------------------------------------------------------------------- */
/*                              Copy Utilities                                 */
/* -------------------------------------------------------------------------- */

func Copy(dst io.Writer, src io.Reader) (int64, error) {
	return io.Copy(dst, src)
}

func copyToFile(src io.Reader, dstPath string) (int64, error) {
	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		return 0, err
	}

	f, err := os.Create(dstPath)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	return Copy(f, src)
}

type ReaderAtResult struct {
	readerAt io.ReaderAt
	size     int64
	cleanup  func() error
	source   string // "direct" | "memory" | "tempFile"
}

// ReaderAt exposes the underlying ReaderAt.
func (r *ReaderAtResult) ReaderAt() io.ReaderAt {
	if r == nil {
		return nil
	}
	return r.readerAt
}

// Size returns the byte size of the underlying data when known.
func (r *ReaderAtResult) Size() int64 {
	if r == nil {
		return 0
	}
	return r.size
}

// Cleanup releases any resources created by ToReaderAt.
func (r *ReaderAtResult) Cleanup() error {
	if r == nil || r.cleanup == nil {
		return nil
	}
	return r.cleanup()
}

// Source reports where the ReaderAt was sourced from.
func (r *ReaderAtResult) Source() string {
	if r == nil {
		return ""
	}
	return r.source
}

type ToReaderAtOptions struct {
	maxMemoryBytes int64  // if <= 0 → always spool to temp file
	tempDir        string // optional; if empty session dir will be used
	tempPattern    string
}

type ToReaderAtOption func(*ToReaderAtOptions)

func WithMaxMemoryBytes(n int64) ToReaderAtOption {
	return func(o *ToReaderAtOptions) { o.maxMemoryBytes = n }
}

func WithTempDir(dir string) ToReaderAtOption {
	return func(o *ToReaderAtOptions) { o.tempDir = dir }
}

func WithTempPattern(p string) ToReaderAtOption {
	return func(o *ToReaderAtOptions) { o.tempPattern = p }
}

type fileStatter interface{ Stat() (os.FileInfo, error) }
type sizer interface{ Size() int64 }

func unwrapReaderAt(r io.ReaderAt) io.ReaderAt {
	// unwrap our wrapper to the real ReaderAt (e.g. *bytes.Reader, *os.File)
	switch v := r.(type) {
	case readerAtReadCloser:
		if v.readerAtReader != nil && v.readerAtReader.ReaderAt != nil {
			return v.readerAtReader.ReaderAt
		}
	case *readerAtReadCloser:
		if v != nil && v.readerAtReader != nil && v.readerAtReader.ReaderAt != nil {
			return v.readerAtReader.ReaderAt
		}
	}
	return r
}

// ToReaderAt converts any io.Reader into something that supports io.ReaderAt.
// - If the input already supports ReaderAt → returns it directly.
// - If it is small enough → keeps it in memory.
// - If too large → spills to temporary file.
func ToReaderAt(ctx context.Context, r io.Reader, opts ...ToReaderAtOption) (*ReaderAtResult, error) {
	if r == nil {
		return nil, errors.New("sio: ToReaderAt: nil reader")
	}

	// unwrap UseReader (binder wrapper)
	if ur, ok := r.(*UseReader); ok {
		if err := ur.Err(); err != nil {
			return nil, err
		}
		if inner := ur.Unwrap(); inner != nil {
			r = inner
		}
	}

	// unwrap downloadReaderCloser wrapper
	if drc, ok := r.(*downloadReaderCloser); ok && drc.reader != nil {
		r = drc.reader
	}

	if drc, ok := r.(readerAtReadCloser); ok && drc.readerAtReader != nil {
		if inner := drc.readerAtReader.Reader; inner != nil {
			r = inner
		}
	}

	// --- Apply options ---
	o := ToReaderAtOptions{
		maxMemoryBytes: 8 << 20,          // default 8MB
		tempPattern:    "sio-readerat-*", // default temp file pattern
	}
	for _, fn := range opts {
		if fn != nil {
			fn(&o)
		}
	}

	// If TempDir not provided, try use IoSession dir
	if strings.TrimSpace(o.tempDir) == "" {
		if ses := Session(ctx); ses != nil {
			if d, ok := ses.(interface{ Dir() string }); ok {
				if dir := strings.TrimSpace(d.Dir()); dir != "" {
					o.tempDir = dir
				}
			}
		}
	}

	// --- Fast path: if already ReaderAt, use directly ---
	if ra0, ok := r.(io.ReaderAt); ok {
		ra := unwrapReaderAt(ra0)

		var size int64 = -1

		if st, ok := ra.(fileStatter); ok {
			if fi, err := st.Stat(); err == nil {
				size = fi.Size()
			}
		}
		if size < 0 {
			if sz, ok := ra.(sizer); ok {
				size = sz.Size()
			}
		}
		if size < 0 {
			// last resort
			if sz := SizeFromReader(r); sz > 0 {
				size = sz
			}
		}

		return &ReaderAtResult{
			readerAt: ra,
			size:     size,
			cleanup:  func() error { return nil },
			source:   "direct",
		}, nil
	}

	// --- Force spool to temp file ---
	if o.maxMemoryBytes <= 0 {
		return spoolToTempFile(r, o.tempDir, o.tempPattern)
	}

	// --- Try in-memory first, then spill if exceeds limit ---
	limit := o.maxMemoryBytes
	buf := make([]byte, 0, minInt64(limit, 64<<10)) // up to 64KB prealloc
	tmp := make([]byte, 32<<10)

	var total int64
	for total <= limit {
		n, err := r.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
			total += int64(n)
		}

		if err != nil {
			// Fits fully in memory
			if err == io.EOF {
				return &ReaderAtResult{
					readerAt: bytes.NewReader(buf),
					size:     int64(len(buf)),
					cleanup:  func() error { return nil },
					source:   "memory",
				}, nil
			}
			return nil, err
		}

		if total > limit {
			break
		}
	}

	// Exceeded allowed memory → spill to disk
	return spillWithPrefix(r, buf, o.tempDir, o.tempPattern)
}

// Writes the entire reader to a temp file and returns ReaderAt
func spoolToTempFile(r io.Reader, dir, pattern string) (*ReaderAtResult, error) {
	tmp, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return nil, err
	}

	n, err := Copy(tmp, r)
	if err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return nil, err
	}

	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return nil, err
	}

	return &ReaderAtResult{
		readerAt: tmp,
		size:     n,
		cleanup: func() error {
			_ = tmp.Close()
			return os.Remove(tmp.Name())
		},
		source: "tempFile",
	}, nil
}

// Writes buffered prefix plus remaining reader to a temp file
func spillWithPrefix(r io.Reader, prefix []byte, dir, pattern string) (*ReaderAtResult, error) {
	tmp, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return nil, err
	}

	var total int64

	// Write prefix first
	if len(prefix) > 0 {
		n, err := tmp.Write(prefix)
		if err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmp.Name())
			return nil, err
		}
		total += int64(n)
	}

	// Write the rest
	n2, err := Copy(tmp, r)
	if err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return nil, err
	}
	total += n2

	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return nil, err
	}

	return &ReaderAtResult{
		readerAt: tmp,
		size:     total,
		cleanup: func() error {
			_ = tmp.Close()
			return os.Remove(tmp.Name())
		},
		source: "tempFile",
	}, nil
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

/* -------------------------------------------------------------------------- */
/*                                  Types                                      */
/* -------------------------------------------------------------------------- */

// UseReader wraps an opened io.Reader from Binder.
type UseReader struct {
	r   io.Reader
	err error
}

// Read implements io.Reader.
func (ur *UseReader) Read(p []byte) (int, error) {
	if ur == nil {
		return 0, errors.New("sio: nil UseReader")
	}
	if ur.err != nil {
		return 0, ur.err
	}
	if ur.r == nil {
		return 0, errors.New("sio: reader not opened")
	}
	return ur.r.Read(p)
}

// Unwrap returns underlying io.Reader (nil if error).
func (ur *UseReader) Unwrap() io.Reader {
	if ur == nil || ur.err != nil {
		return nil
	}
	return ur.r
}

// Err returns any error associated with this reader.
func (ur *UseReader) Err() error {
	if ur == nil {
		return errors.New("sio: nil UseReader")
	}
	return ur.err
}

// Ensure UseReader implements io.Reader at compile time.
var _ io.Reader = (*UseReader)(nil)

/* -------------------------------------------------------------------------- */
/*                                  Binder                                     */
/* -------------------------------------------------------------------------- */

// Binder collects StreamReaders and opens them eagerly on registration.
type Binder struct {
	mu      sync.Mutex
	readers []io.Reader
	closers []io.Closer
	sources []StreamReader
	err     error
}

// openReader registers and immediately opens a StreamReader.
// Returns a UseReader that wraps the opened io.Reader.
func (b *Binder) openReader(sr StreamReader) *UseReader {
	if b == nil {
		return &UseReader{err: errors.New("sio: nil binder")}
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// If previous error occurred, return error reader
	if b.err != nil {
		return &UseReader{err: b.err}
	}

	if sr == nil {
		b.err = fmt.Errorf("sio: source #%d is nil", len(b.readers)+1)
		return &UseReader{err: b.err}
	}

	// Eager open: open immediately
	rc, err := sr.Open()
	if err != nil {
		b.err = fmt.Errorf("sio: source #%d: %w: %v", len(b.readers)+1, ErrOpenFailed, err)
		return &UseReader{err: b.err}
	}

	b.readers = append(b.readers, rc)
	b.closers = append(b.closers, rc)
	b.sources = append(b.sources, sr)

	return &UseReader{
		r: rc,
	}
}

// Use is a convenience wrapper around Reader that returns io.Reader directly.
// If opening fails, Binder.err is set and nil is returned.
func (b *Binder) Use(sr StreamReader) (io.Reader, error) {
	ur := b.openReader(sr)
	if ur == nil {
		return nil, fmt.Errorf("sio: nil UseReader")
	}

	if err := ur.Err(); err != nil {
		return nil, err
	}

	return ur.Unwrap(), nil
}

// cleanup closes all opened readers and cleans up all sources.
func (b *Binder) cleanup() {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, c := range b.closers {
		if c != nil {
			_ = c.Close()
		}
	}
	for _, s := range b.sources {
		if s != nil {
			_ = s.Cleanup()
		}
	}
	b.closers = nil
	b.sources = nil
	b.readers = nil
}

// Err returns any error that occurred during registration.
func (b *Binder) Err() error {
	if b == nil {
		return errors.New("sio: nil binder")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.err
}

/* -------------------------------------------------------------------------- */
/*                          Bind* (refactored API)                             */
/* -------------------------------------------------------------------------- */

// BindRead opens multiple StreamReaders lazily through Binder and executes fn.
// This is standalone (no session required).
func BindRead(ctx context.Context, fn func(ctx context.Context, b *Binder) error) error {
	if fn == nil {
		return errors.New("sio: BindRead: fn is nil")
	}

	b := &Binder{}
	defer b.cleanup()

	return fn(ctx, b)
}

// BindProcess opens multiple StreamReaders lazily, creates an output, and executes fn.
// Requires a session in context.
func BindProcess(ctx context.Context, out OutConfig, fn func(ctx context.Context, b *Binder, w io.Writer) error) (*Output, error) {
	if fn == nil {
		return nil, errors.New("sio: BindProcess: fn is nil")
	}

	ses := Session(ctx)
	if ses == nil {
		return nil, ErrNoSession
	}

	iSes, ok := ses.(*ioSession)
	if !ok {
		return nil, ErrInvalidSessionType
	}

	b := &Binder{}
	defer b.cleanup()

	output, err := iSes.newOutputWithStorage(out.ext, out.getStorageType(iSes.storageType))
	if err != nil {
		return nil, err
	}

	w, err := output.OpenWriter()
	if err != nil {
		_ = output.cleanup()
		return nil, err
	}
	defer w.Close()

	if err := fn(ctx, b, w); err != nil {
		_ = output.cleanup()
		return nil, err
	}

	return output, nil
}

// BindProcessResult is like BindProcess but also returns a result value.
func BindProcessResult[T any](ctx context.Context, out OutConfig, fn func(ctx context.Context, b *Binder, w io.Writer) (*T, error)) (*Output, *T, error) {
	if fn == nil {
		return nil, nil, errors.New("sio: BindProcessResult: fn is nil")
	}

	ses := Session(ctx)
	if ses == nil {
		return nil, nil, ErrNoSession
	}

	iSes, ok := ses.(*ioSession)
	if !ok {
		return nil, nil, ErrInvalidSessionType
	}

	b := &Binder{}
	defer b.cleanup()

	output, err := iSes.newOutputWithStorage(out.ext, out.getStorageType(iSes.storageType))
	if err != nil {
		return nil, nil, err
	}

	w, err := output.OpenWriter()
	if err != nil {
		_ = output.cleanup()
		return nil, nil, err
	}
	defer w.Close()

	res, execErr := fn(ctx, b, w)
	if execErr != nil {
		_ = output.cleanup()
		return nil, nil, execErr
	}

	return output, res, nil
}

// BindReadResult is like BindRead but also returns a result value.
func BindReadResult[T any](ctx context.Context, fn func(ctx context.Context, b *Binder) (*T, error)) (*T, error) {
	if fn == nil {
		return nil, errors.New("sio: BindReadResult: fn is nil")
	}

	b := &Binder{}
	defer b.cleanup()

	return fn(ctx, b)
}
