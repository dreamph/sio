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
)

/* ---------- Errors ---------- */

var (
	ErrNilSource              = errors.New("sio: nil source")
	ErrOpenFailed             = errors.New("sio: cannot open reader")
	ErrIoManagerClosed        = errors.New("sio: manager is closed")
	ErrIoSessionClosed        = errors.New("sio: session is closed")
	ErrInvalidURL             = errors.New("sio: invalid URL")
	ErrDownloadFailed         = errors.New("sio: download failed")
	ErrNoSession              = errors.New("sio: session is nil")
	ErrFileStorageUnavailable = errors.New("sio: file storage requires directory (manager created with StorageBytes only)")
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
	// StorageBytes stores data in memory as byte slices
	StorageBytes
)

// Shorthand aliases
const (
	Mem  = StorageBytes
	File = StorageFile
)

// Storage converts string to StorageType.
// Supports: "file", "disk", "bytes", "mem", "memory"
func Storage(s string) StorageType {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "bytes", "mem", "memory":
		return StorageBytes
	default: // "file", "disk", or unknown
		return StorageFile
	}
}

// String returns string representation of StorageType
func (s StorageType) String() string {
	switch s {
	case StorageBytes:
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

// OutOption configures output behavior
type OutOption struct {
	Ext         string
	StorageType *StorageType // nil = use session default
}

// Out creates output configuration.
//
// Usage:
//
//	sio.Out(".pdf")                        // use session default
//	sio.Out(".pdf", sio.Mem)               // force memory storage
//	sio.Out(".pdf", sio.File)              // force file storage
//	sio.Out(".pdf", sio.Storage("memory")) // from string config
func Out(ext string, opts ...StorageType) OutOption {
	o := OutOption{Ext: ext}
	if len(opts) > 0 {
		o.StorageType = &opts[0]
	}
	return o
}

// getStorageType returns the storage type, using session default if not specified
func (o OutOption) getStorageType(sessionDefault StorageType) StorageType {
	if o.StorageType != nil {
		return *o.StorageType
	}
	return sessionDefault
}

/* -------------------------------------------------------------------------- */
/*                              HTTP Client                                    */
/* -------------------------------------------------------------------------- */

var httpClient = &http.Client{Timeout: 30 * time.Second}

type Config struct {
	Client *http.Client
}

func Configure(config Config) error {
	if config.Client != nil {
		httpClient = config.Client
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

type URLReaderOptions struct {
	Client      *http.Client
	InsecureTLS bool
	Timeout     time.Duration
}

// URLReader streams content directly from a URL.
// Each Open() call creates a new HTTP request.
// Cleanup() is a no-op since the response body is closed via the returned ReadCloser.
type URLReader struct {
	URL    string
	client *http.Client
}

func NewURLReader(urlStr string, opts ...URLReaderOptions) *URLReader {
	r := &URLReader{URL: urlStr}

	baseTimeout := httpClient.Timeout
	if baseTimeout == 0 {
		baseTimeout = 30 * time.Second
	}

	if len(opts) == 0 {
		r.client = httpClient
		return r
	}

	opt := opts[0]

	if opt.Client != nil {
		r.client = opt.Client
		return r
	}

	c := &http.Client{
		Timeout: baseTimeout,
	}

	if opt.Timeout > 0 {
		c.Timeout = opt.Timeout
	}

	if opt.InsecureTLS {
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
	parsed, err := url.Parse(u.URL)
	if err != nil {
		return nil, ErrInvalidURL
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, ErrInvalidURL
	}

	resp, err := u.client.Get(u.URL)
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

	// For StorageBytes, we don't need a directory
	if st == StorageBytes {
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

	// For StorageBytes mode, no directory is needed
	if m.storageType == StorageBytes {
		return &ioSession{
			manager:     m,
			dir:         "",
			storageType: m.storageType,
		}, nil
	}

	// For StorageFile mode, create session directory
	id := fmt.Sprintf("%d-%d", time.Now().UnixNano(), time.Now().UnixNano()%1000000)
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

	Process(ctx context.Context, source StreamReader, out OutOption, fn ProcessFunc) (*Output, error)
	ProcessList(ctx context.Context, sources []StreamReader, out OutOption, fn ProcessListFunc) (*Output, error)

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

	// For StorageBytes mode, create an in-memory output
	if storageType == StorageBytes {
		out := &Output{
			path:        "",
			ses:         s,
			data:        nil,
			storageType: StorageBytes,
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
	f.Close()

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
	out OutOption,
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
	output, err := s.newOutputWithStorage(out.Ext, storageType)
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
	out OutOption,
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
	output, err := s.newOutputWithStorage(out.Ext, storageType)
	if err != nil {
		return nil, err
	}

	w, err := output.OpenWriter()
	if err != nil {
		_ = output.cleanup()
		return nil, err
	}
	defer w.Close()

	if err := fn(ctx, rl.Readers, w); err != nil {
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

	// For StorageBytes mode, just clean up in-memory outputs
	if s.storageType == StorageBytes {
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

const defaultBufSize = 64 * 1024 // 64KB

var bytesBufferPool = sync.Pool{
	New: func() any {
		return bytes.NewBuffer(make([]byte, 0, defaultBufSize))
	},
}

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
		// Take ownership - zero copy
		b.output.data = b.buf.Bytes()
		b.output.mu.Unlock()
		// Don't return to pool - output now owns the backing array
		return nil
	}
	// Only return to pool if no output (error case)
	b.buf.Reset()
	bytesBufferPool.Put(b.buf)
	return nil
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

	// For StorageBytes mode
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

	// For StorageBytes mode, return reader from memory
	if o.storageType == StorageBytes {
		if o.data == nil {
			return io.NopCloser(bytes.NewReader([]byte{})), nil
		}
		return io.NopCloser(bytes.NewReader(o.data)), nil
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

	// For StorageBytes mode, return pooled buffer writer
	if o.storageType == StorageBytes {
		buf := bytesBufferPool.Get().(*bytes.Buffer)
		buf.Reset()
		return &bytesWriteCloser{
			buf:    buf,
			output: o,
		}, nil
	}

	// For StorageFile mode, return file writer
	return os.Create(o.path)
}

func (o *Output) Reader(opts ...InOption) StreamReader {
	o.mu.Lock()
	defer o.mu.Unlock()

	var sr StreamReader
	if o.storageType == StorageBytes {
		sr = NewBytesReader(o.data)
	} else {
		sr = NewFileReader(o.path)
	}

	if len(opts) > 0 {
		return In(sr, opts...)
	}
	return sr
}

// Data returns the raw bytes for StorageBytes mode.
// Returns nil for StorageFile mode.
// The returned slice is owned by Output - do not modify.
func (o *Output) Data() []byte {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.storageType == StorageBytes {
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

	var errs error

	// For StorageBytes mode, just clear the data
	if o.storageType == StorageBytes {
		o.data = nil
		return nil
	}

	// For StorageFile mode, remove the files
	if err := os.Remove(o.path); err != nil && !os.IsNotExist(err) {
		errs = errors.Join(errs, err)
	}

	if o.detachedPath != "" {
		if err := os.Remove(o.detachedPath); err != nil && !os.IsNotExist(err) {
			errs = errors.Join(errs, err)
		}
	}

	return errs
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
	return NewIOReader(p)
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

func readDirect(ctx context.Context, source StreamReader, fn ReadFunc) error {
	if source == nil {
		return ErrNilSource
	}
	if fn == nil {
		return fmt.Errorf("sio: Read: fn is nil")
	}

	r, err := source.Open()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrOpenFailed, err)
	}
	defer r.Close()
	defer source.Cleanup()

	return fn(ctx, r)
}

func readListDirect(ctx context.Context, sources []StreamReader, fn ReadListFunc) error {
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

	ses := Session(ctx)
	if ses != nil {
		return ses.Read(ctx, source, fn)
	}

	return readDirect(ctx, source, fn)
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
	if ses != nil {
		return ses.ReadList(ctx, sources, fn)
	}

	return readListDirect(ctx, sources, fn)
}

// Process is a convenience wrapper for IoSession.Process.
func Process(ctx context.Context, source StreamReader, out OutOption, fn ProcessFunc) (*Output, error) {
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
func ProcessList(ctx context.Context, sources []StreamReader, out OutOption, fn ProcessListFunc) (*Output, error) {
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

func ProcessResult[T any](ctx context.Context, source StreamReader, out OutOption, fn func(ctx context.Context, r io.Reader, w io.Writer) (*T, error)) (*Output, *T, error) {
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

func ProcessListResult[T any](ctx context.Context, sources []StreamReader, out OutOption, fn func(ctx context.Context, readers []io.Reader, w io.Writer) (*T, error)) (*Output, *T, error) {
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

func ToOutput(ctx context.Context, src StreamReader, out OutOption) (*Output, error) {
	return Process(ctx, src, out, func(ctx context.Context, r io.Reader, w io.Writer) error {
		_, err := copyStream(w, r)
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

	return copyStream(w, rc)
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

type LineFn func(line string) error

func ReadLines(ctx context.Context, src StreamReader, fn LineFn) error {
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

func ReadFileLines(ctx context.Context, path string, fn LineFn) error {
	src := NewFileReader(path)
	return ReadLines(ctx, src, fn)
}

func GetFileSize(ctx context.Context, source StreamReader) (int64, error) {
	fileSize, err := ReadResult[int64](ctx, source, func(ctx context.Context, r io.Reader) (*int64, error) {
		fileSize := GetFileSizeByReader(r)
		return &fileSize, nil
	})
	if err != nil {
		return 0, err
	}
	return *fileSize, nil
}

func GetFileSizeByReader(r io.Reader) int64 {
	switch v := r.(type) {
	case *os.File:
		if fi, err := v.Stat(); err == nil {
			return fi.Size()
		}
	case interface{ Len() int }:
		return int64(v.Len())
	}
	return -1
}

func GetFileSizeByPath(path string) (int64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

/* -------------------------------------------------------------------------- */
/*                              Copy Utilities                                 */
/* -------------------------------------------------------------------------- */

var copyBufPool = sync.Pool{
	New: func() any {
		return make([]byte, 1024*1024) // 1MB buffer
	},
}

func Copy(dst io.Writer, src io.Reader) (int64, error) {
	return copyStream(dst, src)
}

func copyStream(dst io.Writer, src io.Reader) (int64, error) {
	if wt, ok := src.(io.WriterTo); ok {
		return wt.WriteTo(dst)
	}
	if rf, ok := dst.(io.ReaderFrom); ok {
		return rf.ReadFrom(src)
	}

	buf := copyBufPool.Get().([]byte)
	defer copyBufPool.Put(buf)
	return io.CopyBuffer(dst, src, buf)
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

	return copyStream(f, src)
}
