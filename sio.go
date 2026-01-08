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

// Common file extensions
const (
	Json = ".json"
	Csv  = ".csv"
	Txt  = ".txt"
	Xml  = ".xml"

	Pdf  = ".pdf"
	Docx = ".docx"
	Xlsx = ".xlsx"
	Pptx = ".pptx"

	Jpg  = ".jpg"
	Jpeg = ".jpeg"
	Png  = ".png"

	Zip = ".zip"
)

// Input source kinds
const (
	KindFile      = "file"
	KindURL       = "url"
	KindMultipart = "multipart"
	KindMemory    = "memory"
	KindReader    = "reader"
	KindStream    = "stream"
)

// MB converts megabytes to bytes.
func MB(size int64) int64 {
	return size * 1024 * 1024
}

// ToExt adds a dot prefix to a format string.
func ToExt(format string) string {
	return "." + format
}

/* -------------------------------------------------------------------------- */
/*                                   Errors                                    */
/* -------------------------------------------------------------------------- */

var (
	ErrNilSource              = errors.New("sio: nil source")
	ErrOpenFailed             = errors.New("sio: cannot open reader")
	ErrIoManagerClosed        = errors.New("sio: manager is closed")
	ErrIoSessionClosed        = errors.New("sio: session is closed")
	ErrInvalidURL             = errors.New("sio: invalid URL")
	ErrDownloadFailed         = errors.New("sio: download failed")
	ErrNoSession              = errors.New("sio: session is nil")
	ErrFileStorageUnavailable = errors.New("sio: file storage requires directory")
	ErrInvalidSessionType     = errors.New("sio: invalid session type")
	ErrNilFunc                = errors.New("sio: fn is nil")
	ErrOutputFinalized        = errors.New("sio: output already finalized")
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
	// File stores data as temporary files on disk (default)
	File StorageType = iota
	// Memory stores data in memory as byte slices
	Memory
)

// Storage converts string to StorageType.
// Supports: "file", "memory"
func Storage(s string) StorageType {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "memory":
		return Memory
	default: // "file" or unknown
		return File
	}
}

// String returns string representation of StorageType
func (s StorageType) String() string {
	switch s {
	case Memory:
		return "memory"
	case File:
		return "file"
	default:
		return "file"
	}
}

/* -------------------------------------------------------------------------- */
/*                            Manager Options                                  */
/* -------------------------------------------------------------------------- */

// ManagerOption is a functional option for configuring IoManager.
type ManagerOption interface {
	applyManager(*managerConfig)
}

// ManagerOptionFunc adapts a function to ManagerOption.
type ManagerOptionFunc func(*managerConfig)

func (f ManagerOptionFunc) applyManager(c *managerConfig) {
	f(c)
}

// managerConfig holds configuration for IoManager
type managerConfig struct {
	autoThreshold int64 // 0 = disabled, >0 = threshold in bytes for auto storage switching
}

type thresholdOption int64

func (t thresholdOption) applyManager(c *managerConfig) {
	c.autoThreshold = int64(t)
}

func (t thresholdOption) applyOut(o *OutConfig) {
	val := int64(t)
	o.autoThreshold = &val
}

// WithThreshold sets the automatic storage threshold.
// Files smaller than the threshold use the default storage type (usually Memory).
// Files equal to or larger than the threshold automatically switch to File storage.
//
// Example:
//
//	manager := NewIoManager("./temp", Memory,
//	    WithThreshold(100*1024*1024), // 100MB threshold
//	)
//
// It can also be used per operation via Out().
func WithThreshold(bytes int64) thresholdOption {
	return thresholdOption(bytes)
}

/* -------------------------------------------------------------------------- */
/*                             Output Options                                  */
/* -------------------------------------------------------------------------- */

// OutConfig configures output behavior
type OutConfig struct {
	ext           string
	storageType   *StorageType // nil = use session default
	autoThreshold *int64       // nil = use session default
}

// OutOption configures output behavior.
type OutOption interface {
	applyOut(*OutConfig)
}

// OutOptionFunc adapts a function to OutOption.
type OutOptionFunc func(*OutConfig)

func (f OutOptionFunc) applyOut(o *OutConfig) {
	f(o)
}

func (st StorageType) applyOut(o *OutConfig) {
	o.storageType = &st
}

// Out creates output configuration.
//
// Usage:
//
//	Out(".pdf")                        // use session default
//	Out(".pdf", Memory)            // force memory storage
//	Out(".pdf", File)              // force file storage
//	Out(".pdf", Storage("memory")) // from string config
//	Out(".pdf", WithThreshold(10)) // per-operation threshold
func Out(ext string, opts ...OutOption) OutConfig {
	o := OutConfig{ext: ext}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt.applyOut(&o)
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

// AutoThreshold returns the configured threshold, or nil when unset.
func (o OutConfig) AutoThreshold() *int64 {
	return o.autoThreshold
}

/* -------------------------------------------------------------------------- */
/*                              HTTP Client                                    */
/* -------------------------------------------------------------------------- */

var httpClient = &http.Client{Timeout: 30 * time.Second}

// Config holds global configuration
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

// Configure applies global configuration (call at app startup only)
func Configure(config Config) error {
	if config.client != nil {
		httpClient = config.client
	}
	return nil
}

/* -------------------------------------------------------------------------- */
/*                           StreamReader Abstraction                          */
/* -------------------------------------------------------------------------- */

// StreamReader represents any input source that can be opened as an io.ReadCloser.
type StreamReader interface {
	Open() (io.ReadCloser, error)
	Cleanup() error
}

/* -------------------------------------------------------------------------- */
/*                               FileReader                                    */
/* -------------------------------------------------------------------------- */

// FileReader reads from a file path on disk.
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

func (f *FileReader) Cleanup() error { return nil }

func (f *FileReader) Path() string { return f.path }

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

func (m *MultipartReader) Size() int64 {
	if m.file == nil {
		return -1
	}
	return m.file.Size
}

func (m *MultipartReader) Cleanup() error { return nil }

/* -------------------------------------------------------------------------- */
/*                                GenericReader                                */
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

func (m *GenericReader) Size() int64 { return SizeAny(m.file) }

func (m *GenericReader) Cleanup() error { return nil }

/* -------------------------------------------------------------------------- */
/*                                BytesReader                                  */
/* -------------------------------------------------------------------------- */

// BytesReader exposes an in-memory []byte as a StreamReader.
// Cleanup() optionally clears the underlying buffer.
type BytesReader struct {
	data             []byte
	releaseOnCleanup bool
}

type BytesReaderOption func(*BytesReader)

// WithReleaseOnCleanup clears the underlying buffer when Cleanup() is called.
func WithReleaseOnCleanup() BytesReaderOption {
	return func(b *BytesReader) {
		b.releaseOnCleanup = true
	}
}

func NewBytesReader(data []byte, opts ...BytesReaderOption) *BytesReader {
	br := &BytesReader{data: data}
	for _, opt := range opts {
		if opt != nil {
			opt(br)
		}
	}
	return br
}

func (b *BytesReader) Open() (io.ReadCloser, error) {
	return NopCloser(bytes.NewReader(b.data)), nil
}

func (b *BytesReader) Size() int64 {
	return int64(len(b.data))
}

func (b *BytesReader) Cleanup() error {
	if b.releaseOnCleanup {
		b.data = nil
	}
	return nil
}

// Data returns the underlying byte slice.
func (b *BytesReader) Data() []byte {
	return b.data
}

/* -------------------------------------------------------------------------- */
/*                                 URLReader                                   */
/* -------------------------------------------------------------------------- */

// URLReaderOptions configures URL reading behavior
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
/*                                  Manager                                    */
/* -------------------------------------------------------------------------- */

// IoManager manages sessions and their temporary directories
type IoManager interface {
	NewSession() (IoSession, error)
	Cleanup() error
}

// manager manages a root directory under which multiple IoSessions
// create their own isolated working directories.
type manager struct {
	mu            sync.Mutex
	baseDir       string
	closed        bool
	storageType   StorageType
	autoThreshold int64 // 0 = disabled, >0 = auto storage switching threshold in bytes
}

// NewIoManager creates a new IoManager.
//
//	mgr, _ := NewIoManager("./temp", Memory)
//	mgr, _ := NewIoManager("./temp", File, WithThreshold(100*1024*1024))
func NewIoManager(baseDir string, storageType StorageType, opts ...ManagerOption) (IoManager, error) {
	// Apply options
	config := &managerConfig{
		autoThreshold: 0, // disabled by default
	}
	for _, opt := range opts {
		opt.applyManager(config)
	}

	if strings.TrimSpace(baseDir) == "" {
		dir, err := os.MkdirTemp("", "sio-")
		if err != nil {
			return nil, err
		}
		return &manager{
			baseDir:       dir,
			storageType:   storageType,
			autoThreshold: config.autoThreshold,
		}, nil
	}

	baseDir = filepath.Clean(baseDir)
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, err
	}

	return &manager{
		baseDir:       baseDir,
		storageType:   storageType,
		autoThreshold: config.autoThreshold,
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

	// Create session directory under baseDir
	id := uuid.New().String()
	dir := filepath.Join(m.baseDir, id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	return &ioSession{
		manager:       m,
		dir:           dir,
		storageType:   m.storageType,
		autoThreshold: m.autoThreshold,
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

	if strings.TrimSpace(m.baseDir) == "" {
		return nil // nothing to remove
	}

	err := os.RemoveAll(m.baseDir)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

/* -------------------------------------------------------------------------- */
/*                                 IoSession                                   */
/* -------------------------------------------------------------------------- */

type ReadFunc func(ctx context.Context, r io.Reader) error
type ReadListFunc func(ctx context.Context, readers []io.Reader) error
type ProcessFunc func(ctx context.Context, r io.Reader, w io.Writer) error
type ProcessListFunc func(ctx context.Context, readers []io.Reader, w io.Writer) error

// IoSession manages temporary files for a single job/request
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
	mu            sync.Mutex
	manager       IoManager
	dir           string
	closed        bool
	outputs       []*Output // tracked outputs inside this session
	cleanupFns    []func() error
	storageType   StorageType
	autoThreshold int64 // 0 = disabled, >0 = auto storage switching threshold
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

func (s *ioSession) decideStorageType(source StreamReader, out OutConfig) StorageType {
	if out.storageType != nil {
		return *out.storageType
	}

	if out.autoThreshold != nil && *out.autoThreshold > 0 {
		size := SizeAny(source)
		return s.storageByThreshold(size, *out.autoThreshold)
	}

	if s.autoThreshold > 0 {
		size := SizeAny(source)
		return s.storageByThreshold(size, s.autoThreshold)
	}

	return s.storageType
}

func (s *ioSession) decideStorageTypeForList(sources []StreamReader, out OutConfig) StorageType {
	if out.storageType != nil {
		return *out.storageType
	}

	if out.autoThreshold != nil && *out.autoThreshold > 0 {
		size := SizeFromStreamList(sources)
		return s.storageByThreshold(size, *out.autoThreshold)
	}

	if s.autoThreshold > 0 {
		size := SizeFromStreamList(sources)
		return s.storageByThreshold(size, s.autoThreshold)
	}

	return s.storageType
}

func (s *ioSession) storageByThreshold(size, threshold int64) StorageType {
	if size < 0 {
		return s.storageType
	}
	if size >= threshold {
		return File
	}
	return s.storageType
}

// newOutputWithStorage creates output with specified storage type.
func (s *ioSession) newOutputWithStorage(ext string, storageType StorageType) (*Output, error) {
	if err := s.ensureOpen(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// For Memory mode, create an in-memory output
	if storageType == Memory {
		out := &Output{
			path:        "",
			ses:         s,
			data:        nil,
			storageType: Memory,
		}
		s.outputs = append(s.outputs, out)
		return out, nil
	}

	// File requires directory
	if s.dir == "" {
		return nil, ErrFileStorageUnavailable
	}

	// For File mode, create a temporary file
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
		storageType: File,
	}
	s.outputs = append(s.outputs, out)
	return out, nil
}

func (s *ioSession) Process(ctx context.Context, source StreamReader, out OutConfig, fn ProcessFunc) (*Output, error) {
	if source == nil {
		return nil, ErrNilSource
	}
	if fn == nil {
		return nil, ErrNilFunc
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

	storageType := s.decideStorageType(source, out)
	output, err := s.newOutputWithStorage(out.ext, storageType)
	if err != nil {
		return nil, err
	}

	sizeHint := SizeAny(source)
	w, err := output.OpenWriter(sizeHint)
	if err != nil {
		_ = output.cleanup()
		return nil, err
	}

	if err := fn(ctx, r, w); err != nil {
		_ = w.Close()
		_ = output.cleanup()
		return nil, err
	}

	if err := w.Close(); err != nil {
		_ = output.cleanup()
		return nil, err
	}

	return output, nil
}

func (s *ioSession) ProcessList(ctx context.Context, sources []StreamReader, out OutConfig, fn ProcessListFunc) (*Output, error) {
	if len(sources) == 0 {
		return nil, ErrNilSource
	}
	if fn == nil {
		return nil, ErrNilFunc
	}
	if err := s.ensureOpen(); err != nil {
		return nil, err
	}

	rl, err := OpenReaderList(sources)
	if err != nil {
		return nil, err
	}
	defer rl.Close()

	storageType := s.decideStorageTypeForList(sources, out)
	output, err := s.newOutputWithStorage(out.ext, storageType)
	if err != nil {
		return nil, err
	}

	sizeHint := SizeFromStreamList(sources)
	w, err := output.OpenWriter(sizeHint)
	if err != nil {
		_ = output.cleanup()
		return nil, err
	}

	if err := fn(ctx, rl.readers, w); err != nil {
		_ = w.Close()
		_ = output.cleanup()
		return nil, err
	}

	if err := w.Close(); err != nil {
		_ = output.cleanup()
		return nil, err
	}

	return output, nil
}

func (s *ioSession) Read(ctx context.Context, source StreamReader, fn ReadFunc) error {
	if source == nil {
		return ErrNilSource
	}
	if fn == nil {
		return ErrNilFunc
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

func (s *ioSession) ReadList(ctx context.Context, sources []StreamReader, fn ReadListFunc) error {
	if len(sources) == 0 {
		return ErrNilSource
	}
	if fn == nil {
		return ErrNilFunc
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

func (s *ioSession) Cleanup() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	var firstErr error
	for _, out := range s.outputs {
		out.mu.Lock()
		shouldSkip := out.keep && !out.closed && out.storageType == File
		out.mu.Unlock()
		if shouldSkip {
			continue
		}
		if err := out.cleanup(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	for _, fn := range s.cleanupFns {
		if fn == nil {
			continue
		}
		if err := fn(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	s.cleanupFns = nil

	if s.dir == "" {
		return firstErr
	}

	// Clean up any remaining files in the session directory
	entries, err := os.ReadDir(s.dir)
	if err != nil && !os.IsNotExist(err) {
		if firstErr == nil {
			firstErr = err
		}
		return firstErr
	}

	for _, e := range entries {
		full := filepath.Join(s.dir, e.Name())
		if s.isKeptPath(full) {
			continue
		}
		if err := os.RemoveAll(full); err != nil && !os.IsNotExist(err) && firstErr == nil {
			firstErr = err
		}
	}

	if err := os.Remove(s.dir); err != nil && !os.IsNotExist(err) && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func (s *ioSession) addCleanup(fn func() error) {
	if s == nil || fn == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		_ = fn()
		return
	}
	s.cleanupFns = append(s.cleanupFns, fn)
}

func (s *ioSession) spillOutputToFile(out *Output, ext string) error {
	if s == nil || out == nil {
		return nil
	}
	if s.dir == "" {
		return ErrFileStorageUnavailable
	}

	out.mu.Lock()
	if out.closed || out.storageType != Memory {
		out.mu.Unlock()
		return nil
	}
	data := out.data
	out.mu.Unlock()

	pattern := "*"
	if ext != "" {
		pattern += ext
	}

	f, err := os.CreateTemp(s.dir, pattern)
	if err != nil {
		return err
	}

	if len(data) > 0 {
		if _, err := f.Write(data); err != nil {
			_ = f.Close()
			_ = os.Remove(f.Name())
			return err
		}
	}

	if err := f.Close(); err != nil {
		_ = os.Remove(f.Name())
		return err
	}

	out.mu.Lock()
	out.path = f.Name()
	out.storageType = File
	out.data = nil
	out.mu.Unlock()

	return nil
}

/* -------------------------------------------------------------------------- */
/*                           Storage Helper Types                              */
/* -------------------------------------------------------------------------- */

// bytesWriteCloser wraps a bytes.Buffer to implement io.WriteCloser
type bytesWriteCloser struct {
	buf             *bytes.Buffer
	output          *Output
	written         bool
	preallocateData []byte // backing slice for pre-allocated buffer
}

func (b *bytesWriteCloser) Write(p []byte) (int, error) {
	// Fast path: detect single large write (>= 64KB)
	if !b.written && b.buf.Len() == 0 && len(p) >= 64<<10 {
		b.written = true
		var data []byte
		if b.preallocateData != nil && len(p) <= cap(b.preallocateData) {
			data = b.preallocateData[:len(p)]
		} else {
			data = make([]byte, len(p))
		}
		copy(data, p)
		b.output.mu.Lock()
		b.output.data = data
		b.output.mu.Unlock()
		return len(p), nil
	}
	b.written = true
	return b.buf.Write(p)
}

func (b *bytesWriteCloser) Close() error {
	if b.output != nil && b.output.data == nil {
		b.output.mu.Lock()
		b.output.data = b.buf.Bytes()
		b.output.mu.Unlock()
	}
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

// NopCloser wraps an io.Reader as io.ReadCloser, preserving ReaderAt if present
func NopCloser(r io.Reader) io.ReadCloser {
	if r == nil {
		return nil
	}

	if rc, ok := r.(io.ReadCloser); ok {
		return rc
	}

	if rc, ok := r.(readerAtReadCloser); ok {
		return rc
	}

	if ra, ok := r.(io.ReaderAt); ok {
		return readerAtReadCloser{
			&readerAtReader{
				Reader:   r,
				ReaderAt: ra,
			},
		}
	}

	return io.NopCloser(r)
}

/* -------------------------------------------------------------------------- */
/*                                   Output                                    */
/* -------------------------------------------------------------------------- */

// Output represents a file or memory buffer produced by processing
type Output struct {
	mu          sync.Mutex
	path        string
	ses         IoSession
	closed      bool
	keep        bool
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

func (o *Output) Size() int64 {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return -1
	}

	if o.storageType == Memory {
		return int64(len(o.data))
	}

	fi, err := os.Stat(o.path)
	if err != nil {
		return -1
	}
	return fi.Size()
}

func (o *Output) OpenReader() (io.ReadCloser, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return nil, fmt.Errorf("sio: output is cleaned up")
	}

	// For Memory mode, return reader from memory
	if o.storageType == Memory {
		if o.data == nil {
			return NopCloser(bytes.NewReader([]byte{})), nil
		}
		return NopCloser(bytes.NewReader(o.data)), nil
	}

	// For File mode, return file reader
	return os.Open(o.path)
}

func (o *Output) OpenWriter(sizeHint ...int64) (io.WriteCloser, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return nil, fmt.Errorf("sio: output is cleaned up")
	}

	if o.storageType == Memory {
		var buf *bytes.Buffer
		var preallocateData []byte
		if len(sizeHint) > 0 && sizeHint[0] > 0 {
			preallocateData = make([]byte, 0, int(sizeHint[0]))
			buf = bytes.NewBuffer(preallocateData)
		} else {
			buf = &bytes.Buffer{}
		}
		return &bytesWriteCloser{
			buf:             buf,
			output:          o,
			preallocateData: preallocateData,
		}, nil
	}

	return os.Create(o.path)
}

func (o *Output) Reader() StreamReader {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.storageType == Memory {
		return NewBytesReader(o.data)
	}
	return NewFileReader(o.path)
}

// Data returns the raw bytes for Memory mode.
// Returns nil for File mode.
// The returned slice is owned by Output - do not modify.
func (o *Output) Data() []byte {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.storageType == Memory {
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

	if o.storageType == Memory {
		o.data = nil
		return nil
	}

	err := os.Remove(o.path)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

// Keep marks the output file as persistent (survives session cleanup)
func (o *Output) Keep() *Output {
	o.mu.Lock()
	if o.storageType == File {
		o.keep = true
	}
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

	res, err := ToReaderAt(ctx, r, opts...)
	if err != nil {
		_ = r.Close()
		return nil, err
	}

	closeReader := res.Source() != readerAtSourceDirect

	var once sync.Once
	var cleanupErr error
	origCleanup := res.cleanup
	res.cleanup = func() error {
		once.Do(func() {
			if origCleanup != nil {
				cleanupErr = errors.Join(cleanupErr, origCleanup())
			}
			if closeReader {
				cleanupErr = errors.Join(cleanupErr, r.Close())
			}
		})
		return cleanupErr
	}

	if ses, ok := o.ses.(*ioSession); ok {
		ses.addCleanup(res.cleanup)
	}

	return res, nil
}

/* -------------------------------------------------------------------------- */
/*                              In Helpers                                     */
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

// NewPartReader wraps multipart.Part as StreamReader
func NewPartReader(p *multipart.Part) *GenericReader {
	return NewGenericReader(p)
}

/* -------------------------------------------------------------------------- */
/*                        DownloadReaderCloser helper                          */
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

// SendStream reads from source and writes directly to w (no Output created).
func SendStream(ctx context.Context, source StreamReader, w io.Writer) error {
	if source == nil {
		return ErrNilSource
	}
	if w == nil {
		return fmt.Errorf("sio: Stream: writer is nil")
	}

	r, err := source.Open()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrOpenFailed, err)
	}
	defer r.Close()
	defer source.Cleanup()

	_, err = io.Copy(w, r)
	return err
}

/* -------------------------------------------------------------------------- */
/*                        StreamReader List Utilities                          */
/* -------------------------------------------------------------------------- */

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
/*                             Context Helpers                                 */
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
/*                       Package-level Functions (Legacy)                      */
/* -------------------------------------------------------------------------- */

// Read is a convenience wrapper that gets IoSession from context
// and calls ses.Read(...). If there is no session, it falls back
// to direct streaming from the StreamReader.
func Read(ctx context.Context, source StreamReader, fn ReadFunc) error {
	if source == nil {
		return ErrNilSource
	}
	if fn == nil {
		return ErrNilFunc
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
		return ErrNilFunc
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
		return nil, ErrNilFunc
	}

	ses := Session(ctx)
	if ses == nil {
		return nil, ErrNoSession
	}

	return ses.Process(ctx, source, out, fn)
}

func ProcessAt(
	ctx context.Context,
	source StreamReader,
	out OutConfig,
	fn func(ctx context.Context, ra io.ReaderAt, size int64, w io.Writer) error,
	opts ...ToReaderAtOption,
) (*Output, error) {
	return Process(ctx, source, out, func(ctx context.Context, r io.Reader, w io.Writer) error {
		res, err := ToReaderAt(ctx, r, opts...)
		if err != nil {
			return err
		}
		defer res.Cleanup()

		return fn(ctx, res.ReaderAt(), res.Size(), w)
	})
}

// ProcessList is a convenience wrapper for IoSession.ProcessList.
func ProcessList(ctx context.Context, sources []StreamReader, out OutConfig, fn ProcessListFunc) (*Output, error) {
	if len(sources) == 0 {
		return nil, ErrNilSource
	}
	if fn == nil {
		return nil, ErrNilFunc
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
		return nil, ErrNilFunc
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

func ReadAt[T any](
	ctx context.Context,
	source StreamReader,
	fn func(ctx context.Context, ra io.ReaderAt, size int64) (*T, error),
	opts ...ToReaderAtOption,
) (*T, error) {
	return ReadResult(ctx, source, func(ctx context.Context, r io.Reader) (*T, error) {
		res, err := ToReaderAt(ctx, r, opts...)
		if err != nil {
			return nil, err
		}
		defer res.Cleanup()

		return fn(ctx, res.ReaderAt(), res.Size())
	})
}

func ReadListResult[T any](ctx context.Context, sources []StreamReader, fn func(ctx context.Context, readers []io.Reader) (*T, error)) (*T, error) {
	if len(sources) == 0 {
		return nil, ErrNilSource
	}
	if fn == nil {
		return nil, ErrNilFunc
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
		return nil, nil, ErrNilFunc
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

func ProcessAtResult[T any](
	ctx context.Context,
	source StreamReader,
	out OutConfig,
	fn func(ctx context.Context, ra io.ReaderAt, size int64, w io.Writer) (*T, error),
	opts ...ToReaderAtOption,
) (*Output, *T, error) {
	return ProcessResult(ctx, source, out, func(ctx context.Context, r io.Reader, w io.Writer) (*T, error) {
		res, err := ToReaderAt(ctx, r, opts...)
		if err != nil {
			return nil, err
		}
		defer res.Cleanup()

		return fn(ctx, res.ReaderAt(), res.Size(), w)
	})
}

func ProcessListResult[T any](ctx context.Context, sources []StreamReader, out OutConfig, fn func(ctx context.Context, readers []io.Reader, w io.Writer) (*T, error)) (*Output, *T, error) {
	if len(sources) == 0 {
		return nil, nil, ErrNilSource
	}
	if fn == nil {
		return nil, nil, ErrNilFunc
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

// tryBytesFastPath attempts direct copy for BytesReader → Memory without io.Copy overhead.
// Returns (output, true) if fast path was used, (nil, false) otherwise.
func tryBytesFastPath(ctx context.Context, src StreamReader, out OutConfig) (*Output, bool) {
	br, ok := src.(*BytesReader)
	if !ok {
		return nil, false
	}

	ses := Session(ctx)
	if ses == nil {
		return nil, false
	}

	iSes, ok := ses.(*ioSession)
	if !ok {
		return nil, false
	}

	storageType := iSes.decideStorageType(src, out)
	if storageType != Memory || iSes.ensureOpen() != nil {
		return nil, false
	}

	output, err := iSes.newOutputWithStorage(out.ext, Memory)
	if err != nil || len(br.Data()) == 0 {
		return nil, false
	}

	// Direct copy without io.Copy
	data := make([]byte, len(br.Data()))
	copy(data, br.Data())
	output.mu.Lock()
	output.data = data
	output.mu.Unlock()

	return output, true
}

func ToOutput(ctx context.Context, src StreamReader, out OutConfig) (*Output, error) {
	// Try fast path for BytesReader → Memory
	if output, ok := tryBytesFastPath(ctx, src, out); ok {
		return output, nil
	}

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

		buf := make([]byte, 0, 64*1024)
		scanner.Buffer(buf, 1024*1024)

		for scanner.Scan() {
			if err := fn(scanner.Text()); err != nil {
				return err
			}
		}
		return scanner.Err()
	})
}

func ReadFileLines(ctx context.Context, path string, fn LineFunc) error {
	return ReadLines(ctx, NewFileReader(path), fn)
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

// SizeStream returns size from StreamReader without reading.
// Returns -1 if size cannot be determined.
func SizeStream(sr StreamReader) int64 {
	return SizeFromStream(sr)
}

// SizeFromStreamList sums sizes for known readers.
// Returns -1 if any size cannot be determined.
func SizeFromStreamList(readers []StreamReader) int64 {
	if len(readers) == 0 {
		return -1
	}

	var total int64
	for _, r := range readers {
		size := SizeAny(r)
		if size < 0 {
			return -1
		}
		total += size
	}

	return total
}

// SizeAny tries to determine size of common stream/file types.
// Returns -1 if size cannot be determined.
func SizeAny(x any) int64 {
	if x == nil {
		return -1
	}

	// 0) unwrap our wrapper that hides the underlying Reader
	switch v := x.(type) {
	case readerAtReadCloser:
		if v.readerAtReader != nil && v.readerAtReader.Reader != nil {
			return SizeAny(v.readerAtReader.Reader)
		}
	case *readerAtReadCloser:
		if v != nil && v.readerAtReader != nil && v.readerAtReader.Reader != nil {
			return SizeAny(v.readerAtReader.Reader)
		}
	}

	// 1) explicit: multipart.FileHeader has Size field
	if fh, ok := x.(*multipart.FileHeader); ok {
		if fh.Size >= 0 {
			return fh.Size
		}
	}

	// 2) Size() int64 (your own types)
	if sr, ok := x.(interface{ Size() int64 }); ok {
		if n := sr.Size(); n >= 0 {
			return n
		}
	}

	// 3) Len() int (buffers)
	if lr, ok := x.(interface{ Len() int }); ok {
		return int64(lr.Len())
	}

	// 4) os.File (stat)
	if f, ok := x.(*os.File); ok {
		if fi, err := f.Stat(); err == nil {
			return fi.Size()
		}
	}

	// 5) io.Seeker / io.ReadSeeker (seek end then restore)
	if seeker, ok := x.(io.Seeker); ok {
		cur, err := seeker.Seek(0, io.SeekCurrent)
		if err != nil {
			return -1
		}
		end, err := seeker.Seek(0, io.SeekEnd)
		if err != nil {
			_, _ = seeker.Seek(cur, io.SeekStart)
			return -1
		}
		if _, err := seeker.Seek(cur, io.SeekStart); err != nil {
			return -1
		}
		return end
	}

	return -1
}

func SizeFromReader(r io.Reader) int64 { return SizeAny(r) }

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

/* -------------------------------------------------------------------------- */
/*                             ToReaderAt                                      */
/* -------------------------------------------------------------------------- */

type ReaderAtResult struct {
	readerAt io.ReaderAt
	size     int64
	cleanup  func() error
	source   string // "direct" | "memory" | "tempFile"
}

const (
	readerAtSourceDirect   = "direct"
	readerAtSourceMemory   = "memory"
	readerAtSourceTempFile = "tempFile"
)

func (r *ReaderAtResult) ReaderAt() io.ReaderAt {
	if r == nil {
		return nil
	}
	return r.readerAt
}

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

		var closer io.Closer
		if c, ok := ra.(io.Closer); ok {
			closer = c
		} else if c, ok := r.(io.Closer); ok {
			closer = c
		}

		return &ReaderAtResult{
			readerAt: ra,
			size:     size,
			cleanup: func() error {
				if closer == nil {
					return nil
				}
				return closer.Close()
			},
			source: readerAtSourceDirect,
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
					source:   readerAtSourceMemory,
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
		source: readerAtSourceTempFile,
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
		source: readerAtSourceTempFile,
	}, nil
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

/* -------------------------------------------------------------------------- */
/*                                 UseReader                                   */
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
/*                                   Binder                                    */
/* -------------------------------------------------------------------------- */

// Binder collects StreamReaders and opens them eagerly on registration.
type Binder struct {
	mu       sync.Mutex
	readers  []io.Reader
	closers  []io.Closer
	sources  []StreamReader
	cleanups []func() error
	err      error
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

// UseAt opens a StreamReader and converts it to io.ReaderAt.
// Cleanup is automatically registered with the Binder.
func (b *Binder) UseAt(ctx context.Context, sr StreamReader, opts ...ToReaderAtOption) (io.ReaderAt, int64, error) {
	if b == nil {
		return nil, 0, errors.New("sio: nil binder")
	}

	b.mu.Lock()
	if b.err != nil {
		err := b.err
		b.mu.Unlock()
		return nil, 0, err
	}
	b.mu.Unlock()

	r, err := b.Use(sr)
	if err != nil {
		return nil, 0, err
	}

	res, err := ToReaderAt(ctx, r, opts...)
	if err != nil {
		return nil, 0, err
	}

	// Register cleanup
	b.mu.Lock()
	b.cleanups = append(b.cleanups, res.Cleanup)
	b.mu.Unlock()

	return res.ReaderAt(), res.Size(), nil
}

// addCleanup registers a cleanup function to be called when Binder is cleaned up.
func (b *Binder) addCleanup(fn func() error) {
	if b == nil || fn == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cleanups = append(b.cleanups, fn)
}

// cleanup closes all opened readers and cleans up all sources.
func (b *Binder) cleanup() {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	// Cleanup registered cleanups first (reverse order)
	for i := len(b.cleanups) - 1; i >= 0; i-- {
		if b.cleanups[i] != nil {
			_ = b.cleanups[i]()
		}
	}
	b.cleanups = nil

	// Close readers
	for _, c := range b.closers {
		if c != nil {
			_ = c.Close()
		}
	}

	// Cleanup sources
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
/*                              Bind* Functions                                */
/* -------------------------------------------------------------------------- */

// finalizeBinderOutput determines the final storage type and spills to file if needed.
func (s *ioSession) finalizeBinderOutput(b *Binder, out OutConfig, output *Output) error {
	var sources []StreamReader
	b.mu.Lock()
	if len(b.sources) > 0 {
		sources = append([]StreamReader(nil), b.sources...)
	}
	b.mu.Unlock()

	finalStorage := out.getStorageType(s.storageType)
	if out.storageType == nil {
		if out.autoThreshold != nil && *out.autoThreshold > 0 {
			finalStorage = s.storageByThreshold(SizeFromStreamList(sources), *out.autoThreshold)
		} else if s.autoThreshold > 0 {
			finalStorage = s.storageByThreshold(SizeFromStreamList(sources), s.autoThreshold)
		}
	}

	if finalStorage == File && output.StorageType() == Memory {
		if err := s.spillOutputToFile(output, out.ext); err != nil {
			return err
		}
	}

	return nil
}

// BindRead opens multiple StreamReaders lazily through Binder and executes fn.
// This is standalone (no session required).
func BindRead(ctx context.Context, fn func(ctx context.Context, b *Binder) error) error {
	if fn == nil {
		return ErrNilFunc
	}

	b := &Binder{}
	defer b.cleanup()

	return fn(ctx, b)
}

// BindProcess opens multiple StreamReaders lazily, creates an output, and executes fn.
// Requires a session in context.
func BindProcess(ctx context.Context, out OutConfig, fn func(ctx context.Context, b *Binder, w io.Writer) error) (*Output, error) {
	if fn == nil {
		return nil, ErrNilFunc
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
	if err := fn(ctx, b, w); err != nil {
		_ = w.Close()
		_ = output.cleanup()
		return nil, err
	}
	if err := w.Close(); err != nil {
		_ = output.cleanup()
		return nil, err
	}

	if err := iSes.finalizeBinderOutput(b, out, output); err != nil {
		_ = output.cleanup()
		return nil, err
	}

	return output, nil
}

// BindProcessResult is like BindProcess but also returns a result value.
func BindProcessResult[T any](ctx context.Context, out OutConfig, fn func(ctx context.Context, b *Binder, w io.Writer) (*T, error)) (*Output, *T, error) {
	if fn == nil {
		return nil, nil, ErrNilFunc
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
	res, execErr := fn(ctx, b, w)
	if execErr != nil {
		_ = w.Close()
		_ = output.cleanup()
		return nil, nil, execErr
	}
	if err := w.Close(); err != nil {
		_ = output.cleanup()
		return nil, nil, err
	}

	if err := iSes.finalizeBinderOutput(b, out, output); err != nil {
		_ = output.cleanup()
		return nil, nil, err
	}

	return output, res, nil
}

// BindReadResult is like BindRead but also returns a result value.
func BindReadResult[T any](ctx context.Context, fn func(ctx context.Context, b *Binder) (*T, error)) (*T, error) {
	if fn == nil {
		return nil, ErrNilFunc
	}

	b := &Binder{}
	defer b.cleanup()

	return fn(ctx, b)
}

// SafeClose safely closes an io.Closer, ignoring errors
func SafeClose(c io.Closer) {
	if c == nil {
		return
	}
	_ = c.Close()
}
