// Package fio provides streaming I/O utilities with session management,
// automatic resource cleanup, and flexible storage backends (memory/file).
package fio

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

/* -------------------------------------------------------------------------- */
/*                                  Consts                                    */
/* -------------------------------------------------------------------------- */

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

// DefaultBaseTempDir is the default base directory for file storage.
const DefaultBaseTempDir = "./temp"

// MB converts megabytes to bytes.
func MB(size int64) int64 { return size * 1024 * 1024 }

// ToExt adds a dot prefix to a format string.
func ToExt(format string) string { return "." + format }

/* -------------------------------------------------------------------------- */
/*                              HTTP Client                                    */
/* -------------------------------------------------------------------------- */

var httpClient = &http.Client{Timeout: 30 * time.Second}

// Config holds global configuration
type Config struct {
	client *http.Client
}

// NewConfig constructs a Config with a custom HTTP client.
func NewConfig(client *http.Client) Config { return Config{client: client} }

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
/*                                   Errors                                    */
/* -------------------------------------------------------------------------- */

var (
	ErrNilSource              = errors.New("fio: nil source")
	ErrOpenFailed             = errors.New("fio: cannot open reader")
	ErrIoManagerClosed        = errors.New("fio: manager is closed")
	ErrIoSessionClosed        = errors.New("fio: session is closed")
	ErrInvalidURL             = errors.New("fio: invalid URL")
	ErrDownloadFailed         = errors.New("fio: download failed")
	ErrNoSession              = errors.New("fio: session is nil")
	ErrFileStorageUnavailable = errors.New("fio: file storage requires directory")
	ErrInvalidSessionType     = errors.New("fio: invalid session type")
	ErrNilFunc                = errors.New("fio: fn is nil")
)

/* -------------------------------------------------------------------------- */
/*                              Storage Types                                  */
/* -------------------------------------------------------------------------- */

// StorageType defines how the IoManager stores data.
type StorageType int

const (
	// File stores data as temporary files on disk (default).
	File StorageType = iota
	// Memory stores data in memory as byte slices.
	Memory
)

// Storage converts string to StorageType.
// Supports: "file", "memory"
func Storage(s string) StorageType {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "memory":
		return Memory
	default:
		return File
	}
}

// String returns string representation of StorageType.
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
/*                                  Input                                      */
/* -------------------------------------------------------------------------- */

// Input represents an opened input source with metadata.
type Input struct {
	R       io.ReadCloser
	Size    int64
	Kind    string // "file" | "url" | "multipart" | "memory" | "reader" | "stream"
	Path    string // for file/url reference
	cleanup func() error
}

// Close closes the reader and cleans up resources.
func (in *Input) Close() error {
	if in == nil {
		return nil
	}
	var errs error
	if in.R != nil {
		errs = errors.Join(errs, in.R.Close())
		in.R = nil
	}
	if in.cleanup != nil {
		errs = errors.Join(errs, in.cleanup())
		in.cleanup = nil
	}
	return errs
}

// OpenIn opens any source type and returns an Input.
//
// Supported types:
//   - string: file path or URL (auto-detected)
//   - *multipart.FileHeader: multipart upload
//   - *os.File: open file handle
//   - []byte: in-memory bytes
//   - io.Reader / io.ReadCloser: generic reader
//   - *Output: output as input (via OpenReader)
//   - *Input: returns as-is
func OpenIn(ctx context.Context, src any) (*Input, error) {
	if src == nil {
		return nil, ErrNilSource
	}

	switch v := src.(type) {
	case *Input:
		return v, nil

	case *Output:
		if v == nil {
			return nil, errors.New("fio: nil *Output")
		}
		rc, err := v.OpenReader()
		if err != nil {
			return nil, err
		}
		return &Input{
			R:    rc,
			Size: v.Size(),
			Kind: KindStream,
			Path: v.Path(),
			cleanup: func() error {
				return rc.Close()
			},
		}, nil

	case string:
		s := strings.TrimSpace(v)
		if s == "" {
			return nil, errors.New("fio: empty string source")
		}
		if isURL(s) {
			return openURL(ctx, s)
		}
		return openFilePath(s)

	case *multipart.FileHeader:
		if v == nil {
			return nil, errors.New("fio: nil *multipart.FileHeader")
		}
		rc, err := v.Open()
		if err != nil {
			return nil, err
		}
		return &Input{
			R:       rc,
			Size:    v.Size,
			Kind:    KindMultipart,
			cleanup: nil, // rc.Close() handled by R
		}, nil

	case *os.File:
		if v == nil {
			return nil, errors.New("fio: nil *os.File")
		}
		return &Input{
			R:       v,
			Size:    fileSize(v),
			Kind:    KindFile,
			Path:    v.Name(),
			cleanup: nil, // v.Close() handled by R
		}, nil

	case []byte:
		return &Input{
			R:       io.NopCloser(bytes.NewReader(v)),
			Size:    int64(len(v)),
			Kind:    KindMemory,
			cleanup: nil,
		}, nil

	case io.ReadCloser:
		return &Input{
			R:       v,
			Size:    SizeAny(v),
			Kind:    KindReader,
			cleanup: nil, // v.Close() handled by R
		}, nil

	case io.Reader:
		return &Input{
			R:       io.NopCloser(v),
			Size:    SizeAny(v),
			Kind:    KindReader,
			cleanup: nil,
		}, nil

	default:
		return nil, fmt.Errorf("fio: unsupported source type %T", src)
	}
}

// OpenInList opens multiple sources and returns cleanup function.
func OpenInList(ctx context.Context, srcs ...any) ([]*Input, func() error, error) {
	if len(srcs) == 0 {
		return nil, func() error { return nil }, nil
	}

	ins := make([]*Input, 0, len(srcs))
	cleanups := make([]func() error, 0, len(srcs))

	for _, s := range srcs {
		in, err := OpenIn(ctx, s)
		if err != nil {
			// Cleanup already opened
			for _, c := range cleanups {
				_ = c()
			}
			return nil, func() error { return nil }, err
		}
		ins = append(ins, in)
		cleanups = append(cleanups, in.Close)
	}

	return ins, JoinCleanup(cleanups...), nil
}

func isURL(s string) bool {
	u, err := url.Parse(s)
	if err != nil {
		return false
	}
	return u.Scheme == "http" || u.Scheme == "https"
}

func fileSize(f *os.File) int64 {
	if f == nil {
		return -1
	}
	fi, err := f.Stat()
	if err != nil {
		return -1
	}
	return fi.Size()
}

func openFilePath(path string) (*Input, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &Input{
		R:       f,
		Size:    fileSize(f),
		Kind:    KindFile,
		Path:    path,
		cleanup: nil, // f.Close() handled by R
	}, nil
}

func openURL(ctx context.Context, urlStr string) (*Input, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
	if err != nil {
		return nil, err
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDownloadFailed, err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("%w: %s", ErrDownloadFailed, resp.Status)
	}

	return &Input{
		R:       resp.Body,
		Size:    resp.ContentLength,
		Kind:    KindURL,
		Path:    urlStr,
		cleanup: nil, // resp.Body.Close() handled by R
	}, nil
}

// JoinCleanup combines multiple cleanup functions into one (errors.Join).
func JoinCleanup(fns ...func() error) func() error {
	return func() error {
		var errs error
		for _, fn := range fns {
			if fn == nil {
				continue
			}
			errs = errors.Join(errs, fn())
		}
		return errs
	}
}

/* -------------------------------------------------------------------------- */
/*                                 OutHandle                                   */
/* -------------------------------------------------------------------------- */

// OutHandle provides direct access to output writer.
type OutHandle struct {
	W       io.WriteCloser
	output  *Output
	session *ioSession
	done    bool
}

// Finalize closes the writer and returns the Output (idempotent).
func (h *OutHandle) Finalize() (*Output, error) {
	if h == nil {
		return nil, errors.New("fio: nil OutHandle")
	}
	if h.done {
		if h.output == nil {
			return nil, errors.New("fio: nil output")
		}
		return h.output, nil
	}
	h.done = true

	if h.output == nil {
		if h.W != nil {
			_ = h.W.Close()
			h.W = nil
		}
		return nil, errors.New("fio: nil output")
	}

	if h.W != nil {
		if err := h.W.Close(); err != nil {
			_ = h.output.cleanup()
			h.W = nil
			return nil, err
		}
		h.W = nil
	}

	return h.output, nil
}

// Cleanup aborts the output (use if error occurs before Finalize).
func (h *OutHandle) Cleanup() error {
	if h == nil || h.done {
		return nil
	}
	h.done = true

	var errs error
	if h.W != nil {
		errs = errors.Join(errs, h.W.Close())
		h.W = nil
	}
	if h.output != nil {
		errs = errors.Join(errs, h.output.cleanup())
	}
	return errs
}

// Output returns the underlying Output (may be nil).
func (h *OutHandle) Output() *Output {
	if h == nil {
		return nil
	}
	return h.output
}

// NewOut creates an output handle for manual writing.
// Requires session in context.
func NewOut(ctx context.Context, out OutConfig, sizeHint ...int64) (*OutHandle, error) {
	ses := Session(ctx)
	if ses == nil {
		return nil, ErrNoSession
	}

	iSes, ok := ses.(*ioSession)
	if !ok {
		return nil, ErrInvalidSessionType
	}

	if err := iSes.ensureOpen(); err != nil {
		return nil, err
	}

	// Determine storage type
	var hint int64 = -1
	if len(sizeHint) > 0 {
		hint = sizeHint[0]
	}

	storageType := iSes.storageType
	if out.storageType != nil {
		storageType = *out.storageType
	} else if out.autoThreshold != nil && *out.autoThreshold > 0 && hint >= *out.autoThreshold {
		storageType = File
	} else if hint >= 0 && iSes.autoThreshold > 0 && hint >= iSes.autoThreshold {
		storageType = File
	}

	output, err := iSes.newOutputWithStorage(out.ext, storageType)
	if err != nil {
		return nil, err
	}

	w, err := output.OpenWriter(hint)
	if err != nil {
		_ = output.cleanup()
		return nil, err
	}

	return &OutHandle{
		W:       w,
		output:  output,
		session: iSes,
	}, nil
}

/* -------------------------------------------------------------------------- */
/*                                   Output                                    */
/* -------------------------------------------------------------------------- */

// Output represents a file or memory buffer produced by processing.
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
		return nil, fmt.Errorf("fio: output is cleaned up")
	}

	// Memory mode
	if o.storageType == Memory {
		if o.data == nil {
			return NopCloser(bytes.NewReader(nil)), nil
		}
		return NopCloser(bytes.NewReader(o.data)), nil
	}

	// File mode
	return os.Open(o.path)
}

// OpenWriter opens writer. For Memory mode, it can pre-grow buffer based on hint (optional).
func (o *Output) OpenWriter(sizeHint ...int64) (io.WriteCloser, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return nil, fmt.Errorf("fio: output is cleaned up")
	}

	if o.storageType == Memory {
		buf := &bytes.Buffer{}
		if len(sizeHint) > 0 && sizeHint[0] > 0 {
			// conservative cap to avoid huge alloc explosions
			const capGrow = int64(64 << 20) // 64MB
			n := sizeHint[0]
			if n > capGrow {
				n = capGrow
			}
			buf.Grow(int(n))
		}
		return &bytesWriteCloser{buf: buf, output: o}, nil
	}

	return os.Create(o.path)
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

// cleanup removes the output file or clears memory.
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

// Keep marks the output file as persistent (survives session cleanup).
func (o *Output) Keep() *Output {
	o.mu.Lock()
	if o.storageType == File {
		o.keep = true
	}
	o.mu.Unlock()
	return o
}

// SaveAs copies the output to a persistent path outside of the session dir.
func (o *Output) SaveAs(path string) error {
	r, err := o.OpenReader()
	if err != nil {
		return err
	}
	defer r.Close()

	_, err = copyToFile(r, path)
	return err
}

// Bytes loads the entire output into memory (careful for very large files).
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
	return io.Copy(w, r)
}

/* -------------------------------------------------------------------------- */
/*                          Scope + Do (One-liner)                             */
/* -------------------------------------------------------------------------- */

// Scope owns opened inputs + (optional) one output handle.
// Everything is auto-cleaned at the end of Do().
type Scope struct {
	ctx context.Context

	ins []*Input

	outHandle *OutHandle
	out       *Output // finalized output (after finalize)
	err       error
}

// Open opens anything that OpenIn supports.
// If it fails, Do() will return the accumulated error.
func (s *Scope) Open(src any) io.Reader {
	in, err := OpenIn(s.ctx, src)
	if err != nil {
		s.err = errors.Join(s.err, err)
		return errReader{err: err}
	}
	s.ins = append(s.ins, in)
	return in.R
}

// NewOut creates output writer managed by the scope.
// Only one output per Do() to keep API simple/deterministic.
func (s *Scope) NewOut(out OutConfig, sizeHint ...int64) io.Writer {
	if s.outHandle != nil {
		err := errors.New("fio: NewOut called more than once")
		s.err = errors.Join(s.err, err)
		return errWriter{err: err}
	}

	oh, err := NewOut(s.ctx, out, sizeHint...)
	if err != nil {
		s.err = errors.Join(s.err, err)
		return errWriter{err: err}
	}

	s.outHandle = oh
	return oh.W
}

// finalize output and close inputs.
func (s *Scope) cleanupAndFinalize(fnErr error) (*Output, error) {
	if s.err != nil {
		fnErr = errors.Join(fnErr, s.err)
	}

	// close inputs
	var cerr error
	for i := len(s.ins) - 1; i >= 0; i-- {
		if s.ins[i] != nil {
			if err := s.ins[i].Close(); err != nil {
				cerr = errors.Join(cerr, err)
			}
		}
	}
	s.ins = nil

	// if function errored, abort output
	if fnErr != nil {
		if s.outHandle != nil {
			_ = s.outHandle.Cleanup()
			s.outHandle = nil
		}
		return nil, fnErr
	}

	// finalize output if exists
	if s.outHandle == nil {
		if cerr != nil {
			return nil, cerr
		}
		return nil, nil
	}

	out, err := s.outHandle.Finalize()
	s.outHandle = nil
	if err != nil {
		return nil, err
	}

	if cerr != nil {
		return out, cerr
	}
	return out, nil
}

// Do executes fn with guaranteed cleanup + optional output finalize.
func Do[T any](ctx context.Context, fn func(s *Scope) (T, error)) (*Output, T, error) {
	var zero T
	if fn == nil {
		return nil, zero, errors.New("fio: nil fn")
	}

	sc := &Scope{ctx: ctx}
	res, err := fn(sc)

	out, finErr := sc.cleanupAndFinalize(err)
	if finErr != nil {
		return nil, zero, finErr
	}
	return out, res, nil
}

/* ---------------- helpers ---------------- */

type errReader struct{ err error }

func (e errReader) Read(p []byte) (int, error) { return 0, e.err }

type errWriter struct{ err error }

func (e errWriter) Write(p []byte) (int, error) { return 0, e.err }

/* -------------------------------------------------------------------------- */
/*                                   Size                                     */
/* -------------------------------------------------------------------------- */

// SizeAny tries to determine size of common stream/file types.
// Returns -1 if size cannot be determined.
func SizeAny(x any) int64 {
	if x == nil {
		return -1
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
/*                                 IoSession                                   */
/* -------------------------------------------------------------------------- */

// IoSession manages temporary files for a single job/request.
type IoSession interface {
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

// newOutputWithStorage creates output with specified storage type.
func (s *ioSession) newOutputWithStorage(ext string, storageType StorageType) (*Output, error) {
	if err := s.ensureOpen(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Memory mode
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

	// File mode: create temp file
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

	var errs error

	for _, out := range s.outputs {
		out.mu.Lock()
		shouldSkip := out.keep && !out.closed && out.storageType == File
		out.mu.Unlock()
		if shouldSkip {
			continue
		}
		errs = errors.Join(errs, out.cleanup())
	}

	for _, fn := range s.cleanupFns {
		if fn == nil {
			continue
		}
		errs = errors.Join(errs, fn())
	}
	s.cleanupFns = nil

	if s.dir == "" {
		return errs
	}

	entries, err := os.ReadDir(s.dir)
	if err != nil && !os.IsNotExist(err) {
		errs = errors.Join(errs, err)
		return errs
	}

	for _, e := range entries {
		full := filepath.Join(s.dir, e.Name())
		if s.isKeptPath(full) {
			continue
		}
		if err := os.RemoveAll(full); err != nil && !os.IsNotExist(err) {
			errs = errors.Join(errs, err)
		}
	}

	if err := os.Remove(s.dir); err != nil && !os.IsNotExist(err) {
		errs = errors.Join(errs, err)
	}
	return errs
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

/* -------------------------------------------------------------------------- */
/*                            Manager Options                                  */
/* -------------------------------------------------------------------------- */

// ManagerOption is a functional option for configuring IoManager.
type ManagerOption interface {
	applyManager(*managerConfig)
}

// ManagerOptionFunc adapts a function to ManagerOption.
type ManagerOptionFunc func(*managerConfig)

func (f ManagerOptionFunc) applyManager(c *managerConfig) { f(c) }

// managerConfig holds configuration for IoManager.
type managerConfig struct {
	autoThreshold int64 // 0 = disabled, >0 = threshold in bytes for auto storage switching
}

type thresholdOption int64

func (t thresholdOption) applyManager(c *managerConfig) { c.autoThreshold = int64(t) }
func (t thresholdOption) applyOut(o *OutConfig) {
	val := int64(t)
	o.autoThreshold = &val
}

// WithThreshold sets the automatic storage threshold.
func WithThreshold(bytes int64) thresholdOption { return thresholdOption(bytes) }

/* -------------------------------------------------------------------------- */
/*                                  Manager                                    */
/* -------------------------------------------------------------------------- */

// IoManager manages sessions and their temporary directories.
type IoManager interface {
	NewSession() (IoSession, error)
	Cleanup() error
}

// manager manages a root directory under which multiple IoSessions create their own dirs.
type manager struct {
	mu            sync.Mutex
	baseDir       string
	closed        bool
	storageType   StorageType
	autoThreshold int64
}

func NewIoManager(baseDir string, storageType StorageType, opts ...ManagerOption) (IoManager, error) {
	config := &managerConfig{autoThreshold: 0}
	for _, opt := range opts {
		if opt != nil {
			opt.applyManager(config)
		}
	}

	if strings.TrimSpace(baseDir) == "" {
		dir, err := os.MkdirTemp("", "fio-")
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

	return &ioSession{
		manager:       m,
		dir:           dir,
		storageType:   m.storageType,
		autoThreshold: m.autoThreshold,
	}, nil
}

func (m *manager) Cleanup() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil
	}
	m.closed = true

	if strings.TrimSpace(m.baseDir) == "" {
		return nil
	}

	err := os.RemoveAll(m.baseDir)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

/* -------------------------------------------------------------------------- */
/*                              Output Options                                 */
/* -------------------------------------------------------------------------- */

// OutConfig configures output behavior.
type OutConfig struct {
	ext           string
	storageType   *StorageType
	autoThreshold *int64
}

// OutOption configures output behavior.
type OutOption interface {
	applyOut(*OutConfig)
}

// OutOptionFunc adapts a function to OutOption.
type OutOptionFunc func(*OutConfig)

func (f OutOptionFunc) applyOut(o *OutConfig) { f(o) }

func (st StorageType) applyOut(o *OutConfig) { o.storageType = &st }

// Out creates output configuration.
func Out(ext string, opts ...OutOption) OutConfig {
	o := OutConfig{ext: ext}
	for _, opt := range opts {
		if opt != nil {
			opt.applyOut(&o)
		}
	}
	return o
}

func (o OutConfig) getStorageType(sessionDefault StorageType) StorageType {
	if o.storageType != nil {
		return *o.storageType
	}
	return sessionDefault
}

func (o OutConfig) Ext() string               { return o.ext }
func (o OutConfig) StorageType() *StorageType { return o.storageType }
func (o OutConfig) AutoThreshold() *int64     { return o.autoThreshold }

/* -------------------------------------------------------------------------- */
/*                           bytesWriteCloser                                  */
/* -------------------------------------------------------------------------- */

type bytesWriteCloser struct {
	buf    *bytes.Buffer
	output *Output
}

func (b *bytesWriteCloser) Write(p []byte) (int, error) { return b.buf.Write(p) }

func (b *bytesWriteCloser) Close() error {
	if b.output != nil {
		b.output.mu.Lock()
		b.output.data = b.buf.Bytes()
		b.output.mu.Unlock()
	}
	return nil
}

/* -------------------------------------------------------------------------- */
/*                               NopCloser                                     */
/* -------------------------------------------------------------------------- */

type readerAtReader struct {
	io.Reader
	io.ReaderAt
}

type readerAtReadCloser struct{ *readerAtReader }

func (r readerAtReadCloser) Close() error { return nil }

// NopCloser wraps an io.Reader as io.ReadCloser, preserving ReaderAt if present.
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
/*                                   Copy                                     */
/* -------------------------------------------------------------------------- */

func Copy(dst io.Writer, src io.Reader) (int64, error) { return io.Copy(dst, src) }

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
