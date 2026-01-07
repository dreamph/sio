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
	Zip  = ".zip"
)

const (
	KindFile      = "file"
	KindURL       = "url"
	KindMultipart = "multipart"
	KindMemory    = "memory"
	KindReader    = "reader"
	KindStream    = "stream"
)

const DefaultBaseTempDir = "./temp"

func MB(size int64) int64        { return size * 1024 * 1024 }
func ToExt(format string) string { return "." + format }

/* -------------------------------------------------------------------------- */
/*                              HTTP Client                                    */
/* -------------------------------------------------------------------------- */

var httpClient = &http.Client{Timeout: 30 * time.Second}

type Config struct{ client *http.Client }

func NewConfig(client *http.Client) Config { return Config{client: client} }

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

type StorageType int

const (
	File StorageType = iota
	Memory
)

func Storage(s string) StorageType {
	if strings.ToLower(strings.TrimSpace(s)) == "memory" {
		return Memory
	}
	return File
}

func (s StorageType) String() string {
	if s == Memory {
		return "memory"
	}
	return "file"
}

/* -------------------------------------------------------------------------- */
/*                                  Input                                      */
/* -------------------------------------------------------------------------- */

// Input represents an opened input source with metadata.
type Input struct {
	R       io.ReadCloser
	Size    int64
	Kind    string
	Path    string
	cleanup func() error

	// Reusable support
	reusable   bool
	needsReset bool        // true after first use
	ra         io.ReaderAt // for reusable inputs
	data       []byte      // for memory-backed reusable
}

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
	// Clear reusable data
	in.ra = nil
	in.data = nil
	in.needsReset = false
	return errs
}

// Reset resets the reader to beginning (for reusable inputs).
// Returns error if input is not reusable.
func (in *Input) Reset() error {
	if in == nil {
		return errors.New("fio: nil input")
	}
	if !in.reusable {
		return errors.New("fio: input is not reusable")
	}

	// Skip reset if not needed (first use or already reset)
	if !in.needsReset {
		return nil
	}

	// Memory-backed
	if in.data != nil {
		in.R = io.NopCloser(bytes.NewReader(in.data))
		in.needsReset = false
		return nil
	}

	// ReaderAt-backed (file)
	if in.ra != nil {
		in.R = &readerAtCloser{
			r:      io.NewSectionReader(in.ra, 0, in.Size),
			closer: nil, // don't close underlying ra
		}
		in.needsReset = false
		return nil
	}

	// Seeker-backed
	if seeker, ok := in.R.(io.Seeker); ok {
		_, err := seeker.Seek(0, io.SeekStart)
		if err == nil {
			in.needsReset = false
		}
		return err
	}

	return errors.New("fio: cannot reset input")
}

// markUsed marks the input as used (needs reset before next use).
// Called automatically by Scope.Open.
func (in *Input) markUsed() {
	if in != nil && in.reusable {
		in.needsReset = true
	}
}

// IsReusable returns true if input can be reset and reused.
func (in *Input) IsReusable() bool {
	return in != nil && in.reusable
}

// ReaderAt returns io.ReaderAt if available (for random access).
// Returns nil if input doesn't support random access.
// Available for: files, []byte, reusable inputs.
func (in *Input) ReaderAt() io.ReaderAt {
	if in == nil {
		return nil
	}

	// Reusable with ReaderAt
	if in.ra != nil {
		return in.ra
	}

	// Memory-backed
	if in.data != nil {
		return bytes.NewReader(in.data)
	}

	// Check if underlying reader supports ReaderAt
	if ra, ok := in.R.(io.ReaderAt); ok {
		return ra
	}

	return nil
}

// HasReaderAt returns true if ReaderAt() will return non-nil.
func (in *Input) HasReaderAt() bool {
	return in.ReaderAt() != nil
}

// readerAtCloser wraps SectionReader with optional closer
type readerAtCloser struct {
	r      *io.SectionReader
	closer io.Closer
}

func (r *readerAtCloser) Read(p []byte) (int, error) { return r.r.Read(p) }
func (r *readerAtCloser) Close() error {
	if r.closer != nil {
		return r.closer.Close()
	}
	return nil
}

/* -------------------------------------------------------------------------- */
/*                              Input Options                                  */
/* -------------------------------------------------------------------------- */

// InOption configures OpenIn behavior.
type InOption func(*inConfig)

type inConfig struct {
	reusable bool
}

// Reusable makes the input reusable across multiple Do() calls.
// The input will buffer data in memory or use ReaderAt for files.
func Reusable() InOption {
	return func(c *inConfig) {
		c.reusable = true
	}
}

// OpenIn opens any source type and returns an Input.
func OpenIn(ctx context.Context, src any, opts ...InOption) (*Input, error) {
	if src == nil {
		return nil, ErrNilSource
	}

	cfg := &inConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(cfg)
		}
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
		in := &Input{R: rc, Size: v.Size(), Kind: KindStream, Path: v.Path()}
		if cfg.reusable {
			return makeReusable(in)
		}
		return in, nil

	case string:
		in, err := openString(ctx, v)
		if err != nil {
			return nil, err
		}
		if cfg.reusable {
			return makeReusable(in)
		}
		return in, nil

	case *multipart.FileHeader:
		if v == nil {
			return nil, errors.New("fio: nil *multipart.FileHeader")
		}
		rc, err := v.Open()
		if err != nil {
			return nil, err
		}
		in := &Input{R: rc, Size: v.Size, Kind: KindMultipart}
		if cfg.reusable {
			return makeReusable(in)
		}
		return in, nil

	case *os.File:
		if v == nil {
			return nil, errors.New("fio: nil *os.File")
		}
		in := &Input{R: v, Size: fileSize(v), Kind: KindFile, Path: v.Name()}
		if cfg.reusable {
			return makeReusableFile(in, v)
		}
		return in, nil

	case []byte:
		in := &Input{
			R:        io.NopCloser(bytes.NewReader(v)),
			Size:     int64(len(v)),
			Kind:     KindMemory,
			reusable: true, // bytes are inherently reusable
			data:     v,
		}
		return in, nil

	case io.ReadCloser:
		in := &Input{R: v, Size: SizeAny(v), Kind: KindReader}
		if cfg.reusable {
			return makeReusable(in)
		}
		return in, nil

	case io.Reader:
		in := &Input{R: io.NopCloser(v), Size: SizeAny(v), Kind: KindReader}
		if cfg.reusable {
			return makeReusable(in)
		}
		return in, nil

	default:
		return nil, fmt.Errorf("fio: unsupported source type %T", src)
	}
}

// makeReusable converts input to reusable by reading into memory
func makeReusable(in *Input) (*Input, error) {
	// Already reusable
	if in.reusable {
		return in, nil
	}

	// For files, use ReaderAt directly
	if f, ok := in.R.(*os.File); ok {
		return makeReusableFile(in, f)
	}

	// For other readers, buffer into memory
	data, err := io.ReadAll(in.R)
	if err != nil {
		in.Close()
		return nil, err
	}
	in.R.Close()

	in.R = io.NopCloser(bytes.NewReader(data))
	in.Size = int64(len(data))
	in.data = data
	in.reusable = true
	return in, nil
}

// makeReusableFile makes file input reusable via ReaderAt
func makeReusableFile(in *Input, f *os.File) (*Input, error) {
	size := fileSize(f)
	if size < 0 {
		// Can't determine size, fall back to memory
		return makeReusable(in)
	}

	// Seek to beginning
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return makeReusable(in)
	}

	in.ra = f
	in.Size = size
	in.reusable = true
	in.R = &readerAtCloser{
		r:      io.NewSectionReader(f, 0, size),
		closer: nil,
	}
	in.cleanup = f.Close // close file when input is closed

	return in, nil
}

func openString(ctx context.Context, s string) (*Input, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, errors.New("fio: empty string source")
	}
	if isURL(s) {
		return openURL(ctx, s)
	}
	return openFilePath(s)
}

func isURL(s string) bool {
	u, err := url.Parse(s)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https")
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
	return &Input{R: f, Size: fileSize(f), Kind: KindFile, Path: path}, nil
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

	return &Input{R: resp.Body, Size: resp.ContentLength, Kind: KindURL, Path: urlStr}, nil
}

/* -------------------------------------------------------------------------- */
/*                           openReader (no alloc)                             */
/* -------------------------------------------------------------------------- */

// openReader opens source and returns reader + cleanup (minimal allocation path)
func openReader(ctx context.Context, src any) (io.ReadCloser, func() error, int64, error) {
	switch v := src.(type) {
	case *Input:
		return v.R, v.Close, v.Size, nil

	case *Output:
		if v == nil {
			return nil, nil, -1, errors.New("fio: nil *Output")
		}
		rc, err := v.OpenReader()
		if err != nil {
			return nil, nil, -1, err
		}
		return rc, rc.Close, v.Size(), nil

	case string:
		s := strings.TrimSpace(v)
		if s == "" {
			return nil, nil, -1, errors.New("fio: empty string source")
		}
		if isURL(s) {
			return openURLDirect(ctx, s)
		}
		return openFileDirect(s)

	case *multipart.FileHeader:
		if v == nil {
			return nil, nil, -1, errors.New("fio: nil *multipart.FileHeader")
		}
		rc, err := v.Open()
		if err != nil {
			return nil, nil, -1, err
		}
		return rc, rc.Close, v.Size, nil

	case *os.File:
		if v == nil {
			return nil, nil, -1, errors.New("fio: nil *os.File")
		}
		return v, v.Close, fileSize(v), nil

	case []byte:
		return io.NopCloser(bytes.NewReader(v)), nil, int64(len(v)), nil

	case io.ReadCloser:
		return v, v.Close, SizeAny(v), nil

	case io.Reader:
		return io.NopCloser(v), nil, SizeAny(v), nil

	default:
		return nil, nil, -1, fmt.Errorf("fio: unsupported source type %T", src)
	}
}

func openFileDirect(path string) (io.ReadCloser, func() error, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, -1, err
	}
	return f, f.Close, fileSize(f), nil
}

func openURLDirect(ctx context.Context, urlStr string) (io.ReadCloser, func() error, int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
	if err != nil {
		return nil, nil, -1, err
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, nil, -1, fmt.Errorf("%w: %v", ErrDownloadFailed, err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_ = resp.Body.Close()
		return nil, nil, -1, fmt.Errorf("%w: %s", ErrDownloadFailed, resp.Status)
	}

	return resp.Body, resp.Body.Close, resp.ContentLength, nil
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

func (h *OutHandle) Finalize() (*Output, error) {
	if h == nil {
		return nil, errors.New("fio: nil OutHandle")
	}
	if h.done {
		return h.output, nil
	}
	h.done = true

	if h.W != nil {
		if err := h.W.Close(); err != nil {
			if h.output != nil {
				_ = h.output.cleanup()
			}
			return nil, err
		}
	}

	return h.output, nil
}

func (h *OutHandle) Cleanup() error {
	if h == nil || h.done {
		return nil
	}
	h.done = true

	var errs error
	if h.W != nil {
		errs = errors.Join(errs, h.W.Close())
	}
	if h.output != nil {
		errs = errors.Join(errs, h.output.cleanup())
	}
	return errs
}

func (h *OutHandle) Output() *Output {
	if h == nil {
		return nil
	}
	return h.output
}

// NewOut creates an output handle for manual writing.
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

	return &OutHandle{W: w, output: output, session: iSes}, nil
}

/* -------------------------------------------------------------------------- */
/*                                   Output                                    */
/* -------------------------------------------------------------------------- */

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
		return nil, errors.New("fio: output is cleaned up")
	}

	if o.storageType == Memory {
		return io.NopCloser(bytes.NewReader(o.data)), nil
	}
	return os.Open(o.path)
}

func (o *Output) OpenWriter(sizeHint ...int64) (io.WriteCloser, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return nil, errors.New("fio: output is cleaned up")
	}

	if o.storageType == Memory {
		buf := &bytes.Buffer{}
		if len(sizeHint) > 0 && sizeHint[0] > 0 {
			n := sizeHint[0]
			if n > 64<<20 { // cap at 64MB
				n = 64 << 20
			}
			buf.Grow(int(n))
		}
		return &bytesWriteCloser{buf: buf, output: o}, nil
	}
	return os.Create(o.path)
}

func (o *Output) Data() []byte {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.storageType == Memory {
		return o.data
	}
	return nil
}

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

func (o *Output) Keep() *Output {
	o.mu.Lock()
	if o.storageType == File {
		o.keep = true
	}
	o.mu.Unlock()
	return o
}

func (o *Output) SaveAs(path string) error {
	r, err := o.OpenReader()
	if err != nil {
		return err
	}
	defer r.Close()
	return copyToFile(r, path)
}

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
/*                          Scope (Optimized)                                  */
/* -------------------------------------------------------------------------- */

// Scope tracks opened resources for automatic cleanup.
// Optimized: stores only cleanup functions, not full Input structs.
type Scope struct {
	ctx       context.Context
	cleanups  []func() error // cleanup functions (no *Input allocation)
	outHandle *OutHandle
	err       error
}

// Open opens source and returns reader. Cleanup is automatic.
func (s *Scope) Open(src any) io.Reader {
	// If src is reusable *Input, reset if needed and use directly
	if in, ok := src.(*Input); ok && in.IsReusable() {
		if err := in.Reset(); err != nil {
			s.err = errors.Join(s.err, err)
			return errReader{err}
		}
		in.markUsed() // mark for reset on next use
		// Don't add cleanup - caller manages reusable input
		return in.R
	}

	rc, cleanup, _, err := openReader(s.ctx, src)
	if err != nil {
		s.err = errors.Join(s.err, err)
		return errReader{err}
	}
	if cleanup != nil {
		s.cleanups = append(s.cleanups, cleanup)
	}
	return rc
}

// OpenSized opens source and returns reader + size.
func (s *Scope) OpenSized(src any) (io.Reader, int64) {
	// If src is reusable *Input, reset if needed and use directly
	if in, ok := src.(*Input); ok && in.IsReusable() {
		if err := in.Reset(); err != nil {
			s.err = errors.Join(s.err, err)
			return errReader{err}, -1
		}
		in.markUsed() // mark for reset on next use
		// Don't add cleanup - caller manages reusable input
		return in.R, in.Size
	}

	rc, cleanup, size, err := openReader(s.ctx, src)
	if err != nil {
		s.err = errors.Join(s.err, err)
		return errReader{err}, -1
	}
	if cleanup != nil {
		s.cleanups = append(s.cleanups, cleanup)
	}
	return rc, size
}

// OpenReaderAt opens source and returns ReaderAt + size.
// For non-seekable sources, data is buffered into memory.
func (s *Scope) OpenReaderAt(src any) (io.ReaderAt, int64) {
	// If src is *Input with ReaderAt support
	if in, ok := src.(*Input); ok {
		if ra := in.ReaderAt(); ra != nil {
			// Don't add cleanup for reusable - caller manages
			if !in.IsReusable() {
				s.cleanups = append(s.cleanups, in.Close)
			}
			return ra, in.Size
		}
	}

	// Open and check for ReaderAt
	rc, cleanup, size, err := openReader(s.ctx, src)
	if err != nil {
		s.err = errors.Join(s.err, err)
		return nil, -1
	}

	// If already ReaderAt, use directly
	if ra, ok := rc.(io.ReaderAt); ok {
		if cleanup != nil {
			s.cleanups = append(s.cleanups, cleanup)
		}
		return ra, size
	}

	// Buffer into memory for ReaderAt support
	data, err := io.ReadAll(rc)
	if cleanup != nil {
		_ = cleanup()
	}
	if err != nil {
		s.err = errors.Join(s.err, err)
		return nil, -1
	}

	return bytes.NewReader(data), int64(len(data))
}

// NewOut creates output writer.
func (s *Scope) NewOut(out OutConfig, sizeHint ...int64) io.Writer {
	if s.outHandle != nil {
		err := errors.New("fio: NewOut called more than once")
		s.err = errors.Join(s.err, err)
		return errWriter{err}
	}

	oh, err := NewOut(s.ctx, out, sizeHint...)
	if err != nil {
		s.err = errors.Join(s.err, err)
		return errWriter{err}
	}

	s.outHandle = oh
	return oh.W
}

// Err returns accumulated error.
func (s *Scope) Err() error { return s.err }

func (s *Scope) cleanup() {
	// Cleanup in reverse order
	for i := len(s.cleanups) - 1; i >= 0; i-- {
		if s.cleanups[i] != nil {
			_ = s.cleanups[i]()
		}
	}
	s.cleanups = nil
}

func (s *Scope) finalize(fnErr error) (*Output, error) {
	if s.err != nil {
		fnErr = errors.Join(fnErr, s.err)
	}

	// Cleanup inputs
	s.cleanup()

	// If error, abort output
	if fnErr != nil {
		if s.outHandle != nil {
			_ = s.outHandle.Cleanup()
		}
		return nil, fnErr
	}

	// Finalize output
	if s.outHandle == nil {
		return nil, nil
	}

	return s.outHandle.Finalize()
}

/* -------------------------------------------------------------------------- */
/*                              Do (Generic)                                   */
/* -------------------------------------------------------------------------- */

// Do executes fn with automatic cleanup and optional output.
func Do[T any](ctx context.Context, fn func(s *Scope) (T, error)) (*Output, T, error) {
	var zero T
	if fn == nil {
		return nil, zero, ErrNilFunc
	}

	// Direct allocation - no pool needed (compiler may stack-allocate)
	s := &Scope{
		ctx:      ctx,
		cleanups: make([]func() error, 0, 4), // pre-alloc for common case
	}

	res, err := fn(s)
	out, finErr := s.finalize(err)

	if finErr != nil {
		return nil, zero, finErr
	}
	return out, res, nil
}

/* -------------------------------------------------------------------------- */
/*                           One-liner Helpers                                 */
/* -------------------------------------------------------------------------- */

// Copy copies src to output.
func Copy(ctx context.Context, src any, out OutConfig) (*Output, error) {
	o, _, err := Do(ctx, func(s *Scope) (struct{}, error) {
		r, size := s.OpenSized(src)
		w := s.NewOut(out, size)
		_, err := io.Copy(w, r)
		return struct{}{}, err
	})
	return o, err
}

// Process applies fn to src and writes to output.
func Process(ctx context.Context, src any, out OutConfig, fn func(r io.Reader, w io.Writer) error) (*Output, error) {
	if fn == nil {
		return nil, ErrNilFunc
	}
	o, _, err := Do(ctx, func(s *Scope) (struct{}, error) {
		r, size := s.OpenSized(src)
		w := s.NewOut(out, size)
		return struct{}{}, fn(r, w)
	})
	return o, err
}

// ProcessResult applies fn and returns result.
func ProcessResult[T any](ctx context.Context, src any, out OutConfig, fn func(r io.Reader, w io.Writer) (T, error)) (*Output, T, error) {
	if fn == nil {
		var zero T
		return nil, zero, ErrNilFunc
	}
	return Do(ctx, func(s *Scope) (T, error) {
		r, size := s.OpenSized(src)
		w := s.NewOut(out, size)
		return fn(r, w)
	})
}

// ProcessAt applies fn with ReaderAt to src and writes to output.
// Useful for formats requiring random access (PDF, ZIP, etc.)
func ProcessAt(ctx context.Context, src any, out OutConfig, fn func(ra io.ReaderAt, size int64, w io.Writer) error) (*Output, error) {
	if fn == nil {
		return nil, ErrNilFunc
	}
	o, _, err := Do(ctx, func(s *Scope) (struct{}, error) {
		ra, size := s.OpenReaderAt(src)
		if ra == nil {
			return struct{}{}, errors.New("fio: cannot get ReaderAt")
		}
		w := s.NewOut(out, size)
		return struct{}{}, fn(ra, size, w)
	})
	return o, err
}

// ProcessAtResult applies fn with ReaderAt and returns result.
func ProcessAtResult[T any](ctx context.Context, src any, out OutConfig, fn func(ra io.ReaderAt, size int64, w io.Writer) (T, error)) (*Output, T, error) {
	if fn == nil {
		var zero T
		return nil, zero, ErrNilFunc
	}
	return Do(ctx, func(s *Scope) (T, error) {
		ra, size := s.OpenReaderAt(src)
		if ra == nil {
			var zero T
			return zero, errors.New("fio: cannot get ReaderAt")
		}
		w := s.NewOut(out, size)
		return fn(ra, size, w)
	})
}

// Read reads src without creating output.
func Read[T any](ctx context.Context, src any, fn func(r io.Reader) (T, error)) (T, error) {
	if fn == nil {
		var zero T
		return zero, ErrNilFunc
	}
	_, res, err := Do(ctx, func(s *Scope) (T, error) {
		r := s.Open(src)
		return fn(r)
	})
	return res, err
}

// ReadAt reads src as ReaderAt without creating output.
func ReadAt[T any](ctx context.Context, src any, fn func(ra io.ReaderAt, size int64) (T, error)) (T, error) {
	if fn == nil {
		var zero T
		return zero, ErrNilFunc
	}
	_, res, err := Do(ctx, func(s *Scope) (T, error) {
		ra, size := s.OpenReaderAt(src)
		if ra == nil {
			var zero T
			return zero, errors.New("fio: cannot get ReaderAt")
		}
		return fn(ra, size)
	})
	return res, err
}

/* -------------------------------------------------------------------------- */
/*                                   Size                                     */
/* -------------------------------------------------------------------------- */

func SizeAny(x any) int64 {
	if x == nil {
		return -1
	}

	if fh, ok := x.(*multipart.FileHeader); ok && fh.Size >= 0 {
		return fh.Size
	}

	if sr, ok := x.(interface{ Size() int64 }); ok {
		if n := sr.Size(); n >= 0 {
			return n
		}
	}

	if lr, ok := x.(interface{ Len() int }); ok {
		return int64(lr.Len())
	}

	if f, ok := x.(*os.File); ok {
		if fi, err := f.Stat(); err == nil {
			return fi.Size()
		}
	}

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
		_, _ = seeker.Seek(cur, io.SeekStart)
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
/*                             Context Helpers                                 */
/* -------------------------------------------------------------------------- */

type ctxKey struct{}

var sessionKey = ctxKey{}

func WithSession(ctx context.Context, ses IoSession) context.Context {
	return context.WithValue(ctx, sessionKey, ses)
}

func Session(ctx context.Context) IoSession {
	ses, _ := ctx.Value(sessionKey).(IoSession)
	return ses
}

/* -------------------------------------------------------------------------- */
/*                                 IoSession                                   */
/* -------------------------------------------------------------------------- */

type IoSession interface {
	Cleanup() error
}

type ioSession struct {
	mu            sync.Mutex
	manager       IoManager
	dir           string
	closed        bool
	outputs       []*Output
	cleanupFns    []func() error
	storageType   StorageType
	autoThreshold int64
}

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

func (s *ioSession) newOutputWithStorage(ext string, storageType StorageType) (*Output, error) {
	if err := s.ensureOpen(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if storageType == Memory {
		out := &Output{ses: s, storageType: Memory}
		s.outputs = append(s.outputs, out)
		return out, nil
	}

	if s.dir == "" {
		return nil, ErrFileStorageUnavailable
	}

	pattern := "*"
	if ext != "" {
		pattern += ext
	}

	f, err := os.CreateTemp(s.dir, pattern)
	if err != nil {
		return nil, err
	}
	_ = f.Close()

	out := &Output{path: f.Name(), ses: s, storageType: File}
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
		skip := out.keep && !out.closed && out.storageType == File
		out.mu.Unlock()
		if skip {
			continue
		}
		errs = errors.Join(errs, out.cleanup())
	}

	for _, fn := range s.cleanupFns {
		if fn != nil {
			errs = errors.Join(errs, fn())
		}
	}
	s.cleanupFns = nil

	if s.dir == "" {
		return errs
	}

	entries, err := os.ReadDir(s.dir)
	if err != nil && !os.IsNotExist(err) {
		return errors.Join(errs, err)
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

/* -------------------------------------------------------------------------- */
/*                            Manager Options                                  */
/* -------------------------------------------------------------------------- */

type ManagerOption interface {
	applyManager(*managerConfig)
}

type ManagerOptionFunc func(*managerConfig)

func (f ManagerOptionFunc) applyManager(c *managerConfig) { f(c) }

type managerConfig struct {
	autoThreshold int64
}

type thresholdOption int64

func (t thresholdOption) applyManager(c *managerConfig) { c.autoThreshold = int64(t) }
func (t thresholdOption) applyOut(o *OutConfig) {
	val := int64(t)
	o.autoThreshold = &val
}

func WithThreshold(bytes int64) thresholdOption { return thresholdOption(bytes) }

/* -------------------------------------------------------------------------- */
/*                                  Manager                                    */
/* -------------------------------------------------------------------------- */

type IoManager interface {
	NewSession() (IoSession, error)
	Cleanup() error
}

type manager struct {
	mu            sync.Mutex
	baseDir       string
	closed        bool
	storageType   StorageType
	autoThreshold int64
}

func NewIoManager(baseDir string, storageType StorageType, opts ...ManagerOption) (IoManager, error) {
	config := &managerConfig{}
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
		return &manager{baseDir: dir, storageType: storageType, autoThreshold: config.autoThreshold}, nil
	}

	baseDir = filepath.Clean(baseDir)
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, err
	}

	return &manager{baseDir: baseDir, storageType: storageType, autoThreshold: config.autoThreshold}, nil
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

type OutConfig struct {
	ext           string
	storageType   *StorageType
	autoThreshold *int64
}

type OutOption interface {
	applyOut(*OutConfig)
}

type OutOptionFunc func(*OutConfig)

func (f OutOptionFunc) applyOut(o *OutConfig) { f(o) }
func (st StorageType) applyOut(o *OutConfig)  { o.storageType = &st }

func Out(ext string, opts ...OutOption) OutConfig {
	o := OutConfig{ext: ext}
	for _, opt := range opts {
		if opt != nil {
			opt.applyOut(&o)
		}
	}
	return o
}

func (o OutConfig) Ext() string                  { return o.ext }
func (o OutConfig) StorageTypeVal() *StorageType { return o.storageType }
func (o OutConfig) AutoThreshold() *int64        { return o.autoThreshold }

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
/*                                  Helpers                                    */
/* -------------------------------------------------------------------------- */

type errReader struct{ err error }

func (e errReader) Read(p []byte) (int, error) { return 0, e.err }

type errWriter struct{ err error }

func (e errWriter) Write(p []byte) (int, error) { return 0, e.err }

func copyToFile(src io.Reader, dstPath string) error {
	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		return err
	}
	f, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, src)
	return err
}

// JoinCleanup combines multiple cleanup functions.
func JoinCleanup(fns ...func() error) func() error {
	return func() error {
		var errs error
		for _, fn := range fns {
			if fn != nil {
				errs = errors.Join(errs, fn())
			}
		}
		return errs
	}
}

// SafeClose closes c ignoring errors.
func SafeClose(c io.Closer) {
	if c != nil {
		_ = c.Close()
	}
}
