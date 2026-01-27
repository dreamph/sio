// Package fio provides streaming I/O utilities with session management,
// automatic resource cleanup, and flexible storage backends (memory/file).
package fio

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
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
	defaultMaxPreallocate = 1 << 20  // 1MB
	defaultSpillThreshold = 64 << 20 // 64MB
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
func ptrInt64(v int64) *int64    { return &v }
func ptrBool(v bool) *bool       { return &v }

// Void replaces struct{} when you want “no value”.
type Void struct{}

/* -------------------------------------------------------------------------- */
/*                              HTTP Client                                   */
/* -------------------------------------------------------------------------- */

var httpClient = &http.Client{Timeout: 30 * time.Second}

type Config struct{ client *http.Client }

func NewConfig(client *http.Client) Config { return Config{client: client} }

func (c Config) WithClient(client *http.Client) Config {
	c.client = client
	return c
}

// Configure applies global configuration (call at app startup only).
func Configure(config Config) error {
	if config.client != nil {
		httpClient = config.client
	}
	return nil
}

/* -------------------------------------------------------------------------- */
/*                                   Errors                                   */
/* -------------------------------------------------------------------------- */

var (
	ErrNilSource              = errors.New("fio: nil source")
	ErrIoManagerClosed        = errors.New("fio: manager is closed")
	ErrIoSessionClosed        = errors.New("fio: session is closed")
	ErrDownloadFailed         = errors.New("fio: download failed")
	ErrNoSession              = errors.New("fio: session is nil")
	ErrFileStorageUnavailable = errors.New("fio: file storage requires directory")
	ErrInvalidSessionType     = errors.New("fio: invalid session type")
	ErrNilFunc                = errors.New("fio: fn is nil")
	ErrEmptyPath              = errors.New("fio: empty path")
	ErrEmptyURL               = errors.New("fio: empty url")
	ErrOutputCleaned          = errors.New("fio: output is cleaned up")
	ErrNilOutHandle           = errors.New("fio: nil OutHandle")
	ErrNilOutScope            = errors.New("fio: nil out-scope")
	ErrNewOutMultiple         = errors.New("fio: NewOut called more than once")
	ErrOutReuseRequiresPtr    = errors.New("fio: OutReuse requires out pointer")
	ErrCannotGetReaderAt      = errors.New("fio: cannot get ReaderAt")
	ErrInputNotReusable       = errors.New("fio: input is not reusable")
	ErrCannotResetInput       = errors.New("fio: cannot reset input")
	ErrToReaderAtNilReader    = errors.New("fio: ToReaderAt: nil reader")
	ErrNilInput               = errors.New("fio: nil input")
)

/* -------------------------------------------------------------------------- */
/*                               Storage Types                                */
/* -------------------------------------------------------------------------- */

type StorageType int

const (
	File StorageType = iota
	Memory
)

func (s StorageType) String() string {
	if s == Memory {
		return "memory"
	}
	return "file"
}

/* -------------------------------------------------------------------------- */
/*                             Type-safe Source                               */
/* -------------------------------------------------------------------------- */

// Source is a type-safe input source.
type Source interface {
	open(ctx context.Context) (rc io.ReadCloser, cleanup func() error, size int64, kind, path string, err error)
}

// Constructors (type safe)
func PathSource(p string) Source               { return pathSource(p) }
func URLSource(u string) Source                { return urlSource(u) }
func BytesSource(b []byte) Source              { return bytesSource(b) }
func ReaderSource(r io.Reader) Source          { return readerSource{r: r} }
func ReadCloserSource(rc io.ReadCloser) Source { return readCloserSource{rc: rc} }
func FileSource(f *os.File) Source             { return fileSource{f: f} }
func MultipartSource(fh *multipart.FileHeader) Source {
	return multipartSource{fh: fh}
}
func OutputSource(o *Output) Source { return outputSource{o: o} }
func InputSource(in *Input) Source  { return inputSource{in: in} } // for passing reusable Input around

type pathSource string

func (p pathSource) open(ctx context.Context) (io.ReadCloser, func() error, int64, string, string, error) {
	path := strings.TrimSpace(string(p))
	if path == "" {
		return nil, nil, -1, "", "", ErrEmptyPath
	}
	rc, cleanup, size, err := openFileDirect(path)
	if err != nil {
		return nil, nil, -1, "", "", err
	}
	return rc, cleanup, size, KindFile, path, nil
}

type urlSource string

func (u urlSource) open(ctx context.Context) (io.ReadCloser, func() error, int64, string, string, error) {
	urlStr := strings.TrimSpace(string(u))
	if urlStr == "" {
		return nil, nil, -1, "", "", ErrEmptyURL
	}
	rc, cleanup, size, err := openURLDirect(ctx, urlStr)
	if err != nil {
		return nil, nil, -1, "", "", err
	}
	return rc, cleanup, size, KindURL, urlStr, nil
}

type bytesSource []byte

func (b bytesSource) open(ctx context.Context) (io.ReadCloser, func() error, int64, string, string, error) {
	if b == nil {
		return nil, nil, -1, "", "", ErrNilSource
	}
	return io.NopCloser(bytes.NewReader(b)), nil, int64(len(b)), KindMemory, "", nil
}

type readerSource struct{ r io.Reader }

func (s readerSource) open(ctx context.Context) (io.ReadCloser, func() error, int64, string, string, error) {
	if s.r == nil {
		return nil, nil, -1, "", "", ErrNilSource
	}
	return io.NopCloser(s.r), nil, SizeAny(s.r), KindReader, "", nil
}

type readCloserSource struct{ rc io.ReadCloser }

func (s readCloserSource) open(ctx context.Context) (io.ReadCloser, func() error, int64, string, string, error) {
	if s.rc == nil {
		return nil, nil, -1, "", "", ErrNilSource
	}
	return s.rc, s.rc.Close, SizeAny(s.rc), KindReader, "", nil
}

type fileSource struct{ f *os.File }

func (s fileSource) open(ctx context.Context) (io.ReadCloser, func() error, int64, string, string, error) {
	if s.f == nil {
		return nil, nil, -1, "", "", ErrNilSource
	}
	return s.f, s.f.Close, fileSize(s.f), KindFile, s.f.Name(), nil
}

type multipartSource struct{ fh *multipart.FileHeader }

func (s multipartSource) open(ctx context.Context) (io.ReadCloser, func() error, int64, string, string, error) {
	if s.fh == nil {
		return nil, nil, -1, "", "", ErrNilSource
	}
	rc, err := s.fh.Open()
	if err != nil {
		return nil, nil, -1, "", "", err
	}
	return rc, rc.Close, s.fh.Size, KindMultipart, "", nil
}

type outputSource struct{ o *Output }

func (s outputSource) open(ctx context.Context) (io.ReadCloser, func() error, int64, string, string, error) {
	if s.o == nil {
		return nil, nil, -1, "", "", ErrNilSource
	}
	rc, err := s.o.OpenReader()
	if err != nil {
		return nil, nil, -1, "", "", err
	}
	return rc, rc.Close, s.o.Size(), KindStream, s.o.Path(), nil
}

type inputSource struct{ in *Input }

func (s inputSource) open(ctx context.Context) (io.ReadCloser, func() error, int64, string, string, error) {
	if s.in == nil {
		return nil, nil, -1, "", "", ErrNilSource
	}
	return s.in.Reader, s.in.Close, s.in.Size, s.in.Kind, s.in.Path, nil
}

/* -------------------------------------------------------------------------- */
/*                                   Input                                    */
/* -------------------------------------------------------------------------- */

// Input represents an opened input source with metadata.
// Supports reusable reset (optional).
type Input struct {
	Reader  io.ReadCloser
	Size    int64
	Kind    string
	Path    string
	cleanup func() error

	// Reusable support
	reusable   bool
	needsReset bool
	readerAt   io.ReaderAt // reusable file
	data       []byte      // reusable memory
}

func (in *Input) Close() error {
	if in == nil {
		return nil
	}
	var errs error
	if in.Reader != nil {
		errs = errors.Join(errs, in.Reader.Close())
		in.Reader = nil
	}
	if in.cleanup != nil {
		errs = errors.Join(errs, in.cleanup())
		in.cleanup = nil
	}
	in.readerAt = nil
	in.data = nil
	in.needsReset = false
	return errs
}

func (in *Input) IsReusable() bool { return in != nil && in.reusable }

func (in *Input) markUsed() {
	if in != nil && in.reusable {
		in.needsReset = true
	}
}

func (in *Input) ReaderAt() io.ReaderAt {
	if in == nil {
		return nil
	}
	if in.readerAt != nil {
		return in.readerAt
	}
	if in.data != nil {
		return bytes.NewReader(in.data)
	}
	if ra, ok := in.Reader.(io.ReaderAt); ok {
		return ra
	}
	return nil
}

func (in *Input) Reset() error {
	if in == nil {
		return ErrNilInput
	}
	if !in.reusable {
		return ErrInputNotReusable
	}
	if !in.needsReset {
		return nil
	}

	if in.data != nil {
		in.Reader = io.NopCloser(bytes.NewReader(in.data))
		in.needsReset = false
		return nil
	}

	if in.readerAt != nil {
		in.Reader = &readerAtCloser{r: io.NewSectionReader(in.readerAt, 0, in.Size)}
		in.needsReset = false
		return nil
	}

	if seeker, ok := in.Reader.(io.Seeker); ok {
		_, err := seeker.Seek(0, io.SeekStart)
		if err == nil {
			in.needsReset = false
		}
		return err
	}

	return ErrCannotResetInput
}

type readerAtCloser struct{ r *io.SectionReader }

func (r *readerAtCloser) Read(p []byte) (int, error) { return r.r.Read(p) }
func (r *readerAtCloser) Close() error               { return nil }

/* --------------------------- Input options -------------------------------- */

type InOption func(*inConfig)
type inConfig struct {
	reusable       bool
	deleteAfterUse bool
}

func Reusable() InOption { return func(c *inConfig) { c.reusable = true } }

// DeleteAfterUse deletes local temp files for file/stream sources after read.
func DeleteAfterUse() InOption { return func(c *inConfig) { c.deleteAfterUse = true } }

// In keeps legacy call sites; DeleteAfterUse is best-effort in fio.
func In(src Source, opts ...InOption) Source {
	if src == nil {
		return src
	}
	cfg := &inConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(cfg)
		}
	}
	if !cfg.deleteAfterUse {
		return src
	}
	return deleteAfterUseSource{src: src}
}

type deleteAfterUseSource struct{ src Source }

func (d deleteAfterUseSource) open(ctx context.Context) (io.ReadCloser, func() error, int64, string, string, error) {
	if d.src == nil {
		return nil, nil, -1, "", "", ErrNilSource
	}
	rc, cleanup, size, kind, path, err := d.src.open(ctx)
	if err != nil {
		return nil, nil, -1, "", "", err
	}

	cleanupFn := func() error {
		var err error
		if cleanup != nil {
			err = errors.Join(err, cleanup())
		} else if rc != nil {
			err = errors.Join(err, rc.Close())
		}

		if path != "" && (kind == KindFile || kind == KindStream) {
			if osrc, ok := d.src.(outputSource); ok && osrc.o != nil && osrc.o.keep {
				return err
			}
			if rmErr := os.Remove(path); rmErr != nil && !errors.Is(rmErr, os.ErrNotExist) {
				err = errors.Join(err, rmErr)
			}
		}
		return err
	}

	return rc, cleanupFn, size, kind, path, nil
}

// OpenIn opens a type-safe Source and returns an Input.
// If Reusable() is set, it will buffer (non-file) into memory or use ReaderAt for files.
func OpenIn(ctx context.Context, src Source, opts ...InOption) (*Input, error) {
	if src == nil {
		return nil, ErrNilSource
	}

	cfg := &inConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(cfg)
		}
	}

	if is, ok := src.(inputSource); ok && is.in != nil {
		return is.in, nil
	}

	rc, cleanup, size, kind, path, err := src.open(ctx)
	if err != nil {
		return nil, err
	}

	in := &Input{Reader: rc, Size: size, Kind: kind, Path: path, cleanup: cleanup}

	if cfg.reusable {
		return makeReusable(in)
	}
	return in, nil
}

func makeReusable(in *Input) (*Input, error) {
	if in == nil {
		return nil, ErrNilSource
	}
	if in.reusable {
		return in, nil
	}

	if f, ok := in.Reader.(*os.File); ok {
		return makeReusableFile(in, f)
	}

	data, err := io.ReadAll(in.Reader)
	if err != nil {
		_ = in.Close()
		return nil, err
	}
	_ = in.Reader.Close()

	in.Reader = io.NopCloser(bytes.NewReader(data))
	in.Size = int64(len(data))
	in.data = data
	in.reusable = true
	in.needsReset = false
	return in, nil
}

func makeReusableFile(in *Input, f *os.File) (*Input, error) {
	size := fileSize(f)
	if size < 0 {
		return makeReusable(in)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return makeReusable(in)
	}

	in.readerAt = f
	in.Size = size
	in.reusable = true
	in.needsReset = false
	in.Reader = &readerAtCloser{r: io.NewSectionReader(f, 0, size)}
	in.cleanup = f.Close
	return in, nil
}

/* -------------------------------------------------------------------------- */
/*                                   Output                                   */
/* -------------------------------------------------------------------------- */

type Output struct {
	mu                  sync.Mutex
	path                string
	session             IoSession
	closed              bool
	keep                bool
	data                []byte
	storageType         StorageType
	maxPreallocateBytes int64
	cleanupFunc         func() error
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
		return nil, ErrOutputCleaned
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
		return nil, ErrOutputCleaned
	}

	if o.storageType == Memory {
		var buf *bytes.Buffer
		var preallocateData []byte
		sizeHintVal := int64(0)
		if len(sizeHint) > 0 && sizeHint[0] > 0 {
			sizeHintVal = sizeHint[0]
			// Pre-allocate with capped capacity to avoid large spikes
			capHint := sizeHint[0]
			maxCap := o.maxPreallocateBytes
			if maxCap > 0 && capHint > maxCap {
				capHint = maxCap
			}
			maxInt := int64(int(^uint(0) >> 1))
			if capHint > maxInt {
				if maxCap > 0 && maxCap < maxInt {
					capHint = maxCap
				} else {
					capHint = maxInt
				}
			}
			preallocateData = make([]byte, 0, int(capHint))
			buf = bytes.NewBuffer(preallocateData)
		} else {
			buf = &bytes.Buffer{}
		}
		return &bytesWriteCloser{
			buf:             buf,
			output:          o,
			preallocateData: preallocateData,
			sizeHint:        sizeHintVal,
		}, nil
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

func (o *Output) cleanup() error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return nil
	}
	o.closed = true

	if o.storageType == Memory {
		if o.cleanupFunc != nil {
			_ = o.cleanupFunc()
			o.cleanupFunc = nil
		}
		o.data = nil
		return nil
	}

	err := os.Remove(o.path)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

/* -------------------------------------------------------------------------- */
/*                                 OutHandle                                  */
/* -------------------------------------------------------------------------- */

type OutHandle struct {
	Writer    io.WriteCloser
	output    *Output
	session   *ioSession
	finalized bool
}

func (h *OutHandle) Finalize() (*Output, error) {
	if h == nil {
		return nil, ErrNilOutHandle
	}
	if h.finalized {
		return h.output, nil
	}
	h.finalized = true

	if h.Writer != nil {
		if err := h.Writer.Close(); err != nil {
			if h.output != nil {
				_ = h.output.cleanup()
			}
			return nil, err
		}
	}

	return h.output, nil
}

func (h *OutHandle) Cleanup() error {
	if h == nil || h.finalized {
		return nil
	}
	h.finalized = true

	var errs error
	if h.Writer != nil {
		errs = errors.Join(errs, h.Writer.Close())
	}
	if h.output != nil {
		errs = errors.Join(errs, h.output.cleanup())
	}
	return errs
}

/* -------------------------------------------------------------------------- */
/*                                IoSession                                   */
/* -------------------------------------------------------------------------- */

type IoSession interface {
	NewOut(out OutConfig, sizeHint ...int64) (*Output, error)
	Cleanup() error
}

type ioSession struct {
	mu                  sync.Mutex
	manager             IoManager
	dir                 string
	closed              atomic.Bool
	outputs             []*Output
	cleanupFns          []func() error
	storageType         StorageType
	autoFileThreshold   int64
	spillThreshold      int64
	maxPreallocateBytes int64
	useMmap             bool
}

func resolveStorageType(out OutConfig, ses *ioSession, sizeHint int64) StorageType {
	storageType := ses.storageType
	if out.storageType != nil {
		storageType = *out.storageType
	} else if out.autoFileThreshold != nil && *out.autoFileThreshold > 0 && sizeHint >= *out.autoFileThreshold {
		storageType = File
	} else if sizeHint >= 0 && ses.autoFileThreshold > 0 && sizeHint >= ses.autoFileThreshold {
		storageType = File
	}

	spill := ses.spillThreshold
	if out.spillThreshold != nil {
		spill = *out.spillThreshold
	}
	if sizeHint >= 0 && storageType == Memory && spill > 0 && sizeHint >= spill {
		storageType = File
	}

	return storageType
}

func resolveMaxPreallocate(out OutConfig, ses *ioSession) int64 {
	maxValue := ses.maxPreallocateBytes
	if out.maxPreallocateBytes != nil {
		maxValue = *out.maxPreallocateBytes
	}
	return maxValue
}

func (s *ioSession) Dir() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dir
}

func (s *ioSession) ensureOpen() error {
	if s.closed.Load() {
		return ErrIoSessionClosed
	}
	return nil
}

func (s *ioSession) newOutput(ext string, storageType StorageType) (*Output, error) {
	out, _, err := s.newOutputWithFile(ext, storageType)
	return out, err
}

// newOutputWithFile creates an output and returns the open file handle for File storage.
// The caller is responsible for closing the file. For Memory storage, file is nil.
func (s *ioSession) newOutputWithFile(ext string, storageType StorageType) (*Output, *os.File, error) {
	// Fast check without lock
	if s.closed.Load() {
		return nil, nil, ErrIoSessionClosed
	}

	if storageType == Memory {
		out := &Output{
			session:             s,
			storageType:         Memory,
			maxPreallocateBytes: s.maxPreallocateBytes,
		}
		s.mu.Lock()
		if s.closed.Load() {
			s.mu.Unlock()
			return nil, nil, ErrIoSessionClosed
		}
		s.outputs = append(s.outputs, out)
		s.mu.Unlock()
		return out, nil, nil
	}

	if s.dir == "" {
		return nil, nil, ErrFileStorageUnavailable
	}

	pattern := "*"
	if ext != "" {
		pattern += ext
	}

	// CreateTemp outside of lock to reduce lock contention
	f, err := os.CreateTemp(s.dir, pattern)
	if err != nil {
		return nil, nil, err
	}

	out := &Output{
		path:                f.Name(),
		session:             s,
		storageType:         File,
		maxPreallocateBytes: s.maxPreallocateBytes,
	}

	s.mu.Lock()
	if s.closed.Load() {
		s.mu.Unlock()
		_ = f.Close()
		_ = os.Remove(f.Name())
		return nil, nil, ErrIoSessionClosed
	}
	s.outputs = append(s.outputs, out)
	s.mu.Unlock()

	return out, f, nil
}

// NewOut creates an output for manual writing within this session.
// Use Output.OpenWriter() to write and Output.OpenReader() to read later.
func (s *ioSession) NewOut(out OutConfig, sizeHint ...int64) (*Output, error) {
	if s == nil {
		return nil, ErrNoSession
	}
	if err := s.ensureOpen(); err != nil {
		return nil, err
	}

	var hint int64 = -1
	if len(sizeHint) > 0 {
		hint = sizeHint[0]
	}

	storageType := resolveStorageType(out, s, hint)

	output, err := s.newOutput(out.ext, storageType)
	if err != nil {
		return nil, err
	}
	output.maxPreallocateBytes = resolveMaxPreallocate(out, s)

	return output, nil
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
	// Use atomic swap to ensure only one cleanup runs
	if s.closed.Swap(true) {
		return nil
	}

	var errs error

	// 1. Identify files to keep and cleanup memory/non-kept outputs
	keepMap := make(map[string]struct{})
	for _, out := range s.outputs {
		var (
			keep        bool
			closed      bool
			storageType StorageType
			path        string
		)
		out.mu.Lock()
		keep = out.keep
		closed = out.closed
		storageType = out.storageType
		path = out.path
		out.mu.Unlock()
		if storageType == File && keep && !closed {
			keepMap[filepath.Clean(path)] = struct{}{}
			continue
		}
		errs = errors.Join(errs, out.cleanup())
	}

	// 2. Run other cleanup functions
	for _, fn := range s.cleanupFns {
		if fn != nil {
			errs = errors.Join(errs, fn())
		}
	}
	s.cleanupFns = nil

	if s.dir == "" {
		return errs
	}

	// --- OPTIMIZATION POINT ---
	// 3. Check if we can do a "Bulk Delete"
	if len(keepMap) == 0 {
		// If nothing to keep, nuke the entire directory in one go.
		// This is significantly faster than reading directory entries.
		if err := os.RemoveAll(s.dir); err != nil && !os.IsNotExist(err) {
			errs = errors.Join(errs, err)
		}
		return errs
	}

	// 4. Selective Delete (Only if there are files to keep)
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return errs
		}
		return errors.Join(errs, err)
	}

	for _, e := range entries {
		fullPath := filepath.Clean(filepath.Join(s.dir, e.Name()))

		// Check map in O(1) time
		if _, shouldKeep := keepMap[fullPath]; !shouldKeep {
			if err := os.RemoveAll(fullPath); err != nil && !os.IsNotExist(err) {
				errs = errors.Join(errs, err)
			}
		}
	}

	// Attempt to remove the session dir.
	// It will only succeed if all entries inside were deleted.
	_ = os.Remove(s.dir)

	return errs
}

/* -------------------------------------------------------------------------- */
/*                                 IoManager                                  */
/* -------------------------------------------------------------------------- */

type IoManager interface {
	NewSession() (IoSession, error)
	Cleanup() error
}

type ManagerOption interface {
	applyManager(*managerConfig)
}

type ManagerOptionFunc func(*managerConfig)

func (f ManagerOptionFunc) applyManager(c *managerConfig) { f(c) }

type managerConfig struct {
	autoFileThreshold   int64
	spillThreshold      *int64
	maxPreallocateBytes *int64
	useMmap             *bool
}

type thresholdOption int64

func (t thresholdOption) applyManager(c *managerConfig) { c.autoFileThreshold = int64(t) }

// WithThreshold sets session auto file threshold (bytes). 0 = disabled.
func WithThreshold(bytes int64) thresholdOption { return thresholdOption(bytes) }

type spillThresholdOption int64

func (t spillThresholdOption) applyManager(c *managerConfig) { c.spillThreshold = ptrInt64(int64(t)) }
func (t spillThresholdOption) applyOut(o *OutConfig)         { o.spillThreshold = ptrInt64(int64(t)) }

// WithSpillThreshold forces Memory to spill to File when sizeHint >= bytes.
// Set to 0 to disable spill-to-file behavior.
func WithSpillThreshold(bytes int64) spillThresholdOption { return spillThresholdOption(bytes) }

type maxPreallocateOption int64

func (t maxPreallocateOption) applyManager(c *managerConfig) {
	c.maxPreallocateBytes = ptrInt64(int64(t))
}
func (t maxPreallocateOption) applyOut(o *OutConfig) { o.maxPreallocateBytes = ptrInt64(int64(t)) }

// WithMaxPreallocate caps memory pre-allocation when sizeHint is provided.
// Set to 0 to disable the cap.
func WithMaxPreallocate(bytes int64) maxPreallocateOption { return maxPreallocateOption(bytes) }

type mmapOption bool

func (t mmapOption) applyManager(c *managerConfig) { c.useMmap = ptrBool(bool(t)) }

// WithMmap enables or disables mmap for file-to-memory fast paths.
func WithMmap(enabled bool) mmapOption { return mmapOption(enabled) }

type manager struct {
	mu                  sync.Mutex
	baseDir             string
	closed              bool
	storageType         StorageType
	autoFileThreshold   int64
	spillThreshold      int64
	maxPreallocateBytes int64
	useMmap             bool
}

func NewIoManager(baseDir string, storageType StorageType, opts ...ManagerOption) (IoManager, error) {
	config := &managerConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt.applyManager(config)
		}
	}
	spill := int64(defaultSpillThreshold)
	if config.spillThreshold != nil {
		spill = *config.spillThreshold
	}
	maxPreallocate := int64(defaultMaxPreallocate)
	if config.maxPreallocateBytes != nil {
		maxPreallocate = *config.maxPreallocateBytes
	}
	useMmap := false
	if config.useMmap != nil {
		useMmap = *config.useMmap
	}

	if strings.TrimSpace(baseDir) == "" {
		dir, err := os.MkdirTemp("", "fio-")
		if err != nil {
			return nil, err
		}
		return &manager{
			baseDir:             dir,
			storageType:         storageType,
			autoFileThreshold:   config.autoFileThreshold,
			spillThreshold:      spill,
			maxPreallocateBytes: maxPreallocate,
			useMmap:             useMmap,
		}, nil
	}

	baseDir = filepath.Clean(baseDir)
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, err
	}

	return &manager{
		baseDir:             baseDir,
		storageType:         storageType,
		autoFileThreshold:   config.autoFileThreshold,
		spillThreshold:      spill,
		maxPreallocateBytes: maxPreallocate,
		useMmap:             useMmap,
	}, nil
}

func (m *manager) NewSession() (IoSession, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil, ErrIoManagerClosed
	}

	dir, err := os.MkdirTemp(m.baseDir, "fio-")
	if err != nil {
		return nil, err
	}

	return &ioSession{
		manager:             m,
		dir:                 dir,
		storageType:         m.storageType,
		autoFileThreshold:   m.autoFileThreshold,
		spillThreshold:      m.spillThreshold,
		maxPreallocateBytes: m.maxPreallocateBytes,
		useMmap:             m.useMmap,
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
/*                             Context Helpers                                */
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
/*                              Output Options                                */
/* -------------------------------------------------------------------------- */

type OutConfig struct {
	ext                 string
	storageType         *StorageType
	autoFileThreshold   *int64
	spillThreshold      *int64
	maxPreallocateBytes *int64
	reusePtr            **Output
	reuseCfg            outReuseConfig
	reuseEnabled        bool
}

type OutOption interface {
	applyOut(*OutConfig)
}

type OutOptionFunc func(*OutConfig)

func (f OutOptionFunc) applyOut(o *OutConfig) { f(o) }
func (st StorageType) applyOut(o *OutConfig)  { o.storageType = &st }

func WithStorage(st StorageType) OutOption { return st }

// OutReuse configures output reuse for OutScope.NewOut.
func OutReuse(outPtr **Output, opts ...OutReuseOpt) OutOption {
	return OutOptionFunc(func(o *OutConfig) {
		o.reusePtr = outPtr
		o.reuseEnabled = true
		cfg := outReuseConfig{cleanupOld: true, keepMemCap: true, maxMemCap: 0}
		for _, opt := range opts {
			if opt != nil {
				opt.applyOutReuse(&cfg)
			}
		}
		o.reuseCfg = cfg
	})
}

func Out(ext string, opts ...OutOption) OutConfig {
	o := OutConfig{ext: ext}
	for _, opt := range opts {
		if opt != nil {
			opt.applyOut(&o)
		}
	}
	return o
}

func WithOut(ext string, opts ...OutOption) OutConfig { return Out(ext, opts...) }

func (o OutConfig) Ext() string                  { return o.ext }
func (o OutConfig) StorageTypeVal() *StorageType { return o.storageType }
func (o OutConfig) AutoThreshold() *int64        { return o.autoFileThreshold }

/* -------------------------------------------------------------------------- */
/*                              bytesWriteCloser                              */
/* -------------------------------------------------------------------------- */

type bytesWriteCloser struct {
	buf             *bytes.Buffer
	output          *Output
	written         bool
	preallocateData []byte // backing slice for pre-allocated buffer
	sizeHint        int64
}

func (b *bytesWriteCloser) Write(p []byte) (int, error) {
	// Fast path: single large write (from bytes.Reader.WriteTo)
	// When io.Copy calls WriteTo, it writes everything at once
	if !b.written && b.buf.Len() == 0 && len(p) >= 64<<10 {
		b.written = true
		var data []byte
		// Reuse pre-allocated buffer if available and fits
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

func (b *bytesWriteCloser) ReadFrom(r io.Reader) (int64, error) {
	if b.output == nil {
		return 0, nil
	}

	b.written = true
	if b.preallocateData != nil && b.sizeHint > 0 && int64(cap(b.preallocateData)) >= b.sizeHint {
		data := b.preallocateData[:b.sizeHint]
		n, err := io.ReadFull(r, data)
		if err == io.ErrUnexpectedEOF || err == io.EOF {
			err = nil
		}
		b.output.mu.Lock()
		b.output.data = data[:n]
		b.output.mu.Unlock()
		return int64(n), err
	}

	n, err := b.buf.ReadFrom(r)
	if b.output != nil && b.output.data == nil {
		b.output.mu.Lock()
		b.output.data = b.buf.Bytes()
		b.output.mu.Unlock()
	}
	return n, err
}

func (b *bytesWriteCloser) Close() error {
	if b.output != nil && b.output.data == nil {
		b.output.mu.Lock()
		b.output.data = b.buf.Bytes()
		b.output.mu.Unlock()
	}
	return nil
}

/* -------------------------------------------------------------------------- */
/*                              NewOut (standard)                              */
/* -------------------------------------------------------------------------- */

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

	storageType := resolveStorageType(out, iSes, hint)

	output, err := iSes.newOutput(out.ext, storageType)
	if err != nil {
		return nil, err
	}
	output.maxPreallocateBytes = resolveMaxPreallocate(out, iSes)

	w, err := output.OpenWriter(hint)
	if err != nil {
		_ = output.cleanup()
		return nil, err
	}

	return &OutHandle{Writer: w, output: output, session: iSes}, nil
}

/* -------------------------------------------------------------------------- */
/*                            Scope (Use / type-safe)                          */
/*              IMPORTANT: Scope has NO NewOut (Do-safe)                       */
/* -------------------------------------------------------------------------- */

type Scope struct {
	ctx      context.Context
	cleanups []func() error
}

// Use opens a type-safe Source and returns reader. Cleanup is automatic.
func (s *Scope) Use(src Source) (io.Reader, error) {
	if src == nil {
		err := ErrNilSource
		return nil, err
	}

	if b, ok := src.(bytesSource); ok {
		if b == nil {
			err := ErrNilSource
			return nil, err
		}
		return bytes.NewReader(b), nil
	}

	// reusable Input fast-path
	if is, ok := src.(inputSource); ok && is.in != nil && is.in.IsReusable() {
		if err := is.in.Reset(); err != nil {
			return nil, err
		}
		is.in.markUsed()
		return is.in.Reader, nil
	}

	rc, cleanup, _, _, _, err := src.open(s.ctx)
	if err != nil {
		return nil, err
	}
	if cleanup != nil {
		s.cleanups = append(s.cleanups, cleanup)
	} else {
		// safety: ensure rc closed
		s.cleanups = append(s.cleanups, rc.Close)
	}
	return rc, nil
}

func (s *Scope) UseSized(src Source) (io.Reader, int64, error) {
	if src == nil {
		err := ErrNilSource
		return nil, -1, err
	}

	if b, ok := src.(bytesSource); ok {
		if b == nil {
			err := ErrNilSource
			return nil, -1, err
		}
		return bytes.NewReader(b), int64(len(b)), nil
	}

	if is, ok := src.(inputSource); ok && is.in != nil && is.in.IsReusable() {
		if err := is.in.Reset(); err != nil {
			return nil, -1, err
		}
		is.in.markUsed()
		return is.in.Reader, is.in.Size, nil
	}

	rc, cleanup, size, _, _, err := src.open(s.ctx)
	if err != nil {
		return nil, -1, err
	}
	if cleanup != nil {
		s.cleanups = append(s.cleanups, cleanup)
	} else {
		s.cleanups = append(s.cleanups, rc.Close)
	}
	return rc, size, nil
}

// UseReaderAt returns ReaderAt + size with options.
// Buffers into memory or spills to temp file based on options.
func (s *Scope) UseReaderAt(src Source, opts ...ToReaderAtOption) (io.ReaderAt, int64, error) {
	if src == nil {
		err := ErrNilSource
		return nil, -1, err
	}

	// Input with ReaderAt support
	if is, ok := src.(inputSource); ok && is.in != nil {
		if ra := is.in.ReaderAt(); ra != nil {
			if !is.in.IsReusable() {
				s.cleanups = append(s.cleanups, is.in.Close)
			}
			return ra, is.in.Size, nil
		}
	}

	rc, cleanup, size, _, _, err := src.open(s.ctx)
	if err != nil {
		return nil, -1, err
	}

	if ra, ok := rc.(io.ReaderAt); ok {
		if cleanup != nil {
			s.cleanups = append(s.cleanups, cleanup)
		} else {
			s.cleanups = append(s.cleanups, rc.Close)
		}
		return ra, size, nil
	}

	res, err := ToReaderAt(s.ctx, rc, opts...)
	if cleanup != nil {
		_ = cleanup()
	} else {
		_ = rc.Close()
	}
	if err != nil {
		return nil, -1, err
	}
	if res != nil && res.cleanup != nil {
		s.cleanups = append(s.cleanups, res.cleanup)
	}
	if res == nil {
		return nil, -1, ErrCannotGetReaderAt
	}
	return res.ReaderAt(), res.Size(), nil
}

func (s *Scope) cleanup() {
	for i := len(s.cleanups) - 1; i >= 0; i-- {
		if s.cleanups[i] != nil {
			_ = s.cleanups[i]()
		}
	}
	s.cleanups = nil
}

func (s *Scope) finalize(fnErr error) error {
	s.cleanup()
	return fnErr
}

/* -------------------------------------------------------------------------- */
/*                             OutScope (Output-capable)                      */
/*        IMPORTANT: only OutScope has NewOut (DoOut-safe)                     */
/* -------------------------------------------------------------------------- */

type OutScope struct {
	Scope
	outHandle      *OutHandle
	outConfig      OutConfig
	outSizeHint    int64
	outSizeHintSet bool
}

func (s *OutScope) setOutSizeHint(size int64) {
	if size >= 0 && !s.outSizeHintSet {
		s.outSizeHint = size
		s.outSizeHintSet = true
	}
}

// UseSized opens a Source and records size for output decisions.
func (s *OutScope) UseSized(src Source) (io.Reader, int64, error) {
	r, size, err := s.Scope.UseSized(src)
	s.setOutSizeHint(size)
	return r, size, err
}

// UseReaderAt opens a Source as ReaderAt and records size for output decisions.
func (s *OutScope) UseReaderAt(src Source, opts ...ToReaderAtOption) (io.ReaderAt, int64, error) {
	ra, size, err := s.Scope.UseReaderAt(src, opts...)
	s.setOutSizeHint(size)
	return ra, size, err
}

type lazyOutWriter struct {
	scope  *OutScope
	writer io.Writer
}

func (w *lazyOutWriter) Write(p []byte) (int, error) {
	if w.scope == nil {
		return 0, ErrNilOutScope
	}
	if w.writer == nil {
		writer, err := w.scope.ensureOutWriter()
		if err != nil {
			return 0, err
		}
		w.writer = writer
	}
	return w.writer.Write(p)
}

func (w *lazyOutWriter) ReadFrom(r io.Reader) (int64, error) {
	if w.scope == nil {
		return 0, ErrNilOutScope
	}
	if w.writer == nil {
		writer, err := w.scope.ensureOutWriter()
		if err != nil {
			return 0, err
		}
		w.writer = writer
	}
	if rf, ok := w.writer.(io.ReaderFrom); ok {
		return rf.ReadFrom(r)
	}
	return io.Copy(w.writer, r)
}

func (s *OutScope) ensureOutWriter() (io.Writer, error) {
	if s.outHandle != nil {
		return s.outHandle.Writer, nil
	}
	hint := int64(-1)
	if s.outSizeHintSet {
		hint = s.outSizeHint
	}
	return s.NewOut(s.outConfig, hint)
}

// NewOut creates output writer (only available in OutScope).
func (s *OutScope) NewOut(out OutConfig, sizeHint ...int64) (io.Writer, error) {
	if s.outHandle != nil {
		return nil, ErrNewOutMultiple
	}

	if out.reuseEnabled {
		return s.newOutReuse(out)
	}

	oh, err := NewOut(s.ctx, out, sizeHint...)
	if err != nil {
		return nil, err
	}

	s.outHandle = oh
	return oh.Writer, nil
}

func (s *OutScope) finalizeOut(fnErr error) (*Output, error) {
	// cleanup inputs first
	s.cleanup()

	if fnErr != nil {
		if s.outHandle != nil {
			_ = s.outHandle.Cleanup()
		}
		return nil, fnErr
	}

	if s.outHandle == nil {
		return nil, nil
	}
	return s.outHandle.Finalize()
}

/* -------------------------------------------------------------------------- */
/*                             OutReuse Options                               */
/* -------------------------------------------------------------------------- */

type OutReuseOpt interface {
	applyOutReuse(*outReuseConfig)
}

type OutReuseOptFunc func(*outReuseConfig)

func (f OutReuseOptFunc) applyOutReuse(c *outReuseConfig) { f(c) }

type outReuseConfig struct {
	cleanupOld bool // default true
	keepMemCap bool // default true
	maxMemCap  int64
}

func WithCleanupOld(v bool) OutReuseOpt {
	return OutReuseOptFunc(func(c *outReuseConfig) { c.cleanupOld = v })
}
func WithKeepMemCap(v bool) OutReuseOpt {
	return OutReuseOptFunc(func(c *outReuseConfig) { c.keepMemCap = v })
}
func WithMaxMemCap(bytes int64) OutReuseOpt {
	return OutReuseOptFunc(func(c *outReuseConfig) { c.maxMemCap = bytes })
}

/* -------------------------------------------------------------------------- */
/*                       Reusable in-memory writer (bytes)                     */
/* -------------------------------------------------------------------------- */

type memWriteCloser struct {
	buf    *bytes.Buffer
	output *Output
	cfg    outReuseConfig
}

func (w *memWriteCloser) Write(p []byte) (int, error) { return w.buf.Write(p) }

func (w *memWriteCloser) Close() error {
	if w.output == nil {
		return nil
	}

	w.output.mu.Lock()
	w.output.data = w.buf.Bytes()
	w.output.mu.Unlock()

	// Optional cap/shrink
	if w.cfg.maxMemCap > 0 && int64(w.buf.Cap()) > w.cfg.maxMemCap {
		b := w.output.data
		limit := w.cfg.maxMemCap
		if int64(len(b)) < limit {
			limit = int64(len(b))
		}
		nb := make([]byte, 0, limit)
		nb = append(nb, b...)
		w.buf = bytes.NewBuffer(nb)
		w.output.mu.Lock()
		w.output.data = w.buf.Bytes()
		w.output.mu.Unlock()
	}

	return nil
}

func (s *OutScope) newOutReuse(cfg OutConfig) (io.Writer, error) {
	if s == nil {
		return nil, ErrNilOutScope
	}
	if s.outHandle != nil {
		return nil, ErrNewOutMultiple
	}
	if cfg.reusePtr == nil {
		return nil, ErrOutReuseRequiresPtr
	}
	outPtr := cfg.reusePtr
	reuseCfg := cfg.reuseCfg
	if !cfg.reuseEnabled {
		reuseCfg = outReuseConfig{cleanupOld: true, keepMemCap: true, maxMemCap: 0}
	}

	ses := Session(s.ctx)
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

	storageType := iSes.storageType
	if cfg.storageType != nil {
		storageType = *cfg.storageType
	}

	prev := *outPtr
	if prev != nil && reuseCfg.cleanupOld && prev.StorageType() != storageType {
		_ = prev.cleanup()
		prev = nil
		*outPtr = nil
	}

	var out *Output
	if prev != nil && prev.StorageType() == storageType {
		out = prev
		out.mu.Lock()
		out.closed = false
		out.keep = false
		if storageType == Memory && out.data != nil {
			out.data = out.data[:0]
		}
		out.mu.Unlock()
	} else {
		newOut, err := iSes.newOutput(cfg.ext, storageType)
		if err != nil {
			return nil, err
		}
		out = newOut
		*outPtr = out
	}
	out.mu.Lock()
	out.maxPreallocateBytes = resolveMaxPreallocate(cfg, iSes)
	out.mu.Unlock()

	switch storageType {
	case Memory:
		out.mu.Lock()
		old := out.data
		out.data = nil
		out.closed = false
		out.mu.Unlock()

		var buf *bytes.Buffer
		if reuseCfg.keepMemCap && cap(old) > 0 {
			buf = bytes.NewBuffer(old[:0])
		} else {
			buf = &bytes.Buffer{}
		}

		wc := &memWriteCloser{buf: buf, output: out, cfg: reuseCfg}
		s.outHandle = &OutHandle{Writer: wc, output: out, session: iSes}
		return wc, nil

	default: // File
		if prev != nil && prev.StorageType() == File && reuseCfg.cleanupOld {
			_ = prev.cleanup()
			newOut, err := iSes.newOutput(cfg.ext, File)
			if err != nil {
				return nil, err
			}
			out = newOut
			*outPtr = out
		}

		w, err := out.OpenWriter()
		if err != nil {
			_ = out.cleanup()
			return nil, err
		}

		s.outHandle = &OutHandle{Writer: w, output: out, session: iSes}
		return w, nil
	}
}

/* -------------------------------------------------------------------------- */
/*                                   Do API                                   */
/* -------------------------------------------------------------------------- */

// Do: returns *T only (NO output possible; Scope has no NewOut)
func Do[T any](ctx context.Context, fn func(s *Scope) (*T, error)) (*T, error) {
	if fn == nil {
		return nil, ErrNilFunc
	}

	s := &Scope{
		ctx: ctx,
		// cleanups: nil - lazy allocate only when needed
	}

	res, err := fn(s)
	finErr := s.finalize(err)
	if finErr != nil {
		return nil, finErr
	}
	return res, nil
}

// DoOut: returns *Output only (output-capable scope)
func DoOut(ctx context.Context, outCfg OutConfig, fn func(ctx context.Context, s *OutScope, w io.Writer) error) (*Output, error) {
	if fn == nil {
		return nil, ErrNilFunc
	}

	s := &OutScope{
		Scope: Scope{
			ctx: ctx,
			// cleanups: nil - lazy allocate only when needed
		},
		outConfig: outCfg,
	}

	w := &lazyOutWriter{scope: s}
	err := fn(ctx, s, w)
	out, finErr := s.finalizeOut(err)
	if finErr != nil {
		return nil, finErr
	}
	return out, nil
}

// DoOutResult: returns *Output + *T (output-capable scope)
func DoOutResult[T any](ctx context.Context, outCfg OutConfig, fn func(ctx context.Context, s *OutScope, w io.Writer) (*T, error)) (*Output, *T, error) {
	if fn == nil {
		return nil, nil, ErrNilFunc
	}

	s := &OutScope{
		Scope: Scope{
			ctx: ctx,
			// cleanups: nil - lazy allocate only when needed
		},
		outConfig: outCfg,
	}

	w := &lazyOutWriter{scope: s}
	res, err := fn(ctx, s, w)
	out, finErr := s.finalizeOut(err)
	if finErr != nil {
		return nil, nil, finErr
	}
	return out, res, nil
}

/* -------------------------------------------------------------------------- */
/*                         One-liner Helpers (typed)                          */
/* -------------------------------------------------------------------------- */

func Copy(ctx context.Context, src Source, out OutConfig) (*Output, error) {
	if out.reuseEnabled {
		return copyViaDoOut(ctx, src, out)
	}

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

	// Fast path: bytesSource to Memory (avoids io.Copy overhead)
	if b, ok := src.(bytesSource); ok && b != nil {
		size := int64(len(b))
		storageType := resolveStorageType(out, iSes, size)
		if storageType == Memory {
			output, err := iSes.newOutput(out.ext, Memory)
			if err != nil {
				return nil, err
			}
			output.mu.Lock()
			output.data = b
			output.mu.Unlock()
			return output, nil
		}
		if storageType == File {
			output, f, err := iSes.newOutputWithFile(out.ext, File)
			if err != nil {
				return nil, err
			}
			// Write directly to the open file handle
			_, writeErr := f.Write(b)
			closeErr := f.Close()
			if writeErr != nil {
				_ = output.cleanup()
				return nil, writeErr
			}
			if closeErr != nil {
				_ = output.cleanup()
				return nil, closeErr
			}
			return output, nil
		}
	}

	// Fast path: pathSource (file) - direct copy without lazy writer
	if ps, ok := src.(pathSource); ok {
		srcPath := string(ps)

		// Quick check: if session and out config both default to File, skip stat
		sessionStorageType := iSes.storageType
		if out.storageType != nil {
			sessionStorageType = *out.storageType
		}
		if sessionStorageType == File && iSes.autoFileThreshold <= 0 && out.autoFileThreshold == nil {
			// Fast path: file → file (uses sendfile/copy_file_range syscall)
			return copyFileToFile(iSes, out, srcPath)
		}

		// Need size for auto-threshold decisions
		size := SizeFromStream(ps)
		storageType := resolveStorageType(out, iSes, size)

		// Fast path: file → memory
		if storageType == Memory && size > 0 {
			return copyFileToMemory(iSes, out, srcPath, size)
		}

		// Fast path: file → file (uses sendfile/copy_file_range syscall)
		if storageType == File {
			return copyFileToFile(iSes, out, srcPath)
		}
	}

	return copyViaDoOut(ctx, src, out)
}

// sliceWriter wraps a byte slice to implement io.Writer and io.ReaderFrom
type sliceWriter struct {
	data []byte
	pos  int
}

func (w *sliceWriter) Write(p []byte) (int, error) {
	n := copy(w.data[w.pos:], p)
	w.pos += n
	return n, nil
}

func (w *sliceWriter) ReadFrom(r io.Reader) (int64, error) {
	// For *os.File, this enables efficient reading
	n, err := io.ReadFull(r, w.data[w.pos:])
	w.pos += n
	if err == io.ErrUnexpectedEOF || err == io.EOF {
		return int64(n), nil
	}
	return int64(n), err
}

func copyFileToMemory(iSes *ioSession, out OutConfig, srcPath string, size int64) (*Output, error) {
	output, err := iSes.newOutput(out.ext, Memory)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(srcPath)
	if err != nil {
		_ = output.cleanup()
		return nil, err
	}
	defer f.Close()

	if iSes != nil && iSes.useMmap {
		if data, cleanup, ok := tryMmap(f, size); ok {
			output.mu.Lock()
			output.data = data
			output.cleanupFunc = cleanup
			output.mu.Unlock()
			return output, nil
		}
	}

	if size >= 0 {
		w, err := output.OpenWriter(size)
		if err != nil {
			_ = output.cleanup()
			return nil, err
		}
		if _, err := io.Copy(w, f); err != nil {
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

	w, err := output.OpenWriter()
	if err != nil {
		_ = output.cleanup()
		return nil, err
	}
	if _, err := io.Copy(w, f); err != nil {
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

func copyFileToFile(iSes *ioSession, out OutConfig, srcPath string) (*Output, error) {
	// Open source file first to fail fast if it doesn't exist
	srcFile, err := os.Open(srcPath)
	if err != nil {
		return nil, err
	}

	output, dstFile, err := iSes.newOutputWithFile(out.ext, File)
	if err != nil {
		_ = srcFile.Close()
		return nil, err
	}

	// Use direct io.Copy to leverage copy_file_range syscall on supported platforms
	_, err = io.Copy(dstFile, srcFile)
	_ = srcFile.Close()
	closeErr := dstFile.Close()
	if err != nil {
		_ = output.cleanup()
		return nil, err
	}
	if closeErr != nil {
		_ = output.cleanup()
		return nil, closeErr
	}

	return output, nil
}

func copyViaDoOut(ctx context.Context, src Source, out OutConfig) (*Output, error) {
	return DoOut(ctx, out, func(ctx context.Context, s *OutScope, w io.Writer) error {
		r, _, err := s.UseSized(src)
		if err != nil {
			return err
		}
		_, err = io.Copy(w, r)
		return err
	})
}

func Process(ctx context.Context, src Source, out OutConfig, fn func(r io.Reader, w io.Writer) error) (*Output, error) {
	if fn == nil {
		return nil, ErrNilFunc
	}
	return DoOut(ctx, out, func(ctx context.Context, s *OutScope, w io.Writer) error {
		r, _, err := s.UseSized(src)
		if err != nil {
			return err
		}
		return fn(r, w)
	})
}

func ProcessList(ctx context.Context, srcs []Source, out OutConfig, fn func(readers []io.Reader, w io.Writer) error) (*Output, error) {
	if len(srcs) == 0 {
		return nil, ErrNilSource
	}
	if fn == nil {
		return nil, ErrNilFunc
	}
	return DoOut(ctx, out, func(ctx context.Context, s *OutScope, w io.Writer) error {
		readers := make([]io.Reader, 0, len(srcs))
		for _, src := range srcs {
			r, _, err := s.UseSized(src)
			if err != nil {
				return err
			}
			readers = append(readers, r)
		}
		return fn(readers, w)
	})
}

func ProcessResult[T any](ctx context.Context, src Source, out OutConfig, fn func(r io.Reader, w io.Writer) (*T, error)) (*Output, *T, error) {
	if fn == nil {
		return nil, nil, ErrNilFunc
	}
	return DoOutResult(ctx, out, func(ctx context.Context, s *OutScope, w io.Writer) (*T, error) {
		r, _, err := s.UseSized(src)
		if err != nil {
			return nil, err
		}
		return fn(r, w)
	})
}

func ProcessAtResult[T any](ctx context.Context, src Source, out OutConfig, fn func(ra io.ReaderAt, size int64, w io.Writer) (*T, error), opts ...ToReaderAtOption) (*Output, *T, error) {
	if fn == nil {
		return nil, nil, ErrNilFunc
	}
	return DoOutResult(ctx, out, func(ctx context.Context, s *OutScope, w io.Writer) (*T, error) {
		ra, size, err := s.UseReaderAt(src, opts...)
		if err != nil {
			return nil, err
		}
		if ra == nil {
			return nil, ErrCannotGetReaderAt
		}
		return fn(ra, size, w)
	})
}

func ProcessListResult[T any](ctx context.Context, srcs []Source, out OutConfig, fn func(readers []io.Reader, w io.Writer) (*T, error)) (*Output, *T, error) {
	if len(srcs) == 0 {
		return nil, nil, ErrNilSource
	}
	if fn == nil {
		return nil, nil, ErrNilFunc
	}
	return DoOutResult(ctx, out, func(ctx context.Context, s *OutScope, w io.Writer) (*T, error) {
		readers := make([]io.Reader, 0, len(srcs))
		var total int64
		for _, src := range srcs {
			r, size, err := s.UseSized(src)
			if err != nil {
				return nil, err
			}
			readers = append(readers, r)
			if total >= 0 && size >= 0 {
				total += size
			} else {
				total = -1
			}
		}
		return fn(readers, w)
	})
}

func ProcessAt(ctx context.Context, src Source, out OutConfig, fn func(ra io.ReaderAt, size int64, w io.Writer) error, opts ...ToReaderAtOption) (*Output, error) {
	if fn == nil {
		return nil, ErrNilFunc
	}
	return DoOut(ctx, out, func(ctx context.Context, s *OutScope, w io.Writer) error {
		ra, size, err := s.UseReaderAt(src, opts...)
		if err != nil {
			return err
		}
		if ra == nil {
			return ErrCannotGetReaderAt
		}
		return fn(ra, size, w)
	})
}

func Read(ctx context.Context, src Source, fn func(r io.Reader) error) error {
	if fn == nil {
		return ErrNilFunc
	}
	_, err := Do(ctx, func(s *Scope) (*Void, error) {
		r, useErr := s.Use(src)
		if useErr != nil {
			return nil, useErr
		}
		return nil, fn(r)
	})
	return err
}

func ReadResult[T any](ctx context.Context, src Source, fn func(r io.Reader) (*T, error)) (*T, error) {
	if fn == nil {
		return nil, ErrNilFunc
	}
	return Do(ctx, func(s *Scope) (*T, error) {
		r, useErr := s.Use(src)
		if useErr != nil {
			return nil, useErr
		}
		return fn(r)
	})
}

func ReadAt(ctx context.Context, src Source, fn func(ra io.ReaderAt, size int64) error, opts ...ToReaderAtOption) error {
	if fn == nil {
		return ErrNilFunc
	}
	_, err := Do(ctx, func(s *Scope) (*Void, error) {
		ra, size, useErr := s.UseReaderAt(src, opts...)
		if useErr != nil {
			return nil, useErr
		}
		if ra == nil {
			return nil, ErrCannotGetReaderAt
		}
		return nil, fn(ra, size)
	})
	return err
}

func ReadAtResult[T any](ctx context.Context, src Source, fn func(ra io.ReaderAt, size int64) (*T, error), opts ...ToReaderAtOption) (*T, error) {
	if fn == nil {
		return nil, ErrNilFunc
	}
	return Do(ctx, func(s *Scope) (*T, error) {
		ra, size, useErr := s.UseReaderAt(src, opts...)
		if useErr != nil {
			return nil, useErr
		}
		if ra == nil {
			return nil, ErrCannotGetReaderAt
		}
		return fn(ra, size)
	})
}

func ReadList(ctx context.Context, srcs []Source, fn func(readers []io.Reader) error) error {
	if len(srcs) == 0 {
		return ErrNilSource
	}
	if fn == nil {
		return ErrNilFunc
	}
	_, err := Do(ctx, func(s *Scope) (*Void, error) {
		readers := make([]io.Reader, 0, len(srcs))
		for _, src := range srcs {
			r, useErr := s.Use(src)
			if useErr != nil {
				return nil, useErr
			}
			readers = append(readers, r)
		}
		if err := fn(readers); err != nil {
			return nil, err
		}
		return nil, nil
	})
	return err
}

func ReadListResult[T any](ctx context.Context, srcs []Source, fn func(readers []io.Reader) (*T, error)) (*T, error) {
	if len(srcs) == 0 {
		return nil, ErrNilSource
	}
	if fn == nil {
		return nil, ErrNilFunc
	}
	return Do(ctx, func(s *Scope) (*T, error) {
		readers := make([]io.Reader, 0, len(srcs))
		for _, src := range srcs {
			r, useErr := s.Use(src)
			if useErr != nil {
				return nil, useErr
			}
			readers = append(readers, r)
		}
		return fn(readers)
	})
}

/* -------------------------------------------------------------------------- */
/*                                   Size                                     */
/* -------------------------------------------------------------------------- */

// Size returns the size of a Source, or -1 if unknown.
func Size(ctx context.Context, src Source) (int64, error) {
	if src == nil {
		return -1, ErrNilSource
	}

	rc, cleanup, size, _, _, err := src.open(ctx)
	if err != nil {
		return -1, err
	}
	if rc != nil {
		_ = rc.Close()
	}
	if cleanup != nil {
		_ = cleanup()
	}
	return size, nil
}

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

// SizeFromStream returns size for a type-safe Source without opening when possible.
// Returns -1 if size cannot be determined.
func SizeFromStream(src Source) int64 {
	if src == nil {
		return -1
	}

	switch v := src.(type) {
	case bytesSource:
		return int64(len(v))
	case pathSource:
		if fi, err := os.Stat(strings.TrimSpace(string(v))); err == nil {
			return fi.Size()
		}
		return -1
	case urlSource:
		return -1
	case readerSource:
		return SizeAny(v.r)
	case readCloserSource:
		return SizeAny(v.rc)
	case fileSource:
		return fileSize(v.f)
	case multipartSource:
		if v.fh != nil && v.fh.Size >= 0 {
			return v.fh.Size
		}
		return -1
	case outputSource:
		if v.o == nil {
			return -1
		}
		return v.o.Size()
	case inputSource:
		if v.in == nil {
			return -1
		}
		return v.in.Size
	default:
		return -1
	}
}

// SizeFromStreamList sums sizes for known sources.
// Returns -1 if any size cannot be determined.
func SizeFromStreamList(srcs []Source) int64 {
	if len(srcs) == 0 {
		return -1
	}

	var total int64
	for _, s := range srcs {
		size := SizeFromStream(s)
		if size < 0 {
			return -1
		}
		total += size
	}
	return total
}

func WriteFile(r io.Reader, path string) (int64, error) {
	if r == nil {
		return 0, ErrNilSource
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return 0, err
	}
	f, err := os.Create(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	return io.Copy(f, r)
}

func WriteStreamToFile(src Source, path string) (int64, error) {
	if src == nil {
		return 0, ErrNilSource
	}

	rc, cleanup, _, _, _, err := src.open(context.Background())
	if err != nil {
		return 0, err
	}
	if cleanup != nil {
		defer cleanup()
	}
	defer rc.Close()

	return WriteFile(rc, path)
}

type LineFunc func(line string) error

func ReadLines(ctx context.Context, src Source, fn LineFunc) error {
	if src == nil {
		return ErrNilSource
	}
	if fn == nil {
		return nil
	}

	_, err := Do(ctx, func(s *Scope) (*Void, error) {
		r, useErr := s.Use(src)
		if useErr != nil {
			return nil, useErr
		}
		scanner := bufio.NewScanner(r)
		buf := make([]byte, 0, 64*1024)
		scanner.Buffer(buf, 1024*1024)
		for scanner.Scan() {
			if err := fn(scanner.Text()); err != nil {
				return nil, err
			}
		}
		if err := scanner.Err(); err != nil {
			return nil, err
		}
		return nil, nil
	})
	return err
}

func ReadFileLines(ctx context.Context, path string, fn LineFunc) error {
	return ReadLines(ctx, PathSource(path), fn)
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
	reader            io.ReadCloser
	cleanup           func() error
	additionalCleanup func()
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
		errs = errors.Join(errs, d.reader.Close())
		d.reader = nil
	}

	if d.cleanup != nil {
		errs = errors.Join(errs, d.cleanup())
		d.cleanup = nil
	}

	if d.additionalCleanup != nil {
		d.additionalCleanup()
		d.additionalCleanup = nil
	}

	return errs
}

// NewDownloadReaderCloser wraps a Source as a DownloadReaderCloser.
func NewDownloadReaderCloser(src Source, cleanup ...func()) (DownloadReaderCloser, error) {
	if src == nil {
		return nil, ErrNilSource
	}

	rc, rcCleanup, _, _, _, err := src.open(context.Background())
	if err != nil {
		return nil, err
	}

	d := &downloadReaderCloser{
		reader:  rc,
		cleanup: rcCleanup,
	}

	if len(cleanup) > 0 {
		d.additionalCleanup = cleanup[0]
	}

	return d, nil
}

/* -------------------------------------------------------------------------- */
/*                                ToReaderAt                                  */
/* -------------------------------------------------------------------------- */

// readerAtReader is any type that implements both io.Reader and io.ReaderAt.
type readerAtReader struct {
	io.Reader
	io.ReaderAt
}

// readerAtReadCloser preserves ReaderAt while adding a no-op Close.
type readerAtReadCloser struct {
	*readerAtReader
}

func (r readerAtReadCloser) Close() error { return nil }

// writerToReadCloser preserves WriterTo while adding a no-op Close.
type writerToReadCloser struct {
	io.Reader
	io.WriterTo
}

func (w writerToReadCloser) Close() error { return nil }

// readerAtWriterToReadCloser preserves ReaderAt and WriterTo while adding a no-op Close.
type readerAtWriterToReadCloser struct {
	*readerAtReader
	io.WriterTo
}

func (r readerAtWriterToReadCloser) Close() error { return nil }

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

func (r *ReaderAtResult) Cleanup() error {
	if r == nil || r.cleanup == nil {
		return nil
	}
	return r.cleanup()
}

func (r *ReaderAtResult) Source() string {
	if r == nil {
		return ""
	}
	return r.source
}

type ToReaderAtOptions struct {
	maxMemoryBytes int64
	tempDir        string
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
	switch v := r.(type) {
	case readerAtReadCloser:
		if v.readerAtReader != nil && v.readerAtReader.ReaderAt != nil {
			return v.readerAtReader.ReaderAt
		}
	case *readerAtReadCloser:
		if v != nil && v.readerAtReader != nil && v.readerAtReader.ReaderAt != nil {
			return v.readerAtReader.ReaderAt
		}
	case readerAtWriterToReadCloser:
		if v.readerAtReader != nil && v.readerAtReader.ReaderAt != nil {
			return v.readerAtReader.ReaderAt
		}
	case *readerAtWriterToReadCloser:
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
		return nil, ErrToReaderAtNilReader
	}

	o := ToReaderAtOptions{
		maxMemoryBytes: 8 << 20,
		tempPattern:    "fio-readerat-*",
	}
	for _, fn := range opts {
		if fn != nil {
			fn(&o)
		}
	}

	if strings.TrimSpace(o.tempDir) == "" {
		if ses := Session(ctx); ses != nil {
			if d, ok := ses.(interface{ Dir() string }); ok {
				if dir := strings.TrimSpace(d.Dir()); dir != "" {
					o.tempDir = dir
				}
			}
		}
	}

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
			if sz := SizeAny(r); sz > 0 {
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

	if o.maxMemoryBytes <= 0 {
		return spoolToTempFile(r, o.tempDir, o.tempPattern)
	}

	limit := o.maxMemoryBytes
	buf := make([]byte, 0, minInt64(limit, 64<<10))
	tmp := make([]byte, 32<<10)

	var total int64
	for total <= limit {
		n, err := r.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
			total += int64(n)
		}

		if err != nil {
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

	return spillWithPrefix(r, buf, o.tempDir, o.tempPattern)
}

func spoolToTempFile(r io.Reader, dir, pattern string) (*ReaderAtResult, error) {
	tmp, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return nil, err
	}

	n, err := io.Copy(tmp, r)
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

func spillWithPrefix(r io.Reader, prefix []byte, dir, pattern string) (*ReaderAtResult, error) {
	tmp, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return nil, err
	}

	var total int64
	if len(prefix) > 0 {
		n, err := tmp.Write(prefix)
		if err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmp.Name())
			return nil, err
		}
		total += int64(n)
	}

	n2, err := io.Copy(tmp, r)
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
/*                                  Helpers                                   */
/* -------------------------------------------------------------------------- */

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

func SafeClose(c io.Closer) {
	if c != nil {
		_ = c.Close()
	}
}
