# fio

Package `fio` provides streaming I/O utilities with session management, automatic resource cleanup, and flexible storage backends (memory/file).

## Features

- **Type-safe Sources** - Strongly typed input sources (files, URLs, bytes, readers, multipart)
- **Session Management** - Automatic temp file cleanup via IoManager/IoSession
- **Dual Storage Backends** - Memory or file-based storage with automatic spill-to-disk
- **Reusable Inputs** - Reset and reuse input streams multiple times
- **ReaderAt Conversion** - Convert any io.Reader to io.ReaderAt with memory/temp-file buffering
- **Memory-mapped I/O** - Optional mmap support on Unix systems for fast file reading
- **Buffer Pooling** - Shared copy buffers to reduce allocations

## Installation

```go
import "github.com/dreamph/sio/fio"
```

## Quick Start

### Basic Copy

```go
// Create an IoManager with memory storage
mgr, _ := fio.NewIoManager("./temp", fio.Memory)
defer mgr.Cleanup()

// Create a session
ses, _ := mgr.NewSession()
defer ses.Cleanup()

// Use session in context
ctx := fio.WithSession(context.Background(), ses)

// Copy from file to memory output
output, _ := fio.Copy(ctx, fio.PathSource("input.txt"), fio.Out(".txt"))

// Read the result
data, _ := output.Bytes()
```

### Reading Files

```go
// Read and process a file
result, err := fio.Read(ctx, fio.PathSource("data.json"), func(r io.Reader) (MyData, error) {
    var data MyData
    return data, json.NewDecoder(r).Decode(&data)
})
```

### Processing with Output

```go
// Transform input to output
output, err := fio.Process(ctx, fio.PathSource("input.txt"), fio.Out(".txt"),
    func(r io.Reader, w io.Writer) error {
        // Transform data from r to w
        _, err := io.Copy(w, r)
        return err
    })
```

## Source Types

Create type-safe input sources:

```go
// From file path
src := fio.PathSource("/path/to/file.txt")

// From URL (auto-downloads)
src := fio.URLSource("https://example.com/file.txt")

// From bytes
src := fio.BytesSource([]byte("hello world"))

// From io.Reader
src := fio.ReaderSource(reader)

// From io.ReadCloser
src := fio.ReadCloserSource(readCloser)

// From *os.File
src := fio.FileSource(file)

// From multipart file header
src := fio.MultipartSource(fileHeader)

// From existing Output
src := fio.OutputSource(output)

// From existing Input
src := fio.InputSource(input)
```

## Session Management

### IoManager

Manages temp directories and creates sessions:

```go
// Create manager with file storage backend
mgr, err := fio.NewIoManager("./temp", fio.File,
    fio.WithThreshold(1024*1024),      // Auto-switch to file at 1MB
    fio.WithSpillThreshold(64<<20),    // Spill memory to file at 64MB
    fio.WithMaxPreallocate(1<<20),     // Cap pre-allocation at 1MB
    fio.WithCopyBufferPool(true),      // Enable shared copy buffers
    fio.WithMmap(true),                // Enable mmap on Unix
)
defer mgr.Cleanup()
```

### IoSession

Represents a single operation scope with automatic cleanup:

```go
ses, _ := mgr.NewSession()
defer ses.Cleanup() // Cleans up all temp files

// Create output within session
output, _ := ses.NewOut(fio.Out(".json"), 1024)
```

### Context Integration

```go
ctx := fio.WithSession(context.Background(), ses)

// Retrieve session from context
ses := fio.Session(ctx)
```

## Output Configuration

Configure output behavior:

```go
// Basic output with extension
out := fio.Out(".json")

// Force memory storage
out := fio.Out(".json", fio.Memory)

// Force file storage
out := fio.Out(".json", fio.File)

// With spill threshold
out := fio.Out(".json", fio.WithSpillThreshold(32<<20))

// With output reuse (for repeated operations)
var cached *fio.Output
out := fio.Out(".json", fio.OutReuse(&cached))
```

## Reusable Inputs

Open a source once and read multiple times:

```go
// Open as reusable
input, _ := fio.OpenIn(ctx, fio.PathSource("data.txt"), fio.Reusable())
defer input.Close()

// First read
io.Copy(w1, input.R)

// Reset and read again
input.Reset()
io.Copy(w2, input.R)
```

## ReaderAt Conversion

Convert streaming readers to random-access:

```go
// Auto-buffers in memory or spills to temp file
result, _ := fio.ToReaderAt(ctx, reader,
    fio.WithMaxMemoryBytes(8<<20),  // Buffer up to 8MB in memory
    fio.WithTempDir("./temp"),       // Temp dir for spill files
)
defer result.Cleanup()

ra := result.ReaderAt()
size := result.Size()
```

## Scoped Operations

### Read-only Scope (Do)

```go
result, err := fio.Do(ctx, func(s *fio.Scope) (MyResult, error) {
    r := s.Use(fio.PathSource("input.txt"))  // Auto-cleanup on scope exit
    // Process r...
    return result, nil
})
```

### Output Scope (DoOut)

```go
output, err := fio.DoOut(ctx, fio.Out(".txt"),
    func(ctx context.Context, s *fio.OutScope, w io.Writer) error {
        r := s.Use(fio.PathSource("input.txt"))
        _, err := io.Copy(w, r)
        return err
    })
```

### Output with Result (DoOutResult)

```go
output, metadata, err := fio.DoOutResult(ctx, fio.Out(".txt"),
    func(ctx context.Context, s *fio.OutScope, w io.Writer) (Metadata, error) {
        r, size := s.UseSized(fio.PathSource("input.txt"))
        _, err := io.Copy(w, r)
        return Metadata{Size: size}, err
    })
```

## Utility Functions

### Size Detection

```go
// Get size from source (may open the source)
size, _ := fio.Size(ctx, src)

// Get size without opening (when possible)
size := fio.SizeFromStream(src)

// Get size from any type
size := fio.SizeAny(reader)
```

### Line Reading

```go
err := fio.ReadLines(ctx, fio.PathSource("file.txt"), func(line string) error {
    fmt.Println(line)
    return nil
})

// Shorthand for file path
err := fio.ReadFileLines(ctx, "file.txt", func(line string) error {
    return nil
})
```

### Direct File Writing

```go
// Write reader to file
n, err := fio.WriteFile(reader, "/path/to/output.txt")

// Write source to file
n, err := fio.WriteStreamToFile(src, "/path/to/output.txt")
```

## Output Methods

```go
// Get data as bytes
data, _ := output.Bytes()

// Get raw byte slice (memory storage only)
data := output.Data()

// Open reader
r, _ := output.OpenReader()
defer r.Close()

// Open writer
w, _ := output.OpenWriter(sizeHint)
defer w.Close()

// Write to io.Writer
n, _ := output.WriteTo(writer)

// Save to file
err := output.SaveAs("/path/to/file.txt")

// Keep file after session cleanup
output.Keep()

// Get file path (file storage only)
path := output.Path()

// Get storage type
st := output.StorageType()

// Get size
size := output.Size()
```

## File Extension Constants

```go
fio.Json  // ".json"
fio.Csv   // ".csv"
fio.Txt   // ".txt"
fio.Xml   // ".xml"
fio.Pdf   // ".pdf"
fio.Docx  // ".docx"
fio.Xlsx  // ".xlsx"
fio.Pptx  // ".pptx"
fio.Jpg   // ".jpg"
fio.Jpeg  // ".jpeg"
fio.Png   // ".png"
fio.Zip   // ".zip"
```

## Helper Functions

```go
// Convert MB to bytes
bytes := fio.MB(10) // 10485760

// Convert format to extension
ext := fio.ToExt("json") // ".json"

// Join cleanup functions
cleanup := fio.JoinCleanup(fn1, fn2, fn3)
defer cleanup()

// Safe close (ignores nil)
fio.SafeClose(closer)
```

## Global Configuration

```go
// Configure custom HTTP client (for URL sources)
fio.Configure(fio.NewConfig(&http.Client{
    Timeout: 60 * time.Second,
}))
```

## Error Types

```go
fio.ErrNilSource              // nil source provided
fio.ErrIoManagerClosed        // manager is closed
fio.ErrIoSessionClosed        // session is closed
fio.ErrDownloadFailed         // URL download failed
fio.ErrNoSession              // session is nil
fio.ErrFileStorageUnavailable // file storage requires directory
fio.ErrInvalidSessionType     // invalid session type
fio.ErrNilFunc                // function is nil
```

## Platform Support

- **Memory-mapped I/O**: Available on Darwin, Linux, FreeBSD, NetBSD, OpenBSD
- **Other platforms**: Falls back to standard file I/O
