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
fio.ErrEmptyPath              // empty path string
fio.ErrEmptyURL               // empty URL string
fio.ErrOutputCleaned          // output already cleaned up
fio.ErrNilOutHandle           // nil OutHandle
fio.ErrNilOutScope            // nil out-scope
fio.ErrNewOutMultiple         // NewOut called more than once
fio.ErrOutReuseRequiresPtr    // OutReuse requires output pointer
fio.ErrCannotGetReaderAt      // reader does not support ReaderAt
fio.ErrInputNotReusable       // input is not reusable
fio.ErrCannotResetInput       // input reset failed
fio.ErrToReaderAtNilReader    // ToReaderAt called with nil reader
```

Use `errors.Is` to check wrapped errors:

```go
if errors.Is(err, fio.ErrDownloadFailed) {
    // handle URL download failures
}
```

## Platform Support

- **Memory-mapped I/O**: Available on Darwin, Linux, FreeBSD, NetBSD, OpenBSD
- **Other platforms**: Falls back to standard file I/O

## Benchmark Comparison
Benchmark comparing `fio` and `normal` (standard library io.Copy) on Apple M2 Max.
### Legend
| Symbol | Meaning |
|--------|---------|
| ⚡ | Fastest speed |
| 💾 | Lowest memory |
| 🏆 | Best overall |
### Bytes Source → Memory Storage
| Size | Method | Speed | Throughput | Memory | Allocs | Notes |
|------|--------|-------|------------|--------|--------|-------|
| **1KB** | normal | 195 ns | 5,259 MB/s | 1,152 B | 5 | copies data |
|  | **fio** | **135 ns** | **7,567 MB/s** | **249 B** | **3** | 🏆⚡💾 zero-copy |
| **1MB** | normal | 92.8 µs | 11,304 MB/s | 1.0 MB | 5 | copies data |
|  | **fio** | **145 ns** | **7,244,530 MB/s** | **244 B** | **3** | 🏆⚡💾 zero-copy |
| **10MB** | normal | 436.5 µs | 24,021 MB/s | 10 MB | 5 | copies data |
|  | **fio** | **149 ns** | **70,581,965 MB/s** | **240 B** | **3** | 🏆⚡💾 zero-copy |
| **100MB** | normal | 3.58 ms | 29,281 MB/s | 100 MB | 5 | copies data |
|  | **fio** | **142 ns** | **740,346,343 MB/s** | **245 B** | **3** | 🏆⚡💾 zero-copy |

### Bytes Source → File Storage
| Size | Method | Speed | Throughput | Memory | Allocs | Notes |
|------|--------|-------|------------|--------|--------|-------|
| **1KB** | **normal** | **116.5 µs** | **8.8 MB/s** | 744 B | 9 | ⚡ |
|  | **fio** | 137.3 µs | 7.5 MB/s | **722 B** | **11** | 💾 |
| **1MB** | normal | 423.9 µs | 2,474 MB/s | 744 B | 9 |  |
|  | **fio** | **346.9 µs** | **3,023 MB/s** | **722 B** | **11** | 🏆⚡💾 |
| **10MB** | **normal** | **2.89 ms** | **3,629 MB/s** | 752 B | 9 | ⚡ |
|  | **fio** | 3.27 ms | 3,210 MB/s | **708 B** | **11** | 💾 |
| **100MB** | **normal** | **25.72 ms** | **4,077 MB/s** | 805 B | 9 | ⚡ |
|  | **fio** | 38.20 ms | 2,745 MB/s | **725 B** | **11** | 💾 |

### File Source → Memory Storage
| Size | Method | Speed | Throughput | Memory | Allocs | Notes |
|------|--------|-------|------------|--------|--------|-------|
| **1KB** | normal | 19.3 µs | 53.1 MB/s | 34,096 B | 8 |  |
|  | **fio** | **16.9 µs** | **60.7 MB/s** | **1,958 B** | **13** | 🏆⚡💾 |
| **1MB** | normal | 463.0 µs | 2,265 MB/s | 2.0 MB | 13 |  |
|  | **fio** | **119.8 µs** | **8,754 MB/s** | **1.0 MB** | **13** | 🏆⚡💾 |
| **10MB** | normal | 2.27 ms | 4,612 MB/s | 32 MB | 17 |  |
|  | **fio** | **1.08 ms** | **9,690 MB/s** | **10 MB** | **13** | 🏆⚡💾 |
| **100MB** | **normal** | **16.26 ms** | **6,449 MB/s** | 256 MB | 20 | ⚡ |
|  | **fio** | 17.96 ms | 5,839 MB/s | **100 MB** | **13** | 💾 |

### File Source → File Storage
| Size | Method | Speed | Throughput | Memory | Allocs | Notes |
|------|--------|-------|------------|--------|--------|-------|
| **1KB** | **normal** | **142.3 µs** | **7.2 MB/s** | **33,696 B** | **13** | 🏆⚡💾 |
|  | fio | 162.3 µs | 6.3 MB/s | 33,733 B | 17 |  |
| **1MB** | **normal** | **532.9 µs** | **1,968 MB/s** | **33,712 B** | **13** | 🏆⚡💾 |
|  | fio | 558.2 µs | 1,879 MB/s | 33,720 B | 17 |  |
| **10MB** | **normal** | **4.12 ms** | **2,543 MB/s** | **33,713 B** | **13** | 🏆⚡💾 |
|  | fio | 4.83 ms | 2,169 MB/s | 33,723 B | 17 |  |
| **100MB** | **normal** | **35.87 ms** | **2,923 MB/s** | **33,727 B** | **13** | 🏆⚡💾 |
|  | fio | 45.47 ms | 2,306 MB/s | 33,737 B | 17 |  |

### Summary
| Scenario | Winner | Why |
|----------|--------|-----|
| **bytes → memory** | 🏆 **fio** | Zero-copy, fastest in all sizes, minimal memory |
| **bytes → file** | **normal** | Faster at 1KB/10MB/100MB; fio uses slightly less memory and wins at 1MB |
| **file → memory** | **fio** | Faster for 1KB–10MB with lower memory; 100MB slower but still uses less memory |
| **file → file** | **normal** | Faster across all sizes; memory is effectively the same |

### Key Takeaways

1. **fio bytes→memory is zero-copy** - constant-time regardless of data size
2. **Memory efficiency** - fio allocates dramatically less for bytes sources and less for file→memory
3. **Bytes→file is mixed** - normal wins most sizes, fio wins 1MB and uses slightly less memory
4. **File→file favors normal** - normal is faster across all sizes with similar memory

### Run Benchmarks

```bash
# Basic benchmark
go test -bench=BenchmarkCompareFioSio -benchmem

# With mmap enabled (Unix only)
FIO_BENCH_USE_MMAP=true go test -bench=BenchmarkCompareFioSio -benchmem

```
