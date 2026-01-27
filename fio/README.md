# fio

Package `fio` provides streaming I/O utilities with session management, automatic resource cleanup, and flexible storage backends (memory/file).

## Features

- **Write Once, Dynamic Storage** - Same code works with memory or file storage; automatically switches based on data size
- **Type-safe Sources** - Strongly typed input sources (files, URLs, bytes, readers, multipart)
- **Session Management** - Automatic temp file cleanup via IoManager/IoSession
- **Dual Storage Backends** - Memory or file-based storage with automatic spill-to-disk
- **Reusable Inputs** - Reset and reuse input streams multiple times
- **ReaderAt Conversion** - Convert any io.Reader to io.ReaderAt with memory/temp-file buffering
- **Memory-mapped I/O** - Optional mmap support on Unix systems for fast file reading

## Why fio?

**Write once, run with any storage backend.** Your code doesn't change whether data is stored in memory or files:

```go
// Same code - storage is determined by configuration, not code changes
output, _ := fio.Copy(ctx, fio.PathSource("input.txt"), fio.Out(".txt"))

// Read result the same way regardless of storage type
data, _ := output.Bytes()      // works for both memory and file
reader, _ := output.OpenReader() // works for both memory and file
```

**Automatic storage selection based on data size:**

```go
// Manager with auto-threshold: small data → memory, large data → file
mgr, _ := fio.NewIoManager("./temp", fio.Memory,
    fio.WithThreshold(10*1024*1024),  // Switch to file at 10MB
)

// Your code stays the same - fio decides storage automatically
output, _ := fio.Copy(ctx, source, fio.Out(".json"))
// 1KB file  → stored in memory
// 50MB file → stored in temp file
```

**Benefits:**
- No `if/else` for memory vs file handling
- Automatic cleanup of temp files via session
- Consistent API regardless of storage backend
- Optimal performance for each scenario

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
result, err := fio.Read(ctx, fio.PathSource("data.json"), func(r io.Reader) (*MyData, error) {
    var data MyData
    if err := json.NewDecoder(r).Decode(&data); err != nil {
        return nil, err
    }
    return &data, nil
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
io.Copy(w1, input.Reader)

// Reset and read again
input.Reset()
io.Copy(w2, input.Reader)
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
    r, err := s.Use(fio.PathSource("input.txt"))  // Auto-cleanup on scope exit
    if err != nil {
        return MyResult{}, err
    }
    // Process r...
    return result, nil
})
```

### Output Scope (DoOut)

```go
output, err := fio.DoOut(ctx, fio.Out(".txt"),
    func(ctx context.Context, s *fio.OutScope, w io.Writer) error {
        r, err := s.Use(fio.PathSource("input.txt"))
        if err != nil {
            return err
        }
        _, err := io.Copy(w, r)
        return err
    })
```

### Output with Result (DoOutResult)

```go
output, metadata, err := fio.DoOutResult(ctx, fio.Out(".txt"),
    func(ctx context.Context, s *fio.OutScope, w io.Writer) (*Metadata, error) {
        r, size, err := s.UseSized(fio.PathSource("input.txt"))
        if err != nil {
            return nil, err
        }
        _, err := io.Copy(w, r)
        if err != nil {
            return nil, err
        }
        m := Metadata{Size: size}
        return &m, nil
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
| **1KB** | normal | 201 ns | 5,084 MB/s | 1,152 B | 5 | copies data |
|  | **fio** | **130 ns** | **7,865 MB/s** | **249 B** | **3** | 🏆⚡💾 zero-copy |
| **1MB** | normal | 115 µs | 9,139 MB/s | 1.0 MB | 5 | copies data |
|  | **fio** | **137 ns** | **7,655,321 MB/s** | **241 B** | **3** | 🏆⚡💾 zero-copy |
| **10MB** | normal | 604 µs | 17,362 MB/s | 10 MB | 5 | copies data |
|  | **fio** | **144 ns** | **72,978,112 MB/s** | **241 B** | **3** | 🏆⚡💾 zero-copy |
| **100MB** | normal | 3.61 ms | 29,010 MB/s | 100 MB | 5 | copies data |
|  | **fio** | **133 ns** | **788,153,403 MB/s** | **240 B** | **3** | 🏆⚡💾 zero-copy |

### Bytes Source → File Storage
| Size | Method | Speed | Throughput | Memory | Allocs | Notes |
|------|--------|-------|------------|--------|--------|-------|
| **1KB** | **normal** | **117 µs** | **8.7 MB/s** | 743 B | 9 | ⚡ |
|  | fio | 139 µs | 7.4 MB/s | **727 B** | 11 | 💾 |
| **1MB** | normal | 379 µs | 2,765 MB/s | 744 B | 9 |  |
|  | **fio** | **268 µs** | **3,914 MB/s** | **720 B** | **11** | 🏆⚡💾 |
| **10MB** | normal | 2.80 ms | 3,751 MB/s | 746 B | 9 |  |
|  | **fio** | **2.18 ms** | **4,818 MB/s** | **709 B** | **11** | 🏆⚡💾 |
| **100MB** | normal | 23.1 ms | 4,540 MB/s | 781 B | 9 |  |
|  | **fio** | **22.8 ms** | **4,591 MB/s** | **718 B** | **11** | 🏆⚡💾 |

### File Source → Memory Storage
| Size | Method | Speed | Throughput | Memory | Allocs | Notes |
|------|--------|-------|------------|--------|--------|-------|
| **1KB** | normal | 20.7 µs | 49.4 MB/s | 34,096 B | 8 |  |
|  | **fio** | **17.9 µs** | **57.2 MB/s** | **1,965 B** | **13** | 🏆⚡💾 |
| **1MB** | normal | 797 µs | 1,315 MB/s | 2.0 MB | 13 |  |
|  | **fio** | **108 µs** | **9,685 MB/s** | **1.0 MB** | **13** | 🏆⚡💾 |
| **10MB** | normal | 2.66 ms | 3,939 MB/s | 32 MB | 17 |  |
|  | **fio** | **1.33 ms** | **7,893 MB/s** | **10 MB** | **13** | 🏆⚡💾 |
| **100MB** | normal | 16.7 ms | 6,290 MB/s | 256 MB | 20 |  |
|  | **fio** | **16.3 ms** | **6,432 MB/s** | **100 MB** | **13** | 🏆⚡💾 |

### File Source → File Storage
| Size | Method | Speed | Throughput | Memory | Allocs | Notes |
|------|--------|-------|------------|--------|--------|-------|
| **1KB** | **normal** | **144 µs** | **7.1 MB/s** | **33,696 B** | **13** | ⚡💾 |
|  | fio | 166 µs | 6.2 MB/s | 33,730 B | 17 |  |
| **1MB** | **normal** | **513 µs** | **2,043 MB/s** | **33,712 B** | **13** | ⚡💾 |
|  | fio | 573 µs | 1,830 MB/s | 33,730 B | 17 |  |
| **10MB** | **normal** | **4.32 ms** | **2,425 MB/s** | **33,712 B** | **13** | ⚡💾 |
|  | fio | 4.51 ms | 2,326 MB/s | 33,721 B | 17 |  |
| **100MB** | normal | 44.0 ms | 2,385 MB/s | 33,717 B | 13 |  |
|  | **fio** | **41.1 ms** | **2,553 MB/s** | 33,723 B | 17 | 🏆⚡ |

### Bytes Source → Read Only (no output)

| Size | Method | Speed | Throughput | Memory | Allocs | Notes |
|------|--------|-------|------------|--------|--------|-------|
| **1KB** | **normal** | **39.6 ns** | **25,862 MB/s** | **64 B** | **2** | ⚡💾 |
| | fio | 65.0 ns | 15,755 MB/s | 136 B | 3 | |
| **1MB** | **normal** | **43.5 ns** | **24.1 TB/s** | **64 B** | **2** | ⚡💾 zero-copy discard |
| | fio | 72.3 ns | 14.5 TB/s | 136 B | 3 | |
| **10MB** | **normal** | **41.0 ns** | **256 TB/s** | **64 B** | **2** | ⚡💾 zero-copy discard |
| | fio | 68.1 ns | 154 TB/s | 136 B | 3 | |
| **100MB** | **normal** | **39.9 ns** | **2.6 PB/s** | **64 B** | **2** | ⚡💾 zero-copy discard |
| | fio | 66.2 ns | 1.6 PB/s | 136 B | 3 | |

### File Source → Read Only (no output)

| Size | Method | Speed | Throughput | Memory | Allocs | Notes |
|------|--------|-------|------------|--------|--------|-------|
| **1KB** | **normal** | **15.7 µs** | **65.1 MB/s** | **240 B** | **4** | ⚡💾 |
| | fio | 16.5 µs | 62.1 MB/s | 553 B | 9 | |
| **1MB** | **normal** | **98.6 µs** | **10,630 MB/s** | **240 B** | **4** | ⚡💾 |
| | fio | 99.7 µs | 10,516 MB/s | 553 B | 9 | |
| **10MB** | fio | 839 µs | 12,488 MB/s | 554 B | 9 | ⚡ |
| | **normal** | 844 µs | 12,424 MB/s | **244 B** | **4** | 💾 |
| **100MB** | **normal** | **11.1 ms** | **9,407 MB/s** | **245 B** | **4** | 💾 |
| | fio | 11.5 ms | 9,100 MB/s | 583 B | 9 | |

### Summary

| Scenario | Winner | Why |
|----------|--------|-----|
| **bytes → memory** | 🏆 **fio** | Zero-copy, fastest in all sizes, minimal memory |
| **bytes → file** | 🏆 **fio** | Faster at 1MB/10MB/100MB (28-41% faster); slightly slower at 1KB |
| **file → memory** | 🏆 **fio** | Faster for all sizes with 50% less memory |
| **file → file** | **mixed** | normal faster at small sizes (4-12%); fio faster at 100MB (7%) |
| **bytes → read-only** | **normal** | Both near-instant; fio has minimal overhead (~1.6x) |
| **file → read-only** | **~equal** | Comparable performance; fio slightly more allocations |

### Key Takeaways

1. **fio bytes→memory is zero-copy** - constant-time regardless of data size
2. **fio bytes→file is optimized** - 28-41% faster than normal for 1MB+ files
3. **fio file→memory is efficient** - 7x faster at 1MB with 50% less memory
4. **file→file is competitive** - fio slightly slower at small files (~10%), faster at large files
5. **read-only is near-instant for bytes** - both normal and fio use zero-copy to io.Discard
6. **file read-only is I/O bound** - ~10-12 GB/s throughput, both methods comparable

### Run Benchmarks

```bash
# Basic benchmark
go test -bench=BenchmarkCompareFioSio -benchmem -benchtime=3s

# With mmap enabled (Unix only)
FIO_BENCH_USE_MMAP=true go test -bench=BenchmarkCompareFioSio -benchmem -benchtime=3s
```
