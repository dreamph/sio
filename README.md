# sio

[![Go Version](https://img.shields.io/badge/Go-%3E%3D%201.23-blue)](https://go.dev/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/dreamph/sio)](https://goreportcard.com/report/github.com/dreamph/sio)

**Stream I/O made simple.** Zero surprises, predictable memory, automatic cleanup.

`sio` is a Go library that brings sanity to file processing. Whether you're handling uploads from HTTP requests, processing files from disk, fetching content from URLs, or working with in-memory data—`sio` gives you a unified interface with predictable resource usage and automatic cleanup.

## Table of Contents

- [Why sio?](#why-sio)
- [Key Features](#key-features)
- [Quick Start](#quick-start)
- [Real-World Example: HTTP File Upload](#real-world-example-http-file-upload)
- [Common Use Cases](#common-use-cases)
- [API Overview](#api-overview)
- [Advanced Features](#advanced-features)
- [Best Practices](#best-practices)
- [Performance Tips](#performance-tips)
- [FAQ](#faq)
- [File Extension Constants](#file-extension-constants)
- [Design Philosophy](#design-philosophy)
- [Contributing](#contributing)
- [License](#license)

---

## Why sio?

**The Problem:**

When building production services that process files, you face a maze of decisions:

- Where do temporary files go? Who cleans them up?
- How do I avoid memory explosions on large uploads?
- How do I handle files, bytes, URLs, and multipart uploads consistently?
- How do I prevent resource leaks when errors occur?

**The Solution:**

`sio` provides a simple, composable API that handles all of this for you:

```go
// Process a file upload with automatic cleanup
output, err := sio.Process(ctx,
    sio.NewMultipartReader(fileHeader),
    sio.Out(sio.Pdf),
    func(ctx context.Context, r io.Reader, w io.Writer) error {
        // Your processing logic here
        return processDocument(r, w)
    },
)
// Temp files cleaned up automatically when session ends
```

---

## Key Features

### 🔄 **Unified Interface**

Work with files, bytes, URLs, and multipart uploads through a single `StreamReader` interface. No more switching between different APIs.

### 💾 **Flexible Storage**

Choose between disk-backed or in-memory storage. Need speed? Use memory. Processing large files? Use disk. Mix and match per operation.

### 🧹 **Automatic Cleanup**

Temporary files are tracked and cleaned up automatically. No more leaked temp files filling up your disk.

### 📊 **Predictable Memory**

Control exactly how your data flows. Stream large files without loading everything into memory.

### ⚡ **Production Ready & Optimized**

Built for real-world use: handles errors gracefully, supports concurrent sessions, and integrates seamlessly with popular frameworks.

**Performance:** Optimized for production with microsecond-level latency (3.9µs for 1KB files), memory pooling, and fast-path execution for memory-only operations.

---

## Quick Start

### Installation

```bash
go get github.com/dreamph/sio
```

### Basic Example

```go
package main

import (
    "context"
    "fmt"
    "io"
    "log"
    "path/filepath"

    "github.com/dreamph/sio"
)

func main() {
    ctx := context.Background()

    // Create a manager (handles temp directory)
    ioManager, _ := sio.NewIoManager(filepath.Join(sio.DefaultBaseTempDir, "myapp"), sio.File)
    defer ioManager.Cleanup()

    // Create a session (isolated workspace)
    ses, _ := ioManager.NewSession()
    defer ses.Cleanup()

    ctx = sio.WithSession(ctx, ses)

    // Process data from any source
    src := sio.NewBytesReader([]byte("hello world"))
    output, err := sio.Process(ctx, src, sio.Out(sio.Text),
        func(ctx context.Context, r io.Reader, w io.Writer) error {
            _, err := io.Copy(w, r)
            return err
        },
    )

    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("Output: %s\n", output.Path())
}
```

---

## Real-World Example: HTTP File Upload

Here's how to handle file uploads in a web server with automatic cleanup:

```go
app.Post("/convert", func(c *fiber.Ctx) error {
    // Get uploaded file
    fileHeader, err := c.FormFile("document")
    if err != nil {
        return fiber.NewError(fiber.StatusBadRequest, "missing file")
    }

    // Create isolated session for this request
    ses, _ := ioManager.NewSession()
    defer ses.Cleanup() // Auto-cleanup when request ends

    ctx := sio.WithSession(c.UserContext(), ses)

    // Process the upload
    output, err := sio.Process(ctx,
        sio.NewMultipartReader(fileHeader),
        sio.Out(sio.Pdf),
        func(ctx context.Context, r io.Reader, w io.Writer) error {
            // Your conversion logic
            return convertToPDF(r, w)
        },
    )

    if err != nil {
        return err
    }

    // Stream result back to client
    reader, _ := sio.NewDownloadReaderCloser(output.Reader())
    c.Set("Content-Type", "application/pdf")
    return c.SendStream(reader)
})
```

**See the full example:** [`example/main.go`](example/main.go)

---

## Common Use Cases

### 📤 **Process File Uploads**

```go
src := sio.NewMultipartReader(fileHeader)
output, _ := sio.Process(ctx, src, sio.Out(sio.Jpg), processImage)
```

### 🌐 **Fetch and Process URLs**

```go
src := sio.NewURLReader("https://example.com/data.csv")
output, _ := sio.Process(ctx, src, sio.Out(sio.Xlsx), convertToExcel)
```

### 💾 **Transform Local Files**

```go
src := sio.NewFileReader("/path/to/input.txt")
output, _ := sio.Process(ctx, src, sio.Out(sio.Pdf), generatePDF)
```

### 🧠 **Work with In-Memory Data**

```go
src := sio.NewBytesReader(data)
output, _ := sio.Process(ctx, src, sio.Out(sio.Zip), compress)
```

### ⚡ **Force Memory Storage for Speed**

```go
// Process small files entirely in memory for max performance
output, _ := sio.Process(ctx, src,
    sio.Out(sio.Json, sio.Memory), // Force in-memory storage
    transform,
)
```

---

## API Overview

### Core Components

| Component          | Purpose                                                          |
|--------------------|------------------------------------------------------------------|
| **`Manager`**      | Manages the root temp directory and creates isolated sessions   |
| **`Session`**      | Isolated workspace for processing streams with automatic cleanup |
| **`StreamReader`** | Unified interface for files, bytes, URLs, and multipart uploads |
| **`Output`**       | Result of a processing operation—can be read, saved, or streamed |

### Storage Modes

```go
// Disk-backed (default) - for large files
ioManager, _ := sio.NewIoManager("/tmp/myapp", sio.File)

// Memory-only - for small, fast operations
ioManager, _ := sio.NewIoManager("", sio.Memory)

// Override per operation
output, _ := sio.Process(ctx, src,
    sio.Out(sio.Pdf, sio.Memory), // Force memory for this output
    process,
)
```

### Common Patterns

**Read without creating output:**

```go
err := sio.Read(ctx, src, func(ctx context.Context, r io.Reader) error {
    // Just read, no output file created
    return processStream(r)
})
```

**Process multiple inputs:**

```go
sources := []sio.StreamReader{
    sio.NewFileReader("file1.txt"),
    sio.NewFileReader("file2.txt"),
}
output, _ := sio.ProcessList(ctx, sources, sio.Out(sio.Zip), mergeFiles)
```

**Save output permanently:**

```go
output, _ := sio.Process(ctx, src, sio.Out(sio.Pdf), convert)
output.SaveAs("/permanent/location/result.pdf")
```

**Keep output beyond session cleanup:**

```go
output, _ := sio.Process(ctx, src, sio.Out(sio.Pdf), convert)
output.Keep() // Won't be deleted when session.Cleanup() runs
```

---

## Advanced Features

### Stream from URLs with Options

```go
src := sio.NewURLReader("https://api.example.com/data",
    sio.URLReaderOptions{}.
        WithTimeout(60 * time.Second).
        WithInsecureTLS(true),
)
```

### Read Line by Line

```go
err := sio.ReadLines(ctx, src, func(line string) error {
    // Process each line
    return handleLine(line)
})
```

### Convert to ReaderAt

```go
result, _ := output.AsReaderAt(ctx,
    sio.WithMaxMemoryBytes(10 << 20), // Keep up to 10MB in memory
)
defer result.Cleanup()
// Use result.ReaderAt() with libraries that need random access
```

### Bind Multiple Streams

```go
output, _ := sio.BindProcess(ctx, sio.Out(sio.Zip),
    func(ctx context.Context, b *sio.Binder, w io.Writer) error {
        file1, _ := b.Use(sio.NewFileReader("doc1.txt"))
        file2, _ := b.Use(sio.NewFileReader("doc2.txt"))
        return createZip(w, file1, file2)
    },
)
```

---

## Best Practices

### Always Use defer for Cleanup

```go
// Manager cleanup
ioManager, _ := sio.NewIoManager("/tmp/myapp", sio.File)
defer ioManager.Cleanup() // Always defer!

// Session cleanup
ses, _ := ioManager.NewSession()
defer ses.Cleanup() // Clean up after each request
```

### Handle Errors Properly

```go
output, err := sio.Process(ctx, src, sio.Out(sio.Pdf), convert)
if err != nil {
    // Error already triggered cleanup
    return fmt.Errorf("processing failed: %w", err)
}
// Output is valid and can be used
```

### Choose the Right Storage Mode

```go
// Small files (< 10MB) - use Memory for speed
output, _ := sio.Process(ctx, src,
    sio.Out(sio.Json, sio.Memory),
    processSmall,
)

// Large files (> 10MB) - use File to avoid memory pressure
output, _ := sio.Process(ctx, src,
    sio.Out(sio.Pdf, sio.File),
    processLarge,
)
```

### One Session Per Request

```go
// Good: Each HTTP request gets its own session
app.Post("/upload", func(c *fiber.Ctx) error {
    ses, _ := ioManager.NewSession()
    defer ses.Cleanup()

    ctx := sio.WithSession(c.UserContext(), ses)
    // Process files...
})

// Bad: Reusing session across requests
var globalSession sio.IoSession // DON'T DO THIS
```

### Use Keep() or SaveAs() for Persistent Files

```go
// Temporary processing - auto cleanup
output, _ := sio.Process(ctx, src, sio.Out(sio.ToExt("tmp")), process)
// File deleted when session.Cleanup() runs

// Keep for later use
output, _ := sio.Process(ctx, src, sio.Out(sio.Pdf), process)
output.Keep() // Won't be deleted

// Save to permanent location
output, _ := sio.Process(ctx, src, sio.Out(sio.Pdf), process)
output.SaveAs("/permanent/files/result.pdf")
```

---

## Performance Tips

### Memory vs Disk Trade-offs

```go
// Fast but uses memory - good for small files
ioManager, _ := sio.NewIoManager("", sio.Memory)

// Slower but handles unlimited size - good for large files
ioManager, _ := sio.NewIoManager("/tmp/myapp", sio.File)

// Best of both: Default to disk, override for small operations
ioManager, _ := sio.NewIoManager("/tmp/myapp", sio.File)
// Use Memory for specific small operations
output, _ := sio.Process(ctx, src, sio.Out(sio.Json, sio.Memory), fn)
```

### Streaming for Large Files

```go
// Don't load entire file into memory
err := sio.Read(ctx, src, func(ctx context.Context, r io.Reader) error {
    // Process in chunks
    scanner := bufio.NewScanner(r)
    for scanner.Scan() {
        processLine(scanner.Text())
    }
    return scanner.Err()
})
```

### Reuse Managers

```go
// Good: Create once, reuse for all requests
var ioManager sio.IoManager

func init() {
    ioManager, _ = sio.NewIoManager("/tmp/myapp", sio.File)
}

// Bad: Creating manager per request
func handler() {
    mgr, _ := sio.NewIoManager("/tmp/myapp", sio.File) // Expensive!
}
```

### URL Downloads with Timeouts

```go
// Set appropriate timeouts for external resources
src := sio.NewURLReader("https://slow-api.com/data",
    sio.URLReaderOptions{}.
        WithTimeout(60 * time.Second), // Prevent hanging
)
```

### Concurrent Processing

```go
// Safe: Each goroutine gets its own session
var wg sync.WaitGroup
for _, file := range files {
    wg.Add(1)
    go func(f string) {
        defer wg.Done()

        ses, _ := ioManager.NewSession() // Isolated session
        defer ses.Cleanup()

        ctx := sio.WithSession(context.Background(), ses)
        sio.Process(ctx, sio.NewFileReader(f), sio.Out(sio.Pdf), convert)
    }(file)
}
wg.Wait()
```

---

## FAQ

### How do I process files larger than available RAM?

Use file-based storage (default) and stream the data:

```go
ioManager, _ := sio.NewIoManager("/tmp/myapp", sio.File)
// Files are written to disk, not loaded into memory
```

### Is sio safe for concurrent use?

Yes! Each session is isolated. Just create a new session per request/goroutine:

```go
// Thread-safe: Each goroutine gets its own session
ses, _ := ioManager.NewSession()
defer ses.Cleanup()
```

### What happens if I forget to call Cleanup()?

Temporary files will remain on disk until the program exits or the OS cleans them up. Always use `defer`:

```go
ses, _ := ioManager.NewSession()
defer ses.Cleanup() // Always!
```

### Can I mix file and memory storage?

Yes! You can override storage per operation:

```go
ioManager, _ := sio.NewIoManager("/tmp/myapp", sio.File)

// This operation uses memory
output1, _ := sio.Process(ctx, src, sio.Out(sio.Json, sio.Memory), fn)

// This operation uses disk
output2, _ := sio.Process(ctx, src, sio.Out(sio.Pdf, sio.File), fn)
```

### How do I handle errors during processing?

If your processing function returns an error, the output is automatically cleaned up:

```go
output, err := sio.Process(ctx, src, sio.Out(sio.Pdf), func(ctx context.Context, r io.Reader, w io.Writer) error {
    if err := validate(r); err != nil {
        return err // Output is cleaned up automatically
    }
    return process(r, w)
})
if err != nil {
    // Output was already cleaned up
    return err
}
// Output is valid
```

### Can I use sio with standard library functions?

Absolutely! StreamReader produces standard `io.Reader`:

```go
err := sio.Read(ctx, src, func(ctx context.Context, r io.Reader) error {
    // r is io.Reader - use with any stdlib function
    return json.NewDecoder(r).Decode(&data)
})
```

### How do I download from URLs with authentication?

Use a custom HTTP client:

```go
client := &http.Client{
    Transport: &customTransport{token: "..."},
}
src := sio.NewURLReader("https://api.example.com/data",
    sio.URLReaderOptions{}.WithClient(client),
)
```

### What's the difference between Keep() and SaveAs()?

- **Keep()**: Marks the temp file to survive `session.Cleanup()`, but it stays in the temp directory
- **SaveAs()**: Copies the file to a permanent location outside the temp directory

```go
output, _ := sio.Process(ctx, src, sio.Out(sio.Pdf), convert)

// Keep in temp directory but don't delete
output.Keep()

// OR copy to permanent location
output.SaveAs("/permanent/files/result.pdf")
```

---

## File Extension Constants

```go
sio.Pdf    // ".pdf"
sio.Text   // ".txt"
sio.Csv    // ".csv"
sio.Jpg    // ".jpg"
sio.Png    // ".png"
sio.Xlsx   // ".xlsx"
sio.Docx   // ".docx"
sio.Pptx   // ".pptx"
sio.Zip    // ".zip"
sio.Json   // ".json"
sio.Xml    // ".xml"

// Or create your own
customExt := sio.ToExt("xml") // ".xml"
```

---

## Requirements

- **Go 1.23** or newer

---

## Design Philosophy

`sio` follows these principles:

1. **Predictable**: Resources are cleaned up automatically and deterministically
2. **Composable**: Small, focused components that work together
3. **Practical**: Built for real production workloads, not academic exercises
4. **Safe**: Proper error handling and resource management built-in
5. **Flexible**: Choose the trade-offs that make sense for your use case

---

## License

MIT License. See [`LICENSE`](LICENSE) for details.

---

## Support

If you find `sio` helpful, consider supporting development:

[![Buy Me a Coffee](https://www.buymeacoffee.com/assets/img/custom_images/orange_img.png)](https://www.buymeacoffee.com/dreamph)

---

Built with ❤️ for Gophers who process files
