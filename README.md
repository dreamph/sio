sio
===

`sio` is a Go library for efficient, stream-based file processing with predictable memory usage. It provides disk-backed sessions, in-memory options, and a unified interface for files, bytes, URLs, and multipart uploads.

Contents
--------
- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick start](#quick-start)
- [API Overview](#api-overview)
- [Example: HTTP File Processing](#example-http-file-processing)
- [License](#license)

Features
--------
- Unified `StreamReader` interface for files, bytes, URLs, and multipart uploads.
- Disk-backed and in-memory session management for predictable resource usage.
- Automatic cleanup of temporary files and session directories.
- Simple helpers for copying, saving, and reading outputs.
- Designed for server-side, CLI, and pipeline use cases.

Requirements
------------
- Go 1.23 or newer

Installation
------------
```bash
go get github.com/dreamph/sio
```

Import in your project:
```go
import "github.com/dreamph/sio"
```

Quick start
-----------
```go
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

	ioManager, err := sio.NewIoManager(filepath.Join(sio.DefaultBaseTempDir, "app-temp1"))
	if err != nil {
		log.Fatalf("manager: %v", err)
	}
	defer ioManager.Cleanup()

	ses, err := ioManager.NewSession()
	if err != nil {
		log.Fatalf("session: %v", err)
	}
	defer ses.Cleanup()

	ctx = sio.WithSession(ctx, ses)

	src := sio.NewBytesReader([]byte("hello world"))
	output, err := sio.Process(ctx, src, sio.Text, func(ctx context.Context, r io.Reader, w io.Writer) error {
		// example process file
		_, err := io.Copy(w, r)
		return err
	})
	if err != nil {
		log.Fatalf("process: %v", err)
	}
	defer output.Cleanup()

	fmt.Printf("output file: %s\n", output.Path())
}
```

API Overview
------------
- `Manager`: Manages a root temp directory and creates isolated `Session`s.
- `Session`: Processes streams, manages outputs, and cleans up temp files.
- `StreamReader`: Interface for file, bytes, URL, and multipart sources.
- `Output`: Represents a processed file, can be saved, read, or kept.

Example: HTTP File Processing
-----------------------------
See `example/main.go` for a full HTTP server. Here is a minimal handler:

```go
app.Post("/process", func(c *fiber.Ctx) error {
	ctx := c.Context()

	fileHeader, err := c.FormFile("file")
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "missing file")
	}

	// ioManager should be created at application startup
	ses, err := ioManager.NewSession()
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, err.Error())
	}
	defer ses.Cleanup()

	ctx = sio.WithSession(ctx, ses)

	in := sio.NewMultipartReader(fileHeader)
	output, err := sio.Process(ctx, in, sio.Pdf, func(ctx context.Context, r io.Reader, w io.Writer) error {
		// example process file
		_, err := io.Copy(w, r)
		return err
	})
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, err.Error())
	}

	readerCloser, err := sio.NewDownloadReaderCloser(output.AsStreamReader())
	if err != nil {
		return err
	}

	return c.SendStream(readerCloser)
})
```

License
-------
sio is distributed under the MIT License. See `LICENSE` for details.

Buy Me a Coffee
===============
[![](https://www.buymeacoffee.com/assets/img/custom_images/orange_img.png)](https://www.buymeacoffee.com/dreamph)
