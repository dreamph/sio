package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/dreamph/sio"

	"github.com/gofiber/fiber/v2"
)

func newServerApp(ioManager sio.Manager) (*fiber.App, error) {
	app := fiber.New(fiber.Config{
		BodyLimit: 1000 * 1024 * 1024,
	})

	app.Post("/process-by-io", func(c *fiber.Ctx) error {
		fileHeader, err := c.FormFile("file")
		if err != nil {
			return fiber.NewError(fiber.StatusBadRequest, "missing multipart form field : file")
		}

		in := sio.NewMultipartReader(fileHeader)

		outputExt := filepath.Ext(fileHeader.Filename)
		if outputExt == "" {
			outputExt = ".bin"
		}

		result, err := sio.Process(c.UserContext(), in, outputExt, func(ctx context.Context, r io.Reader, w io.Writer) error {
			_, err := io.Copy(w, r)
			return err
		})
		if err != nil {
			return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("process failed: %v", err))
		}
		result = result.Keep()

		c.Type("application/octet-stream")
		c.Set(fiber.HeaderContentDisposition, fmt.Sprintf("attachment; filename=%q", "processed"+outputExt))

		readerCloser, err := sio.NewDownloadReaderCloser(result.AsStreamReader())
		if err != nil {
			return err
		}

		return c.SendStream(readerCloser)
	})

	app.Post("/process-by-bytes", func(c *fiber.Ctx) error {
		fileHeader, err := c.FormFile("file")
		if err != nil {
			return fiber.NewError(fiber.StatusBadRequest, "missing multipart form field \"file\"")
		}

		file, err := fileHeader.Open()
		if err != nil {
			return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("open upload failed: %v", err))
		}
		defer file.Close()

		data, err := io.ReadAll(file)
		if err != nil {
			return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("read upload failed: %v", err))
		}

		var out bytes.Buffer
		out.Write(data)

		c.Type("application/octet-stream")
		c.Set(fiber.HeaderContentDisposition, fmt.Sprintf("attachment; filename=%q", fileHeader.Filename))
		return c.Send(out.Bytes())
	})

	return app, nil
}

func main() {
	ioManager, err := sio.NewManager("./temp-files")
	if err != nil {
		log.Fatal(err)
	}
	defer ioManager.Cleanup()

	app, err := newServerApp(ioManager)
	if err != nil {
		log.Fatalf("newServerApp: %v", err)
	}

	shutdownCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- app.Listen(":8080")
	}()

	log.Println("server listening on :8080")

	select {
	case <-shutdownCtx.Done():
		log.Println("shutdown signal received")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := app.ShutdownWithContext(ctx); err != nil {
			log.Printf("server shutdown error: %v", err)
		}
		if err := <-serverErr; err != nil {
			log.Printf("server exited with error: %v", err)
		}
	case err := <-serverErr:
		if err != nil {
			log.Fatalf("server error: %v", err)
		}
	}

	log.Println("server stopped")
}

func requestContext(c *fiber.Ctx) context.Context {
	ctx := c.UserContext()
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
