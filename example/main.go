package main

import (
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

func newServerApp(ioManager sio.IoManager) (*fiber.App, error) {
	app := fiber.New(fiber.Config{
		BodyLimit: 1000 * 1024 * 1024,
	})

	app.Post("/process", func(c *fiber.Ctx) error {
		fileHeader, err := c.FormFile("file")
		if err != nil {
			return fiber.NewError(fiber.StatusBadRequest, "missing multipart form field : file")
		}

		outputExt := filepath.Ext(fileHeader.Filename)
		ses, err := ioManager.NewSession()
		if err != nil {
			return fiber.NewError(fiber.StatusInternalServerError, err.Error())
		}
		defer ses.Cleanup()

		ctx := sio.WithSession(c.UserContext(), ses)

		in := sio.NewMultipartReader(fileHeader)
		output, err := sio.Process(ctx, in, sio.Out(outputExt), func(ctx context.Context, r io.Reader, w io.Writer) error {
			_, err := io.Copy(w, r)
			return err
		})
		if err != nil {
			return fiber.NewError(fiber.StatusInternalServerError, fmt.Sprintf("process failed: %v", err))
		}

		c.Type("application/octet-stream")
		c.Set(fiber.HeaderContentDisposition, fmt.Sprintf("attachment; filename=%q", "processed"+outputExt))

		readerCloser, err := sio.NewDownloadReaderCloser(output.Reader())
		if err != nil {
			return err
		}

		return c.SendStream(readerCloser)
	})

	return app, nil
}

func main() {
	ioManager, err := sio.NewIoManager(filepath.Join(sio.DefaultBaseTempDir, "test1"))
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
