package siox

import (
	"context"
	"io"
	"mime/multipart"

	"github.com/dreamph/sio"
)

type DoFn = func(ctx context.Context, r io.Reader, w io.Writer) error

// -----------------------------------------------------------------------------
// Do: io.Reader → Output
// -----------------------------------------------------------------------------

func Do(ctx context.Context, r io.Reader, out sio.OutConfig, fn DoFn) (*sio.Output, error) {
	src := sio.NewGenericReader(r)
	return sio.Process(ctx, src, out, fn)
}

// -----------------------------------------------------------------------------
// Bytes
// -----------------------------------------------------------------------------

func DoBytes(ctx context.Context, data []byte, out sio.OutConfig, fn DoFn) (*sio.Output, error) {
	src := sio.NewBytesReader(data)
	return sio.Process(ctx, src, out, fn)
}

// -----------------------------------------------------------------------------
// File path
// -----------------------------------------------------------------------------

func DoFile(ctx context.Context, path string, out sio.OutConfig, fn DoFn) (*sio.Output, error) {
	src := sio.NewFileReader(path)
	return sio.Process(ctx, src, out, fn)
}

// -----------------------------------------------------------------------------
// multipart.FileHeader
// -----------------------------------------------------------------------------

func DoMultipart(ctx context.Context, fh *multipart.FileHeader, out sio.OutConfig, fn DoFn) (*sio.Output, error) {
	src := sio.NewMultipartReader(fh)
	return sio.Process(ctx, src, out, fn)
}

// -----------------------------------------------------------------------------
// URL
// -----------------------------------------------------------------------------

func DoURL(ctx context.Context, urlStr string, out sio.OutConfig, fn DoFn) (*sio.Output, error) {
	src := sio.NewURLReader(urlStr)
	return sio.Process(ctx, src, out, fn)
}

// -----------------------------------------------------------------------------
// streaming helper
// -----------------------------------------------------------------------------

func StreamOutput(out *sio.Output, w io.Writer) (int64, error) {
	sr := out.Reader()
	rc, err := sr.Open()
	if err != nil {
		return 0, err
	}
	defer rc.Close()
	defer sr.Cleanup()

	return io.Copy(w, rc)
}
