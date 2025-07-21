package ipdb

import "io"

type noOpReadCloser struct {
	io.Reader
}

func (n noOpReadCloser) Close() error {
	return nil
}
