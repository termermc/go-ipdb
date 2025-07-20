package ipdb_geoip_tools

import "io"

type noOpReadCloser struct {
	io.Reader
}

func (n noOpReadCloser) Close() error {
	return nil
}
