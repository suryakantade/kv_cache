package server

// Config holds server configuration.
type Config struct {
	Addr        string // bind address, e.g. "0.0.0.0:6380"
	MaxConns    int    // 0 = unlimited
	ReadBufSize int    // per-connection read buffer (default 4096)
}
