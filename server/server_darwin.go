//go:build darwin

package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"

	"golang.org/x/sys/unix"

	"github.com/skantade/kv-cache/commands"
	"github.com/skantade/kv-cache/resp"
)

// Server implements a kqueue-based single-threaded reactor for Darwin (macOS).
type Server struct {
	cfg        Config
	handler    *commands.Handler
	kq         int
	listenFd   int
	wakePipe   [2]int
	conns      map[int]*kqConn
	mu         sync.Mutex
	connCnt    atomic.Int32
	shutdownCh chan struct{}
}

// kqConn holds per-connection state.
type kqConn struct {
	fd    int
	rdBuf []byte       // accumulated inbound bytes (partial commands)
	wbuf  bytes.Buffer // pending outbound bytes
	wantW bool         // EVFILT_WRITE registered?
}

func New(cfg Config, handler *commands.Handler) *Server {
	if cfg.ReadBufSize == 0 {
		cfg.ReadBufSize = 4096
	}
	return &Server{
		cfg:        cfg,
		handler:    handler,
		conns:      make(map[int]*kqConn),
		shutdownCh: make(chan struct{}),
	}
}

func (s *Server) ConnCount() int { return int(s.connCnt.Load()) }

func (s *Server) ListenAndServe() error {
	listenFd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		return err
	}
	_ = unix.SetsockoptInt(listenFd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1)
	_ = unix.SetsockoptInt(listenFd, unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
	_ = unix.SetNonblock(listenFd, true)

	sa, err := resolveTCPAddr(s.cfg.Addr)
	if err != nil {
		unix.Close(listenFd)
		return err
	}
	if err := unix.Bind(listenFd, sa); err != nil {
		unix.Close(listenFd)
		return err
	}
	if err := unix.Listen(listenFd, 128); err != nil {
		unix.Close(listenFd)
		return err
	}
	s.listenFd = listenFd

	kq, err := unix.Kqueue()
	if err != nil {
		unix.Close(listenFd)
		return err
	}
	s.kq = kq

	if err := unix.Pipe(s.wakePipe[:]); err != nil {
		unix.Close(kq)
		unix.Close(listenFd)
		return err
	}
	if err := s.keventAdd(listenFd, unix.EVFILT_READ, 0); err != nil {
		unix.Close(kq)
		unix.Close(listenFd)
		return err
	}
	if err := s.keventAdd(s.wakePipe[0], unix.EVFILT_READ, 0); err != nil {
		unix.Close(kq)
		unix.Close(listenFd)
		return err
	}

	log.Printf("kv-cache listening on %s (kqueue event loop)", s.cfg.Addr)

	events := make([]unix.Kevent_t, 256)
	for {
		n, err := unix.Kevent(kq, nil, events, nil)
		if err != nil {
			if err == unix.EINTR {
				continue
			}
			select {
			case <-s.shutdownCh:
				return nil
			default:
				return err
			}
		}
		for i := 0; i < n; i++ {
			fd := int(events[i].Ident)
			if fd == s.wakePipe[0] {
				return nil
			}
			if fd == s.listenFd {
				s.acceptAll()
				continue
			}
			if events[i].Flags&unix.EV_EOF != 0 || events[i].Flags&unix.EV_ERROR != 0 {
				s.closeConn(fd)
				continue
			}
			if events[i].Filter == unix.EVFILT_READ {
				s.handleRead(fd)
			}
			if events[i].Filter == unix.EVFILT_WRITE {
				s.handleWrite(fd)
			}
		}
	}
}

func (s *Server) acceptAll() {
	for {
		nfd, _, err := unix.Accept(s.listenFd)
		if err != nil {
			if err == unix.EAGAIN || err == unix.EWOULDBLOCK {
				return
			}
			log.Printf("accept error: %v", err)
			return
		}
		if s.cfg.MaxConns > 0 && int(s.connCnt.Load()) >= s.cfg.MaxConns {
			unix.Close(nfd)
			continue
		}
		_ = unix.SetNonblock(nfd, true)
		_ = unix.SetsockoptInt(nfd, unix.IPPROTO_TCP, unix.TCP_NODELAY, 1)
		if err := s.keventAdd(nfd, unix.EVFILT_READ, 0); err != nil {
			log.Printf("kevent add fd=%d: %v", nfd, err)
			unix.Close(nfd)
			continue
		}
		s.mu.Lock()
		s.conns[nfd] = &kqConn{fd: nfd}
		s.mu.Unlock()
		s.connCnt.Add(1)
		s.handler.IncrConn()
		log.Printf("new connection fd=%d", nfd)
	}
}

// handleRead drains the fd into the connection's read buffer, then parses and
// dispatches complete RESP commands. Partial commands stay in rdBuf for the
// next READ event — no data is ever thrown away mid-parse.
func (s *Server) handleRead(fd int) {
	s.mu.Lock()
	c, ok := s.conns[fd]
	s.mu.Unlock()
	if !ok {
		return
	}

	// Step 1: drain all available bytes from fd.
	tmp := make([]byte, s.cfg.ReadBufSize)
	for {
		n, err := unix.Read(fd, tmp)
		if n > 0 {
			c.rdBuf = append(c.rdBuf, tmp[:n]...)
		}
		if err == unix.EAGAIN || err == unix.EWOULDBLOCK {
			break
		}
		if err != nil || n == 0 {
			s.closeConn(fd)
			return
		}
	}

	// Step 2: parse complete RESP commands from the accumulated buffer.
	// We wrap c.rdBuf in a bytes.Reader so we can compute consumed bytes:
	//   consumed = len(c.rdBuf) - bReader.Len() - bufRd.Buffered()
	bReader := bytes.NewReader(c.rdBuf)
	bufRd := bufio.NewReader(bReader)
	quit := false

	for {
		val, err := resp.Parse(bufRd)
		if err != nil {
			// Preserve unconsumed bytes (partial command) in rdBuf.
			unconsumed := bReader.Len() + bufRd.Buffered()
			c.rdBuf = c.rdBuf[len(c.rdBuf)-unconsumed:]
			if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				// Genuine parse error — bad client.
				s.closeConn(fd)
				return
			}
			break // partial command; wait for next READ event
		}

		if val.Type == resp.TypeArray && len(val.Array) > 0 {
			result := s.handler.Handle(val.Array)
			if err := result.Write(&c.wbuf); err != nil {
				s.closeConn(fd)
				return
			}
			if strings.ToUpper(val.Array[0].Str) == "QUIT" {
				quit = true
				unconsumed := bReader.Len() + bufRd.Buffered()
				c.rdBuf = c.rdBuf[len(c.rdBuf)-unconsumed:]
				break
			}
		}
	}

	if c.wbuf.Len() > 0 {
		s.flushWrite(fd, c)
	}
	if quit {
		s.closeConn(fd)
	}
}

func (s *Server) handleWrite(fd int) {
	s.mu.Lock()
	c, ok := s.conns[fd]
	s.mu.Unlock()
	if !ok {
		return
	}
	s.flushWrite(fd, c)
}

func (s *Server) flushWrite(fd int, c *kqConn) {
	if c.wbuf.Len() == 0 {
		if c.wantW {
			_ = s.keventDel(fd, unix.EVFILT_WRITE)
			c.wantW = false
		}
		return
	}
	n, err := unix.Write(fd, c.wbuf.Bytes())
	if n > 0 {
		c.wbuf.Next(n)
	}
	if err != nil && err != unix.EAGAIN && err != unix.EWOULDBLOCK {
		s.closeConn(fd)
		return
	}
	if c.wbuf.Len() > 0 && !c.wantW {
		_ = s.keventAdd(fd, unix.EVFILT_WRITE, 0)
		c.wantW = true
	} else if c.wbuf.Len() == 0 && c.wantW {
		_ = s.keventDel(fd, unix.EVFILT_WRITE)
		c.wantW = false
	}
}

func (s *Server) closeConn(fd int) {
	s.mu.Lock()
	_, ok := s.conns[fd]
	if ok {
		delete(s.conns, fd)
	}
	s.mu.Unlock()
	if !ok {
		return
	}
	_ = s.keventDel(fd, unix.EVFILT_READ)
	_ = s.keventDel(fd, unix.EVFILT_WRITE)
	unix.Close(fd)
	s.connCnt.Add(-1)
	s.handler.DecrConn()
	log.Printf("connection closed fd=%d", fd)
}

func (s *Server) Shutdown(_ context.Context) error {
	close(s.shutdownCh)
	_, _ = unix.Write(s.wakePipe[1], []byte{1})
	unix.Close(s.wakePipe[1])
	unix.Close(s.wakePipe[0])
	unix.Close(s.kq)
	unix.Close(s.listenFd)
	return nil
}

// ---------------------------------------------------------------------------
// kqueue helpers
// ---------------------------------------------------------------------------

func (s *Server) keventAdd(fd int, filter int16, flags uint16) error {
	var changes [1]unix.Kevent_t
	unix.SetKevent(&changes[0], fd, int(filter), unix.EV_ADD|unix.EV_ENABLE|int(flags))
	_, err := unix.Kevent(s.kq, changes[:], nil, nil)
	return err
}

func (s *Server) keventDel(fd int, filter int16) error {
	var changes [1]unix.Kevent_t
	unix.SetKevent(&changes[0], fd, int(filter), unix.EV_DELETE)
	_, err := unix.Kevent(s.kq, changes[:], nil, nil)
	return err
}

func resolveTCPAddr(addr string) (unix.Sockaddr, error) {
	ta, err := net.ResolveTCPAddr("tcp4", addr)
	if err != nil {
		return nil, err
	}
	sa := &unix.SockaddrInet4{Port: ta.Port}
	if ip := ta.IP.To4(); ip != nil {
		copy(sa.Addr[:], ip)
	}
	return sa, nil
}
