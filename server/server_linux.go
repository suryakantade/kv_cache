//go:build linux

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

// Server implements an epoll edge-triggered reactor for Linux.
type Server struct {
	cfg        Config
	handler    *commands.Handler
	epfd       int
	listenFd   int
	eventFd    int // eventfd for shutdown wakeup
	conns      map[int]*epConn
	mu         sync.Mutex
	connCnt    atomic.Int32
	shutdownCh chan struct{}
}

type epConn struct {
	fd    int
	rdBuf []byte
	wbuf  bytes.Buffer
	wantW bool
}

func New(cfg Config, handler *commands.Handler) *Server {
	if cfg.ReadBufSize == 0 {
		cfg.ReadBufSize = 4096
	}
	return &Server{
		cfg:        cfg,
		handler:    handler,
		conns:      make(map[int]*epConn),
		shutdownCh: make(chan struct{}),
	}
}

func (s *Server) ConnCount() int { return int(s.connCnt.Load()) }

func (s *Server) ListenAndServe() error {
	listenFd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return err
	}
	_ = unix.SetsockoptInt(listenFd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1)
	_ = unix.SetsockoptInt(listenFd, unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)

	ta, err := net.ResolveTCPAddr("tcp4", s.cfg.Addr)
	if err != nil {
		unix.Close(listenFd)
		return err
	}
	sa := &unix.SockaddrInet4{Port: ta.Port}
	if ip := ta.IP.To4(); ip != nil {
		copy(sa.Addr[:], ip)
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

	epfd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		unix.Close(listenFd)
		return err
	}
	s.epfd = epfd

	efd, err := unix.Eventfd(0, unix.EFD_NONBLOCK|unix.EFD_CLOEXEC)
	if err != nil {
		unix.Close(epfd)
		unix.Close(listenFd)
		return err
	}
	s.eventFd = efd

	if err := unix.EpollCtl(epfd, unix.EPOLL_CTL_ADD, listenFd,
		&unix.EpollEvent{Events: unix.EPOLLIN, Fd: int32(listenFd)}); err != nil {
		unix.Close(efd)
		unix.Close(epfd)
		unix.Close(listenFd)
		return err
	}
	if err := unix.EpollCtl(epfd, unix.EPOLL_CTL_ADD, efd,
		&unix.EpollEvent{Events: unix.EPOLLIN, Fd: int32(efd)}); err != nil {
		unix.Close(efd)
		unix.Close(epfd)
		unix.Close(listenFd)
		return err
	}

	log.Printf("kv-cache listening on %s (epoll event loop)", s.cfg.Addr)

	events := make([]unix.EpollEvent, 256)
	for {
		n, err := unix.EpollWait(epfd, events, -1)
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
			fd := int(events[i].Fd)
			if fd == s.eventFd {
				return nil
			}
			if fd == s.listenFd {
				s.acceptAll()
				continue
			}
			ev := events[i].Events
			if ev&(unix.EPOLLERR|unix.EPOLLHUP) != 0 {
				s.closeConn(fd)
				continue
			}
			if ev&unix.EPOLLIN != 0 {
				s.handleRead(fd)
			}
			if ev&unix.EPOLLOUT != 0 {
				s.handleWrite(fd)
			}
		}
	}
}

func (s *Server) acceptAll() {
	for {
		nfd, _, err := unix.Accept4(s.listenFd, unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC)
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
		_ = unix.SetsockoptInt(nfd, unix.IPPROTO_TCP, unix.TCP_NODELAY, 1)
		if err := unix.EpollCtl(s.epfd, unix.EPOLL_CTL_ADD, nfd,
			&unix.EpollEvent{Events: unix.EPOLLIN | unix.EPOLLET, Fd: int32(nfd)}); err != nil {
			log.Printf("epoll_ctl add fd=%d: %v", nfd, err)
			unix.Close(nfd)
			continue
		}
		s.mu.Lock()
		s.conns[nfd] = &epConn{fd: nfd}
		s.mu.Unlock()
		s.connCnt.Add(1)
		s.handler.IncrConn()
		log.Printf("new connection fd=%d", nfd)
	}
}

func (s *Server) handleRead(fd int) {
	s.mu.Lock()
	c, ok := s.conns[fd]
	s.mu.Unlock()
	if !ok {
		return
	}

	// Drain all available bytes from fd (edge-triggered: must read until EAGAIN).
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

	bReader := bytes.NewReader(c.rdBuf)
	bufRd := bufio.NewReader(bReader)
	quit := false

	for {
		val, err := resp.Parse(bufRd)
		if err != nil {
			unconsumed := bReader.Len() + bufRd.Buffered()
			c.rdBuf = c.rdBuf[len(c.rdBuf)-unconsumed:]
			if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				s.closeConn(fd)
				return
			}
			break
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

func (s *Server) flushWrite(fd int, c *epConn) {
	if c.wbuf.Len() == 0 {
		if c.wantW {
			_ = unix.EpollCtl(s.epfd, unix.EPOLL_CTL_MOD, fd,
				&unix.EpollEvent{Events: unix.EPOLLIN | unix.EPOLLET, Fd: int32(fd)})
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
		_ = unix.EpollCtl(s.epfd, unix.EPOLL_CTL_MOD, fd,
			&unix.EpollEvent{Events: unix.EPOLLIN | unix.EPOLLOUT | unix.EPOLLET, Fd: int32(fd)})
		c.wantW = true
	} else if c.wbuf.Len() == 0 && c.wantW {
		_ = unix.EpollCtl(s.epfd, unix.EPOLL_CTL_MOD, fd,
			&unix.EpollEvent{Events: unix.EPOLLIN | unix.EPOLLET, Fd: int32(fd)})
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
	_ = unix.EpollCtl(s.epfd, unix.EPOLL_CTL_DEL, fd, nil)
	unix.Close(fd)
	s.connCnt.Add(-1)
	s.handler.DecrConn()
	log.Printf("connection closed fd=%d", fd)
}

func (s *Server) Shutdown(_ context.Context) error {
	close(s.shutdownCh)
	var buf [8]byte
	buf[7] = 1
	_, _ = unix.Write(s.eventFd, buf[:])
	unix.Close(s.eventFd)
	unix.Close(s.epfd)
	unix.Close(s.listenFd)
	return nil
}
