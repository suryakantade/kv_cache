//go:build !darwin && !linux

package server

import (
	"bufio"
	"context"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/skantade/kv-cache/commands"
	"github.com/skantade/kv-cache/resp"
)

// Server is a goroutine-per-connection TCP server for non-Linux/Darwin platforms.
type Server struct {
	cfg      Config
	handler  *commands.Handler
	listener net.Listener
	mu       sync.Mutex
	conns    map[net.Conn]struct{}
	connCnt  atomic.Int32
	done     chan struct{}
}

func New(cfg Config, handler *commands.Handler) *Server {
	if cfg.ReadBufSize == 0 {
		cfg.ReadBufSize = 4096
	}
	return &Server{
		cfg:     cfg,
		handler: handler,
		conns:   make(map[net.Conn]struct{}),
		done:    make(chan struct{}),
	}
}

func (s *Server) ConnCount() int { return int(s.connCnt.Load()) }

func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return err
	}
	s.listener = ln
	log.Printf("kv-cache listening on %s (goroutine-per-conn mode)", s.cfg.Addr)

	for {
		nc, err := ln.Accept()
		if err != nil {
			select {
			case <-s.done:
				return nil
			default:
				log.Printf("accept error: %v", err)
				continue
			}
		}
		s.mu.Lock()
		if s.cfg.MaxConns > 0 && len(s.conns) >= s.cfg.MaxConns {
			s.mu.Unlock()
			nc.Close()
			continue
		}
		s.conns[nc] = struct{}{}
		s.mu.Unlock()
		s.connCnt.Add(1)
		s.handler.IncrConn()
		go s.handle(nc)
	}
}

func (s *Server) handle(nc net.Conn) {
	defer func() {
		nc.Close()
		s.mu.Lock()
		delete(s.conns, nc)
		s.mu.Unlock()
		s.connCnt.Add(-1)
		s.handler.DecrConn()
		log.Printf("connection closed: %s", nc.RemoteAddr())
	}()

	log.Printf("new connection: %s", nc.RemoteAddr())
	br := bufio.NewReaderSize(nc, s.cfg.ReadBufSize)

	for {
		val, err := resp.Parse(br)
		if err != nil {
			if err != io.EOF {
				log.Printf("parse error from %s: %v", nc.RemoteAddr(), err)
			}
			return
		}
		if val.Type != resp.TypeArray || len(val.Array) == 0 {
			continue
		}
		result := s.handler.Handle(val.Array)
		if err := result.Write(nc); err != nil {
			return
		}
		if strings.ToUpper(val.Array[0].Str) == "QUIT" {
			return
		}
	}
}

func (s *Server) Shutdown(_ context.Context) error {
	close(s.done)
	if s.listener != nil {
		s.listener.Close()
	}
	s.mu.Lock()
	for nc := range s.conns {
		nc.Close()
	}
	s.mu.Unlock()
	return nil
}
