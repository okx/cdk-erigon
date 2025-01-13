package logging

import (
	"bufio"
	"context"
	"github.com/ledgerwatch/log/v3"
	"io"
	"sync"
	"time"
)

const (
	_BufferSize    = 1 << 20 // 1 MiB
	_FlushInterval = time.Second * 10
)

type AsyncBufferedWriter struct {
	Size          int
	FlushInterval time.Duration
	ctx           context.Context

	mu sync.Mutex

	bufferedWriter *bufio.Writer
	ticker         *time.Ticker
	done           chan struct{}
	stop           chan struct{}

	initialized bool
	stopped     bool
}

func AsyncHandler(wr io.Writer, format log.Format, ctx context.Context) log.Handler {
	asyncBufferedWriter := &AsyncBufferedWriter{ctx: ctx}
	asyncBufferedWriter.initialize(wr)

	h := log.FuncHandler(func(r *log.Record) error {
		_, err := asyncBufferedWriter.write(format.Format(r))
		return err
	})
	return h
}

func (s *AsyncBufferedWriter) initialize(wr io.Writer) {
	if s.initialized {
		return
	}

	s.Size = _BufferSize
	s.FlushInterval = _FlushInterval

	s.ticker = time.NewTicker(s.FlushInterval)
	s.bufferedWriter = bufio.NewWriterSize(wr, s.Size)

	s.done = make(chan struct{})
	s.stop = make(chan struct{})

	s.initialized = true

	go s.flushLoop()
}

func (s *AsyncBufferedWriter) write(b []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(b) >= s.bufferedWriter.Available() && s.bufferedWriter.Buffered() > 0 {
		if err := s.bufferedWriter.Flush(); err != nil {
			return 0, err
		}
	}

	return s.bufferedWriter.Write(b)
}

func (s *AsyncBufferedWriter) flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.bufferedWriter.Flush()
}

func (s *AsyncBufferedWriter) flushLoop() {
	defer close(s.done)
	for {
		select {
		case <-s.ticker.C:
			_ = s.flush()
		case <-s.stop:
			return
		case <-s.ctx.Done():
			s.Stop()
		}
	}
}

func (s *AsyncBufferedWriter) Stop() (err error) {
	var stopped bool
	func() {
		s.mu.Lock()
		defer s.mu.Unlock()

		if !s.initialized {
			return
		}
		stopped = s.stopped

		if stopped {
			return
		}

		s.stopped = true

		s.ticker.Stop()
		close(s.stop)
		<-s.done
	}()

	if !stopped {
		err = s.bufferedWriter.Flush()
	}
	return err
}
