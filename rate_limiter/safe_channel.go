package rate_limiter

import "sync"

type SafeChan[T any] struct {
	ch   chan T
	once sync.Once
}

func NewSafeChan[T any](bufferSize int) *SafeChan[T] {
	return &SafeChan[T]{
		ch: make(chan T, bufferSize),
	}
}

func (sc *SafeChan[T]) Chan() chan T {
	return sc.ch
}

func (sc *SafeChan[T]) Close() {
	sc.once.Do(func() {
		close(sc.ch)
	})
}
