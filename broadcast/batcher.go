package main

import (
	"sync"
	"time"
	"fmt"
	"os"
)

type SingleOp struct {
	item int64
	done chan error
}

type Batcher struct {
	mu    sync.Mutex
	queue []SingleOp
	fn    func([]int64) error
}

func NewBatcher(fn func([]int64) error) *Batcher {
	return &Batcher{
		fn: fn,
	}
}

func (b *Batcher) Run(max_delay time.Duration) {
	fmt.Fprintf(os.Stderr, "Starting batcher with %d delay.\n", max_delay)
	for {
		b.mu.Lock()
		queue := b.queue
		b.queue = []SingleOp{}
		b.mu.Unlock()

		ops := make([]int64, len(queue))
		dones := make([]chan error, len(queue))
		for id, op := range queue {
			ops[id] = op.item
			dones[id] = op.done
		}
		go b.processBatch(ops, dones)
		time.Sleep(max_delay)
	}
}

func (b *Batcher) processBatch(ops []int64, dones []chan error) {
	fmt.Fprintf(os.Stderr, "Starting batch processing of %v.\n", ops)
	err := b.fn(ops)
	for _, done := range dones {
		done <- err
	}
}

func (b *Batcher) Process(op int64) error {
	done := make(chan error)
	b.mu.Lock()
	b.queue = append(b.queue, SingleOp{
		item: op,
		done: done,
	})
	b.mu.Unlock()
	return <-done
}
