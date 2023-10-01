package main

import (
	"fmt"
	"os"
	"time"
)

type SingleOp struct {
	item int64
	done chan error
}

type Batcher struct {
	queue chan SingleOp
	fn    func([]int64) error
}

func NewBatcher(fn func([]int64) error) *Batcher {
	return &Batcher{
		fn:    fn,
		queue: make(chan SingleOp),
	}
}

func (b *Batcher) Run(max_delay time.Duration) {
	fmt.Fprintf(os.Stderr, "Starting batcher with %d delay.\n", max_delay)
	for {
		ops, dones := b.collectFromQueueForDuration(max_delay)
		go b.processBatch(ops, dones)
		time.Sleep(max_delay)
	}
}

func (b *Batcher) collectFromQueueForDuration(max_delay time.Duration) ([]int64, []chan error) {
	ops := []int64{}
	dones := []chan error{}
	timer := time.NewTimer(max_delay)
	for {
		select {
		case op := <-b.queue:
			ops = append(ops, op.item)
			dones = append(dones, op.done)
		case <-timer.C:
			return ops, dones
		}
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
	b.queue <- SingleOp{
		item: op,
		done: done,
	}
	return <-done
}
