package main

import (
	"context"
	"encoding/json"
	"log"
	"sort"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	messages := NewConcurrentSet()
	batchers := map[string]*Batcher{}

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		nodes := n.NodeIDs()
		sort.Strings(nodes)
		var index int
		for idx, node := range n.NodeIDs() {
			if node == n.ID() {
				index = idx
				break
			}
		}

		indexes := []int{(index - 1) / 2, index*2 + 1, index*2 + 2}
		for _, r_index := range indexes {
			if r_index >= 0 && r_index < len(nodes) {
				batcher := NewBatcher(PropagateBatchFn(
					n,
					nodes[r_index],
					300*time.Millisecond,
				))
				batchers[nodes[r_index]] = batcher
				go batcher.Run(100 * time.Millisecond)
			}
		}

		return n.Reply(msg, map[string]any{
			"type": "topology_ok",
		})
	})

	n.Handle("broadcast_batch", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		for _, message := range body["messages"].([]any) {
			message := int64(message.(float64))
			messages.Add(message)
			for node, batcher := range batchers {
				if node == msg.Src {
					continue
				}
				go PropagateSingleAndRetry(batcher, message, nil)
			}
		}
		return n.Reply(msg, map[string]any{
			"type": "broadcast_batch_ok",
		})
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		message := int64(body["message"].(float64))

		if !messages.Contains(message) {
			messages.Add(message)
			var wg sync.WaitGroup
			for _, batcher := range batchers {
				wg.Add(1)
				go PropagateSingleAndRetry(batcher, message, &wg)
			}
			wg.Wait()
		}

		return n.Reply(msg, map[string]any{
			"type": "broadcast_ok",
		})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		return n.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": messages.AllItems(),
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func PropagateSingleAndRetry(batcher *Batcher, message int64, wg *sync.WaitGroup) {
	first_try := wg != nil
	for {
		err := batcher.Process(message)
		if first_try {
			first_try = false
			wg.Done()
		}
		if err == nil {
			return
		}
	}
}

func PropagateBatchFn(n *maelstrom.Node, node string, timeout time.Duration) func([]int64) error {
	return func(messages []int64) error {
		ctx := context.Background()
		ctx, cancel_ctx := context.WithDeadline(ctx, time.Now().Add(timeout))
		defer cancel_ctx()

		body := map[string]any{
			"type":     "broadcast_batch",
			"messages": messages,
		}
		_, err := n.SyncRPC(ctx, node, body)
		return err
	}
}
