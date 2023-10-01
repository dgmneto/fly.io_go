package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)



func main() {
	n := maelstrom.NewNode()
	messages := NewConcurrentSet()
	var nodes []string

	n.Handle("topology", func (msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		
		topology := body["topology"].(map[string]any)
		for _,node := range topology[n.ID()].([]any) {
			nodes = append(nodes, node.(string))
		}

		return n.Reply(msg, map[string]any {
			"type": "topology_ok",
		})
	})

	n.Handle("broadcast", func (msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		
		message := int64(body["message"].(float64))

		if !messages.Contains(message) {
			messages.Add(message)
			var wg sync.WaitGroup
			for _, node := range nodes {
				node := node
				wg.Add(1)
				go Propagate(n, node, body, &wg)
			}
		}

		return n.Reply(msg, map[string]any {
			"type": "broadcast_ok",
		})
	})

	n.Handle("read", func (msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		return n.Reply(msg, map[string]any {
			"type": "read_ok",
			"messages": messages.AllItems(),
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func Propagate(n *maelstrom.Node, node string, body map[string]any, wg *sync.WaitGroup) {
	done := make(chan bool)
	first_try := true
	for {
		n.RPC(node, body, func (msg maelstrom.Message) error {
			done <- true
			return nil
		})
		var success bool
		select {
		case <-done:
			success = true
		case <-time.After(150 * time.Millisecond):
			success = false
		}
		if first_try {
			first_try = false
			wg.Done()
		}
		if success {
			return
		}
	}
}