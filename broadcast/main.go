package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	messages := make(map[int64]string)
	// var nodes []string

	n.Handle("topology", func (msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		
		// topology := body["topology"].(map[string][]string)
		// nodes = topology[n.ID()]

		return n.Reply(msg, map[string]any {
			"type": "topology_ok",
		})
	})

	n.Handle("broadcast", func (msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		
		message := body["message"].(float64)
		messages[int64(message)] = n.ID()

		return n.Reply(msg, map[string]any {
			"type": "broadcast_ok",
		})
	})

	n.Handle("read", func (msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		
		messages_res := make([]int64, len(messages))
		i := 0
		for message := range messages {
			messages_res[i] = message
			i += 1
		}

		return n.Reply(msg, map[string]any {
			"type": "read_ok",
			"messages": messages_res,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}