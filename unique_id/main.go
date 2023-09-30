package main

import (
	"sync/atomic"
	"encoding/json"
	"log"
	"fmt"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	var counter int64 = 0
	n := maelstrom.NewNode()
	n.ID()
	n.Handle("generate", func (msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		body["type"] = "generate_ok"
		id := atomic.AddInt64(&counter, 1)
		body["id"] = fmt.Sprintf("%s_%d", n.ID(), id)

		return n.Reply(msg, body)
	})
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}