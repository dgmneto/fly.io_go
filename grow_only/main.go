package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	var counter int64 = 0
	delta_chan := make(chan int64)

	others_counters := make(map[string]int64)

	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	go func() {
		for {
			delta := <-delta_chan
			counter += delta
			kv.Write(context.Background(), n.ID(), counter)
		}
	}()

	go func() {
		for {
			time.Sleep(time.Second)
			results := map[string]chan int64 {}
			var wg sync.WaitGroup
			for _, node := range n.NodeIDs() {
				node := node
				if node == n.ID() {
					continue
				}
				wg.Add(1)

				results[node] = make(chan int64, 1)
				go func() {
					defer (&wg).Done()
					node_val, _ := kv.ReadInt(context.Background(), node)
					results[node] <- int64(node_val)
					close(results[node])
				}()
			}
			wg.Wait()
			for node, res_chan := range results {
				for node_val := range res_chan {
					others_counters[node] = node_val
				}
			}
		}
	}()

	n.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		delta := int(body["delta"].(float64))
		delta_chan <- int64(delta)

		return n.Reply(msg, map[string]any{
			"type": "add_ok",
		})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		total_val := counter
		for _, other_val := range others_counters {
			total_val += other_val
		}

		return n.Reply(msg, map[string]any{
			"type":  "read_ok",
			"value": total_val,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
