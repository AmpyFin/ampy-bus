package main

import (
	"context"
	"fmt"
	"time"

	"github.com/AmpyFin/ampy-bus/pkg/ampybus"
	"github.com/AmpyFin/ampy-bus/pkg/ampybus/natsbinding"
	"github.com/nats-io/nats.go"
)

func main() {
	bus, err := natsbinding.Connect(natsbinding.Config{
		URLs:          "",
		StreamName:    "AMPY",
		Subjects:      []string{"ampy.>"},
		DurablePrefix: "replayer",
	})
	if err != nil { panic(err) }
	defer bus.Close()
	if err := bus.EnsureStream(); err != nil { panic(err) }

	js := bus.JS()

	// Snapshot the stream tail so we never process messages produced after we start (avoids replay loop).
	si, err := js.StreamInfo("AMPY")
	if err != nil { panic(err) }
	tailAtStart := si.State.LastSeq

	ctrlDurable := "replayer-ctrl"
	ctrlSub, err := js.PullSubscribe("ampy.prod.control.v1.replay_requests", ctrlDurable, nats.BindStream("AMPY"))
	if err != nil { panic(err) }
	fmt.Println("[replayer] listening on ampy.prod.control.v1.replay_requests (durable=replayer-ctrl)")

	for {
		msgs, err := ctrlSub.Fetch(10, nats.MaxWait(2*time.Second))
		if err == nats.ErrTimeout {
			continue
		}
		if err != nil {
			fmt.Printf("[replayer] fetch control error: %v\n", err)
			continue
		}
		for _, m := range msgs {
			subject := m.Header.Get(ampybus.CtrlSubjectKey)
			if subject == "" {
				envName := "prod"
				subject = fmt.Sprintf("ampy.%s.%s.%s.%s", envName,
					m.Header.Get(ampybus.CtrlDomainKey),
					m.Header.Get(ampybus.CtrlVersionKey),
					m.Header.Get(ampybus.CtrlSubtopicKey),
				)
			}
			startStr := m.Header.Get(ampybus.CtrlStartKey)
			endStr := m.Header.Get(ampybus.CtrlEndKey)
			reqID := m.Header.Get(ampybus.CtrlRequestIDKey)

			start, err := time.Parse(time.RFC3339, startStr)
			if err != nil { _ = m.Ack(); fmt.Printf("[replayer] bad start: %v\n", err); continue }
			end, err := time.Parse(time.RFC3339, endStr)
			if err != nil { _ = m.Ack(); fmt.Printf("[replayer] bad end: %v\n", err); continue }
			if !start.Before(end) { _ = m.Ack(); fmt.Println("[replayer] start >= end"); continue }

			fmt.Printf("[replayer] request_id=%s subject=%s window=[%s, %s) tail_seq=%d\n", reqID, subject, startStr, endStr, tailAtStart)

			// Create a temporary consumer starting from start time
			durable := "replay_" + time.Now().UTC().Format("20060102T150405")
			_, err = js.AddConsumer("AMPY", &nats.ConsumerConfig{
				Durable:       durable,
				FilterSubject: subject,
				DeliverPolicy: nats.DeliverByStartTimePolicy,
				OptStartTime:  &start,
				AckPolicy:     nats.AckExplicitPolicy,
				ReplayPolicy:  nats.ReplayInstantPolicy,
			})
			if err != nil { _ = m.Ack(); fmt.Printf("[replayer] add consumer: %v\n", err); continue }

			sub, err := js.PullSubscribe(subject, durable, nats.BindStream("AMPY"))
			if err != nil {
				_ = m.Ack()
				fmt.Printf("[replayer] pull sub: %v\n", err)
				_ = js.DeleteConsumer("AMPY", durable)
				continue
			}

			republished := 0
			done := false
			for !done {
				batch, err := sub.Fetch(256, nats.MaxWait(2*time.Second))
				if err == nats.ErrTimeout {
					// If we're beyond end, stop.
					if time.Now().After(end.Add(2 * time.Second)) {
						break
					}
					continue
				}
				if err != nil { break }

				for _, item := range batch {
					// Skip anything that was already replayed (avoid self-loop).
					if item.Header.Get("replay_of_seq") != "" {
						_ = item.Ack()
						continue
					}

					meta, mdErr := item.Metadata()
					if mdErr == nil {
						// Hard stop at tail snapshot or end-time boundary.
						if meta.Sequence.Stream > tailAtStart {
							_ = item.Ack()
							done = true
							break
						}
						if meta.Timestamp.After(end) {
							_ = item.Ack()
							done = true
							break
						}
					}

					// Map message to Envelope and republish with replay markers.
					env, decErr := natsbinding.MessageToEnvelope(item)
					if decErr != nil {
						_ = item.Ack()
						continue
					}
					extra := map[string]string{
						"replay_of_stream":  "AMPY",
						"replay_of_seq":     fmt.Sprintf("%d", meta.Sequence.Stream),
						"replay_request_id": reqID,
					}
					ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
					_, pubErr := bus.PublishEnvelope(ctx2, env, extra)
					cancel2()
					if pubErr != nil {
						_ = item.Nak()
						continue
					}
					_ = item.Ack()
					republished++
				}
			}
			sub.Unsubscribe()
			js.DeleteConsumer("AMPY", durable)

			fmt.Printf("[replayer] completed: subject=%s republished=%d\n", subject, republished)
			_ = m.Ack()
		}
	}
}

