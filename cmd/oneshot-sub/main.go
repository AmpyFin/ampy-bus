package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/AmpyFin/ampy-bus/pkg/ampybus"
	"github.com/AmpyFin/ampy-bus/pkg/ampybus/natsbinding"
)

type connOpts struct {
	natsURL               string
	user, pass, token     string
	tlsCA, tlsCert, tlsKey string
	tlsInsecure           bool
}

func addConnFlags(fs *flag.FlagSet) *connOpts {
	co := &connOpts{}
	fs.StringVar(&co.natsURL, "nats", getenv("NATS_URL", "nats://127.0.0.1:4222"), "NATS URL")
	fs.StringVar(&co.user, "user", "", "username")
	fs.StringVar(&co.pass, "pass", "", "password")
	fs.StringVar(&co.token, "token", "", "auth token")
	fs.StringVar(&co.tlsCA, "tls-ca", "", "TLS CA file")
	fs.StringVar(&co.tlsCert, "tls-cert", "", "TLS client cert")
	fs.StringVar(&co.tlsKey, "tls-key", "", "TLS client key")
	fs.BoolVar(&co.tlsInsecure, "tls-insecure", false, "skip TLS verify (NOT for prod)")
	return co
}

func makeConfig(co *connOpts) natsbinding.Config {
	var tlsCfg *natsbinding.TLSConfig
	if co.tlsCA != "" || (co.tlsCert != "" && co.tlsKey != "") || co.tlsInsecure {
		tlsCfg = &natsbinding.TLSConfig{
			CAFile:             co.tlsCA,
			CertFile:           co.tlsCert,
			KeyFile:            co.tlsKey,
			InsecureSkipVerify: co.tlsInsecure,
		}
	}
	return natsbinding.Config{
		URLs:       co.natsURL,
		StreamName: "AMPY",
		Subjects:   []string{"ampy.>"},
		Auth: natsbinding.AuthConfig{
			Username: co.user, Password: co.pass, Token: co.token,
		},
		TLS: tlsCfg,
	}
}

func main() {
	fs := flag.NewFlagSet("oneshot-sub", flag.ExitOnError)
	subject := fs.String("subject", "ampy.>", "subject pattern")
	timeout := fs.Duration("timeout", 5*time.Second, "overall timeout")
	co := addConnFlags(fs)
	fs.Parse(os.Args[1:])

	cfg := makeConfig(co)
	bus, err := natsbinding.Connect(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect: %v\n", err)
		os.Exit(2)
	}
	defer bus.Close()
	if err := bus.EnsureStream(); err != nil {
		fmt.Fprintf(os.Stderr, "ensure stream: %v\n", err)
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	done := make(chan struct{}, 1)
	_, err = bus.SubscribePull(*subject, "oneshot", func(ctx context.Context, env ampybus.Envelope) error {
		raw, _ := ampybus.DecodePayload(env.Payload, env.Headers.ContentEncoding)
		fmt.Printf("[go-consume] subject=%s msg_id=%s schema=%s size=%d decoded=%d pk=%s\n",
			env.Topic, env.Headers.MessageID, env.Headers.SchemaFQDN, len(env.Payload), len(raw), env.Headers.PartitionKey)
		done <- struct{}{}
		return nil
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "sub: %v\n", err)
		os.Exit(2)
	}

	select {
	case <-done:
		os.Exit(0)
	case <-ctx.Done():
		fmt.Fprintln(os.Stderr, "timeout waiting for message")
		os.Exit(3)
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); strings.TrimSpace(v) != "" {
		return v
	}
	return def
}
