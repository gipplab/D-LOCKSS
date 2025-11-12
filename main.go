package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/libp2p/go-libp2p"
	// These import paths have changed from "go-libp2p-core"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
)

// TopicName is the name of the pubsub topic to join
const TopicName = "my-chat-topic"

// discoveryServiceTag is used to find other peers on the local network
const discoveryServiceTag = "p2p-chat-example"

func main() {
	// Create a context that gracefully handles termination signals
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- 1. Create a libp2p Host ---
	// Listen on any available network interface and port
	// (0.0.0.0 for IP4, :: for IP6, 0 for port)
	host, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0", "/ip6/::/tcp/0"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create libp2p host: %w", err))
	}

	// Print this node's multiaddresses
	fmt.Println("--- Your Node's Addresses ---")
	for _, addr := range host.Addrs() {
		fmt.Printf("%s/p2p/%s\n", addr, host.ID().String())
	}
	fmt.Println("-------------------------------")
	fmt.Println("Waiting for peers... (Run another instance of this app on your network)")

	// --- 2. Set up mDNS Discovery ---
	// mDNS is a zero-config discovery protocol for local networks
	// The `discoveryNotifee` struct now uses the imported `core/host` and `core/peer` types
	notifee := &discoveryNotifee{h: host, ctx: ctx}
	mdnsService := mdns.NewMdnsService(host, discoveryServiceTag, notifee)
	if err := mdnsService.Start(); err != nil {
		panic(fmt.Errorf("failed to start mDNS service: %w", err))
	}

	// --- 3. Set up GossipSub ---
	// NewGossipSub creates a pubsub router with the gossipsub protocol
	ps, err := pubsub.NewGossipSub(ctx, host)
	if err != nil {
		panic(fmt.Errorf("failed to create pubsub service: %w", err))
	}

	// --- 4. Join the Topic ---
	// "Joining" a topic really means we are subscribing to it
	topic, err := ps.Join(TopicName)
	if err != nil {
		panic(fmt.Errorf("failed to join topic: %w", err))
	}

	// --- 5. Subscribe to the Topic ---
	// Subscribing returns a subscription handle
	sub, err := topic.Subscribe()
	if err != nil {
		panic(fmt.Errorf("failed to subscribe to topic: %w", err))
	}

	// --- 6. Handle Incoming and Outgoing Messages ---
	// Start a goroutine to read and print messages from the subscription
	go readMessages(ctx, sub, host.ID())
	// Start a goroutine to read from stdin and publish messages
	go publishMessages(ctx, topic)

	// --- 7. Wait for Shutdown Signal ---
	// Wait for a termination signal (like Ctrl+C)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\nShutting down...")

	// Close the libp2p host
	if err := host.Close(); err != nil {
		fmt.Println("Error closing host:", err)
	}
	fmt.Println("Done.")
}

// readMessages loops, reads messages from the subscription, and prints them to the console.
func readMessages(ctx context.Context, sub *pubsub.Subscription, selfID peer.ID) {
	for {
		select {
		case <-ctx.Done(): // Check if the context is cancelled
			return
		default:
			msg, err := sub.Next(ctx)
			if err != nil {
				// Check if the context was cancelled, which is a normal shutdown
				if ctx.Err() == context.Canceled {
					return
				}
				fmt.Fprintln(os.Stderr, "Error reading from subscription:", err)
				return
			}

			// Don't print our own messages
			if msg.GetFrom() == selfID {
				continue
			}

			// Print the message
			// \r moves the cursor to the start of the line
			// \n moves to the next line
			// This makes incoming messages play nice with the input prompt
			fmt.Printf("\r[%s]: %s\n> ", msg.GetFrom().ShortString(), string(msg.Data))
		}
	}
}

// publishMessages loops, reads lines from stdin, and publishes them to the topic.
func publishMessages(ctx context.Context, topic *pubsub.Topic) {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("> ") // Initial prompt

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			fmt.Print("> ") // Reprint prompt on empty line
			continue
		}

		// Publish the message
		if err := topic.Publish(ctx, []byte(line)); err != nil {
			fmt.Fprintln(os.Stderr, "Error publishing message:", err)
		}
		fmt.Print("> ") // Reprint prompt after sending
	}

	if err := scanner.Err(); err != nil {
		if ctx.Err() != context.Canceled {
			fmt.Fprintln(os.Stderr, "Error reading from stdin:", err)
		}
	}
}

// discoveryNotifee implements the mdns.Notifee interface
// It's used to handle peers discovered via mDNS
type discoveryNotifee struct {
	h   host.Host // This type is now from `github.com/libp2p/go-libp2p/core/host`
	ctx context.Context
}

// HandlePeerFound is called when a new peer is found
func (n *discoveryNotifee) HandlePeerFound(pi peer.AddrInfo) { // This type is from `github.com/libp2p/go-libp2p/core/peer`
	// \r and > for the same reason as in readMessages
	fmt.Printf("\rDiscovered new peer: %s\n> ", pi.ID.String())

	// Connect to the newly discovered peer
	if err := n.h.Connect(n.ctx, pi); err != nil {
		fmt.Printf("\rError connecting to peer %s: %s\n> ", pi.ID.String(), err)
	} else {
		fmt.Printf("\rConnected to: %s\n> ", pi.ID.String())
	}
}
