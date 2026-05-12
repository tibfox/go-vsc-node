package chain

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/go-zeromq/zmq4"
)

// DashdZMQSubscriber connects to dashd's ZMQ pub-sub interface and exposes
// typed Go channels for each event topic. Designed for low-latency InstantSend
// awareness and mempool deposit detection on a pruned dashd node (where
// scantxoutset-only polling can't see mempool txs).
//
// Required dashd flags (already applied in magi/testnet/vsc-deployment):
//
//	-zmqpubrawtx=tcp://0.0.0.0:28332
//	-zmqpubrawtxlock=tcp://0.0.0.0:28332
//	-zmqpubhashblock=tcp://0.0.0.0:28332
//	-zmqpubrawchainlock=tcp://0.0.0.0:28332
//
// Slow consumers drop events rather than block the receiver loop — buffer
// size is 64 per channel.
type DashdZMQSubscriber struct {
	endpoint string

	// RawTx fires for every new mempool/block tx (topic `rawtx`).
	// IsLocked is always false on this channel.
	RawTx chan DashRawTxEvent
	// RawTxLock fires when dashd accepts an InstantSend lock for a tx
	// (topic `rawtxlock`). IsLocked is always true on this channel.
	RawTxLock chan DashRawTxEvent
	// HashBlock fires when dashd connects a new block (topic `hashblock`).
	HashBlock chan DashHashBlockEvent
	// RawChainLock fires when dashd receives a ChainLock signature for a
	// block (topic `rawchainlock`).
	RawChainLock chan []byte

	ctx    context.Context
	cancel context.CancelFunc
	sub    zmq4.Socket
	wg     sync.WaitGroup
}

// DashRawTxEvent is a tx notification from dashd's ZMQ pub-sub.
type DashRawTxEvent struct {
	// RawTx is the serialized transaction bytes. Decode with
	// wire.MsgTx{}.Deserialize(bytes.NewReader(RawTx)).
	RawTx []byte
	// IsLocked is true when this event came from the `rawtxlock` topic
	// (dashd has accepted an LLMQ InstantSend lock for this tx — finality
	// guarantee equivalent to 1 confirmation, usually within ~1–2 seconds
	// of broadcast).
	IsLocked bool
}

// DashHashBlockEvent fires when dashd connects a new block.
type DashHashBlockEvent struct {
	// Hash is the raw 32-byte block hash (NOT hex). Use chainhash.Hash for
	// pretty-printing or comparison.
	Hash []byte
}

// NewDashdZMQSubscriber dials a ZMQ pub-sub endpoint and starts a goroutine
// that demultiplexes incoming messages onto the typed channels. Cancel via
// the parent context or by calling Close().
func NewDashdZMQSubscriber(parent context.Context, endpoint string) (*DashdZMQSubscriber, error) {
	ctx, cancel := context.WithCancel(parent)
	sub := zmq4.NewSub(ctx)
	if err := sub.Dial(endpoint); err != nil {
		cancel()
		return nil, fmt.Errorf("dashd zmq dial %s: %w", endpoint, err)
	}
	for _, t := range []string{"rawtx", "rawtxlock", "hashblock", "rawchainlock"} {
		if err := sub.SetOption(zmq4.OptionSubscribe, t); err != nil {
			_ = sub.Close()
			cancel()
			return nil, fmt.Errorf("dashd zmq subscribe %s: %w", t, err)
		}
	}
	s := &DashdZMQSubscriber{
		endpoint:     endpoint,
		RawTx:        make(chan DashRawTxEvent, 64),
		RawTxLock:    make(chan DashRawTxEvent, 64),
		HashBlock:    make(chan DashHashBlockEvent, 64),
		RawChainLock: make(chan []byte, 64),
		ctx:          ctx,
		cancel:       cancel,
		sub:          sub,
	}
	s.wg.Add(1)
	go s.run()
	slog.Info("dashd zmq subscriber started", "endpoint", endpoint)
	return s, nil
}

// Close shuts the subscriber down and waits for its goroutine to exit.
func (s *DashdZMQSubscriber) Close() {
	s.cancel()
	if s.sub != nil {
		_ = s.sub.Close()
	}
	s.wg.Wait()
}

func (s *DashdZMQSubscriber) run() {
	defer s.wg.Done()
	for {
		msg, err := s.sub.Recv()
		if err != nil {
			if s.ctx.Err() != nil {
				return
			}
			slog.Warn("dashd zmq recv error", "endpoint", s.endpoint, "err", err)
			continue
		}
		if len(msg.Frames) < 2 {
			continue
		}
		topic := string(msg.Frames[0])
		body := msg.Frames[1]
		// Frame 2 (when present) is a 4-byte little-endian seqno — ignore.

		switch topic {
		case "rawtx":
			zmqSend(s.RawTx, DashRawTxEvent{RawTx: body, IsLocked: false}, topic)
		case "rawtxlock":
			zmqSend(s.RawTxLock, DashRawTxEvent{RawTx: body, IsLocked: true}, topic)
		case "hashblock":
			zmqSend(s.HashBlock, DashHashBlockEvent{Hash: body}, topic)
		case "rawchainlock":
			zmqSend(s.RawChainLock, body, topic)
		default:
			// Subscribed via prefix matching — could see unexpected topics if
			// dashd is reconfigured. Log + skip.
			slog.Debug("dashd zmq unknown topic", "topic", topic)
		}
	}
}

// zmqSend pushes an event to a channel non-blockingly. Drops on full buffer
// to keep the receiver loop healthy. Each drop is logged so we can spot
// consumers that fall behind.
func zmqSend[T any](ch chan T, ev T, topic string) {
	select {
	case ch <- ev:
	default:
		slog.Warn("dashd zmq channel full — dropping event", "topic", topic)
	}
}
