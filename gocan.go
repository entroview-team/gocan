package gocan

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const (
	CR = 0x0D
)

type Adapter interface {
	Init(context.Context) error
	Name() string
	Recv() <-chan CANFrame
	Send() chan<- CANFrame
	Close() error
	SetFilter([]uint32) error
}

type AdapterConfig struct {
	Debug         bool
	Port          string
	PortBaudrate  int
	CANRate       float64
	CANFilter     []uint32
	UseExtendedID bool
	PrintVersion  bool
	OnMessage     func(string)
	OnError       func(error)
}

type Opts func(*Client)

func OptOnIncoming(fn func(CANFrame)) Opts {
	return func(c *Client) {
		c.fh.setOnIncoming(fn)
	}
}

func OptOnOutgoing(fn func(CANFrame)) Opts {
	return func(c *Client) {
		c.fh.setOnOutgoing(fn)
	}
}

type Client struct {
	fh      *FrameHandler
	adapter Adapter
}

func New(ctx context.Context, adapter Adapter) (*Client, error) {
	return NewWithOpts(ctx, adapter)
}

func NewWithOpts(ctx context.Context, adapter Adapter, opts ...Opts) (*Client, error) {
	if err := adapter.Init(ctx); err != nil {
		return nil, err
	}
	c := &Client{
		fh:      newFrameHandler(adapter.Recv()),
		adapter: adapter,
	}

	for _, opt := range opts {
		opt(c)
	}

	go c.fh.run(ctx)
	return c, nil
}

func (c *Client) Adapter() Adapter {
	return c.adapter
}

func (c *Client) SetFilter(filters []uint32) error {
	return c.adapter.SetFilter(filters)
}

func (c *Client) Close() error {
	c.fh.Close()
	return c.adapter.Close()
}

// Send a CAN Frame
func (c *Client) Send(msg CANFrame) error {
	select {
	case c.adapter.Send() <- msg:
		if c.fh.onOutgoing != nil {
			c.fh.onOutgoing(msg)
		}
		return nil
	default:
		return errors.New("gocan failed to send frame")
	}
}

// Shortcommand to send a standard 11bit frame
func (c *Client) SendFrame(identifier uint32, data []byte, f CANFrameType) error {
	var b = make([]byte, len(data))
	copy(b, data)
	frame := NewFrame(identifier, b, f)
	return c.Send(frame)
}

// Send and wait up to <timeout> for a answer on given identifiers
func (c *Client) SendAndPoll(ctx context.Context, frame CANFrame, timeout time.Duration, identifiers ...uint32) (CANFrame, error) {
	frame.SetTimeout(timeout)
	p := c.newSub(ctx, 1, identifiers...)
	c.fh.register <- p
	defer func() {
		c.fh.unregister <- p
	}()
	if err := c.Send(frame); err != nil {
		return nil, err
	}
	return p.Wait(ctx, timeout)
}

// Send and wait up to <timeout> for a answer on given identifiers, retries <retries> times only if error is a TimeoutError
func (c *Client) SendAndPollWithRetries(ctx context.Context, frame CANFrame, timeout time.Duration, retries int64, identifiers ...uint32) (CANFrame, error) {
	var lastErr error

	for attempt := int64(0); attempt <= retries; attempt++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		resp, err := c.SendAndPoll(ctx, frame, timeout, identifiers...)
		if err == nil {
			return resp, nil
		}

		// Do not retry if context was canceled or timed out
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}

		lastErr = err
		var timeoutErr *TimeoutError
		// Retry only if error is a TimeoutError and max retries not reached
		if attempt == retries || !errors.As(err, &timeoutErr) {
			break
		}
	}

	return nil, fmt.Errorf("SendAndPoll failed after %d attempts:  %w", retries+1, lastErr)
}

// Poll for a certain CAN identifier for up to <timeout>
func (c *Client) Poll(ctx context.Context, timeout time.Duration, identifiers ...uint32) (CANFrame, error) {
	p := c.newSub(ctx, 1, identifiers...)
	c.fh.register <- p
	defer func() {
		c.fh.unregister <- p
	}()
	return p.Wait(ctx, timeout)
}

// Subscribe to CAN identifiers and return a message channel
func (c *Client) Subscribe2(ctx context.Context, identifiers ...uint32) chan CANFrame {
	p := c.newSub(ctx, 10, identifiers...)
	c.fh.register <- p
	return p.callback
}

// Subscribe to CAN identifiers and return a message channel
func (c *Client) Subscribe(ctx context.Context, identifiers ...uint32) *Sub {
	p := c.newSub(ctx, 10, identifiers...)
	c.fh.register <- p
	return p
}

func (c *Client) newSub(ctx context.Context, bufferSize int, identifiers ...uint32) *Sub {
	return &Sub{
		ctx:         ctx,
		c:           c,
		identifiers: identifiers,
		callback:    make(chan CANFrame, bufferSize),
	}
}
