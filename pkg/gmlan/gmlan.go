package gmlan

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"time"

	"github.com/roffe/gocan"
	"github.com/roffe/gocan/pkg/frame"
)

type Client struct {
	c *gocan.Client
}

func New(c *gocan.Client) *Client {
	return &Client{
		c: c,
	}
}

func (cl *Client) DisableNormalCommunication(ctx context.Context) error {
	if err := cl.c.SendFrame(0x101, []byte{0xFE, 0x01, 0x28}, frame.Outgoing); err != nil { // DisableNormalCommunication Request Message
		return err
	}
	return nil
}

func (cl *Client) WriteDataByIdentifier(ctx context.Context, canID uint32, identifier byte, data []byte) error {
	r := bytes.NewReader(data)

	firstPart := make([]byte, 4)
	_, err := r.Read(firstPart)
	if err != nil {
		if err == io.EOF {
			// do nothing
		} else {
			return err
		}
	}
	payload := []byte{0x10, byte(len(data) + 2), 0x3B, identifier}
	payload = append(payload, firstPart...)
	//cl.c.SendFrame(canID, payload)
	//log.Printf("%X\n", payload)
	//resp, err := cl.c.Poll(ctx, 100*time.Millisecond, canID+0x400)
	fr := frame.New(canID, payload, frame.ResponseRequired)
	resp, err := cl.c.SendAndPoll(ctx, fr, 150*time.Millisecond, canID+0x400)
	if err != nil {
		return err
	}
	d := resp.Data()
	if d[0] != 0x30 || d[1] != 0x00 {
		log.Println(resp.String())
		return errors.New("invalid response to initial writeDataByIdentifier")
	}

	delay := d[2]

	var seq byte = 0x21

	for r.Len() > 0 {
		pkg := []byte{seq}
	inner:
		for i := 1; i < 8; i++ {
			b, err := r.ReadByte()
			if err != nil {
				if err == io.EOF {
					log.Println("eof")
					break inner
				}
				return err
			}
			pkg = append(pkg, b)
		}
		cl.c.SendFrame(canID, pkg, frame.Outgoing)
		log.Printf("%X\n", pkg)
		time.Sleep(time.Duration(delay) * time.Millisecond)
		seq++
		if seq == 0x30 {
			seq = 0x20
		}
	}

	return nil
}

func (cl *Client) ReadDataByIdentifier(ctx context.Context, canID uint32, identifier byte) ([]byte, error) {
	out := bytes.NewBuffer([]byte{})
	cl.c.SendFrame(canID, []byte{0x02, 0x1A, identifier}, frame.ResponseRequired)
	resp, err := cl.c.Poll(ctx, 100*time.Millisecond, canID+0x400)
	if err != nil {
		return nil, err
	}
	d := resp.Data()
	if d[3] == 0x78 {
		resp2, err := cl.c.Poll(ctx, 150*time.Millisecond, canID+0x400)
		if err != nil {
			return nil, err
		}
		d2 := resp2.Data()
		out.Write(d2[4:])
	} else {
		out.Write(d[4:])
	}

	left := int(d[1])
	left -= 6
	cl.c.SendFrame(canID, []byte{0x30, 0x00, 0x00}, frame.ResponseRequired)

outer:
	for left > 0 {
		read, err := cl.c.Poll(ctx, 100*time.Millisecond, canID+0x400)
		if err != nil {
			return nil, err
		}
		dr := read.Data()
		for _, b := range dr[1:] {
			out.WriteByte(b)
			left--
			if left == 0 {
				break outer
			}
		}

	}

	return out.Bytes(), nil
}
