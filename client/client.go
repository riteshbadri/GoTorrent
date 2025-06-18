package client

import (
	"bytes"
	"fmt"
	"goTorrent/bitfield"
	"goTorrent/handshake"
	"goTorrent/message"
	"goTorrent/peers"
	"net"
	"time"
)

type Client struct {
	Conn     net.Conn
	Choked   bool
	Bitfield bitfield.Bitfield
	Peer     peers.Peer
	infoHash [20]byte
	peerID   [20]byte
}

func completeHandshake(conn net.Conn, infohash, peerID [20]byte) (*handshake.Handshake, error) {
	err := conn.SetDeadline(time.Now().Add(3 * time.Second))
	if err != nil {
		return nil, err
	}
	defer func(conn net.Conn, t time.Time) {
		err := conn.SetDeadline(t)
		if err != nil {

		}
	}(conn, time.Time{}) // Disable the deadline

	req := handshake.New(infohash, peerID)
	_, err = conn.Write(req.Serialize())
	if err != nil {
		return nil, err
	}

	res, err := handshake.Read(conn)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(res.InfoHash[:], infohash[:]) {
		return nil, fmt.Errorf("Expected infohash %x but got %x", res.InfoHash, infohash)
	}
	return res, nil
}

func recvBitfield(conn net.Conn) (bitfield.Bitfield, error) {
	err := conn.SetDeadline(time.Now().Add(5 * time.Second))
	if err != nil {
		return nil, err
	}
	defer func(conn net.Conn, t time.Time) {
		err := conn.SetDeadline(t)
		if err != nil {

		}
	}(conn, time.Time{}) // Disable the deadline

	msg, err := message.Read(conn)
	if err != nil {
		return nil, err
	}
	if msg == nil {
		err := fmt.Errorf("Expected bitfield but got %s", msg)
		return nil, err
	}
	if msg.ID != message.MsgBitfield {
		err := fmt.Errorf("Expected bitfield but got ID %d", msg.ID)
		return nil, err
	}

	return msg.Payload, nil
}

// New connects with a peer, completes a handshake, and receives a handshake
// returns an err if any of those fail.
func New(peer peers.Peer, peerID, infoHash [20]byte) (*Client, error) {
	conn, err := net.DialTimeout("tcp", peer.String(), 3*time.Second)
	if err != nil {
		return nil, err
	}

	_, err = completeHandshake(conn, infoHash, peerID)
	if err != nil {
		err := conn.Close()
		if err != nil {
			return nil, err
		}
		return nil, err
	}

	bf, err := recvBitfield(conn)
	if err != nil {
		err := conn.Close()
		if err != nil {
			return nil, err
		}
		return nil, err
	}

	return &Client{
		Conn:     conn,
		Choked:   true,
		Bitfield: bf,
		Peer:     peer,
		infoHash: infoHash,
		peerID:   peerID,
	}, nil
}

// Read reads and consumes a message from the connection
func (c *Client) Read() (*message.Message, error) {
	msg, err := message.Read(c.Conn)
	return msg, err
}

// SendRequest sends a Request message to the peer
func (c *Client) SendRequest(index, begin, length int) error {
	req := message.FormatRequest(index, begin, length)
	_, err := c.Conn.Write(req.Serialize())
	return err
}

// SendInterested sends an Interested message to the peer
func (c *Client) SendInterested() error {
	msg := message.Message{ID: message.MsgInterested}
	_, err := c.Conn.Write(msg.Serialize())
	return err
}

// SendNotInterested sends a NotInterested message to the peer
func (c *Client) SendNotInterested() error {
	msg := message.Message{ID: message.MsgNotInterested}
	_, err := c.Conn.Write(msg.Serialize())
	return err
}

// SendUnchoke sends an Unchoke message to the peer
func (c *Client) SendUnchoke() error {
	msg := message.Message{ID: message.MsgUnchoke}
	_, err := c.Conn.Write(msg.Serialize())
	return err
}

// SendHave sends a Have message to the peer
func (c *Client) SendHave(index int) error {
	msg := message.FormatHave(index)
	_, err := c.Conn.Write(msg.Serialize())
	return err
}
