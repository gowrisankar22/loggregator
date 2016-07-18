package metrics

import (
	"encoding/binary"
	"net"
	"time"

	"github.com/cloudfoundry/sonde-go/events"
	"github.com/gogo/protobuf/proto"
)

type TCPTestDoppler struct {
	listener     net.Listener
	readMessages uint32
	MessageChan  chan *events.Envelope
}

func NewTCPTestDoppler(address string) *TCPTestDoppler {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		panic(err)
	}
	return &TCPTestDoppler{
		listener:    listener,
		MessageChan: make(chan *events.Envelope),
	}
}

func (d *TCPTestDoppler) Start() {
	for {
		conn, err := d.listener.Accept()
		if err != nil {
			return
		}
		d.readFromConn(conn)
	}
}

func (d *TCPTestDoppler) Stop() {
	d.listener.Close()
}

func (d *TCPTestDoppler) readFromConn(conn net.Conn) {
	for {
		println("Reading in test doppler")
		var size uint32
		err := binary.Read(conn, binary.LittleEndian, &size)
		if err != nil {
			return
		}

		// slow down reading after peeking at the size
		time.Sleep(500 * time.Millisecond)

		buff := make([]byte, size)
		readCount, err := conn.Read(buff)
		if err != nil || readCount != int(size) {
			return
		}
		var env events.Envelope
		if err := proto.Unmarshal(buff, &env); err != nil {
			return
		}
		d.MessageChan <- &env
		println("Sent information on doppler channel")
	}
}
