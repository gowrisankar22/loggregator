package batch

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"sync"
	"time"

	"github.com/cloudfoundry/dropsonde/metricbatcher"
	"github.com/cloudfoundry/dropsonde/metrics"
	"github.com/cloudfoundry/gosteno"
)

const (
	maxOverflowTries  = 1
	minBufferCapacity = 1024
)

type messageBuffer struct {
	bytes.Buffer
	messages uint64
}

func newMessageBuffer(bufferBytes []byte) *messageBuffer {
	child := bytes.NewBuffer(bufferBytes)
	return &messageBuffer{
		Buffer: *child,
	}
}

// Write writes msg to b and increments b.messages.
func (b *messageBuffer) Write(msg []byte) (int, error) {
	b.messages++
	return b.Buffer.Write(msg)
}

// writeNonMessage is provided as a method to write bytes without
// incrementing b.messages.
func (b *messageBuffer) writeNonMessage(msg []byte) (int, error) {
	return b.Buffer.Write(msg)
}

func (b *messageBuffer) Reset() {
	b.messages = 0
	b.Buffer.Reset()
}

//go:generate hel --type BatchChainByteWriter --output mock_byte_writer_test.go

type BatchChainByteWriter interface {
	Write(message []byte, chainers ...metricbatcher.BatchCounterChainer) (sentLength int, err error)
}

//go:generate hel --type DroppedMessageCounter --output mock_dropped_message_counter_test.go

type DroppedMessageCounter interface {
	Drop(count uint32)
}

type Writer struct {
	flushDuration  time.Duration
	outWriter      BatchChainByteWriter
	writerLock     sync.Mutex
	msgBuffer      *messageBuffer
	msgBufferLock  sync.Mutex
	timer          *time.Timer
	logger         *gosteno.Logger
	droppedCounter DroppedMessageCounter
	chainers       []metricbatcher.BatchCounterChainer
	protocol       string
}

func NewWriter(protocol string, writer BatchChainByteWriter, droppedCounter DroppedMessageCounter, bufferCapacity uint64, flushDuration time.Duration, logger *gosteno.Logger) (*Writer, error) {
	if bufferCapacity < minBufferCapacity {
		return nil, fmt.Errorf("batch.Writer requires a buffer of at least %d bytes", minBufferCapacity)
	}

	// Initialize the timer with a long duration so we can stop it before
	// it triggers.  Ideally, we'd initialize the timer without starting
	// it, but that doesn't seem possible in the current library.
	batchTimer := time.NewTimer(time.Second)
	batchTimer.Stop()
	batchWriter := &Writer{
		// flushDuration:  flushDuration,
		flushDuration:  10 * time.Minute, // don't commit this, but try and take timed flushing out of the equation while spiking
		outWriter:      writer,
		droppedCounter: droppedCounter,
		msgBuffer:      newMessageBuffer(make([]byte, 0, bufferCapacity)),
		timer:          batchTimer,
		logger:         logger,
		protocol:       protocol,
	}
	go batchWriter.flushOnTimer()
	return batchWriter, nil
}

func (w *Writer) Write(msgBytes []byte, chainers ...metricbatcher.BatchCounterChainer) (int, error) {
	w.msgBufferLock.Lock()
	defer w.msgBufferLock.Unlock()

	w.chainers = append(w.chainers, chainers...)

	prefixedBytes := prefixMessage(msgBytes)
	switch {
	// we want to be able to continue to service incomming messages received on UDP
	// so empty the buffer at the same time the message is sent such that there is space for new messages
	case w.msgBuffer.Len()+len(prefixedBytes) > w.msgBuffer.Cap():
		count := w.msgBuffer.messages + 1 // don't forget the message we're trying to write now
		allMsgs, err := ioutil.ReadAll(w.msgBuffer)
		if err != nil {
			return 0, err
		}

		allMsgs = append(allMsgs, prefixedBytes...)

		w.msgBuffer.Reset()
		chainers := w.chainers
		w.chainers = nil

		go func() {
			_, retryErr := w.retryWrites(allMsgs, count, chainers...)
			if retryErr != nil {
				w.droppedCounter.Drop(uint32(count))
				fmt.Sprintf("dropped: %d messages", count)
			}
		}()
		return len(msgBytes), nil
	default:
		if w.msgBuffer.Len() == 0 {
			w.timer.Reset(w.flushDuration)
		}
		_, err := w.msgBuffer.Write(prefixedBytes)
		return len(msgBytes), err
	}
}

func (w *Writer) Stop() {
	w.msgBufferLock.Lock()
	defer w.msgBufferLock.Unlock()
	w.timer.Stop()
}

func (w *Writer) flushWrite(toWrite []byte, messageCount uint64, chainers ...metricbatcher.BatchCounterChainer) (int, error) {
	w.writerLock.Lock()
	defer w.writerLock.Unlock()

	println("Writing")
	sent, err := w.outWriter.Write(toWrite, chainers...)
	if err != nil {
		println("Got error", err.Error())
		panic(err)
		//w.logger.Warnf("Received error while trying to flush TCP bytes: %s", err)
		//return 0, err
	}
	println("Successfully wrote")

	metrics.BatchAddCounter("DopplerForwarder.sentMessages", messageCount)
	metrics.BatchAddCounter(w.protocol+".sentMessageCount", messageCount)
	return sent, nil
}

func (w *Writer) flushOnTimer() {
	for range w.timer.C {
		w.flushBuffer()
	}
}

func (w *Writer) flushBuffer() {
	w.msgBufferLock.Lock()
	defer w.msgBufferLock.Unlock()
	if w.msgBuffer.Len() == 0 {
		return
	}
	count := w.msgBuffer.messages
	messages, err := ioutil.ReadAll(w.msgBuffer)
	if err != nil {
		return
	}
	if _, err := w.flushWrite(messages, count, w.chainers...); err != nil {
		metrics.BatchIncrementCounter("DopplerForwarder.retryCount")
		w.timer.Reset(w.flushDuration)
		w.msgBuffer.Write(messages)
		return
	}
	w.msgBuffer.Reset()
	w.chainers = nil
}

func (w *Writer) retryWrites(message []byte, messageCount uint64, chainers ...metricbatcher.BatchCounterChainer) (sent int, err error) {
	for i := 0; i < maxOverflowTries; i++ {
		if i > 0 {
			metrics.BatchIncrementCounter("DopplerForwarder.retryCount")
		}
		sent, err = w.flushWrite(message, messageCount, chainers...)
		if err == nil {
			return sent, nil
		}
	}
	return 0, err
}

func prefixMessage(msgBytes []byte) []byte {
	prefixed := make([]byte, 4, len(msgBytes)+4)
	binary.LittleEndian.PutUint32(prefixed, uint32(len(msgBytes)))
	return append(prefixed, msgBytes...)
}
