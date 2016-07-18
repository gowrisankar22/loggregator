package metrics_test

import (
	dopplerconfig "doppler/config"
	"doppler/dopplerservice"
	"fmt"
	"integration_tests/metron/metrics"
	"log"
	"math/rand"
	"metron/config"
	"net"
	"time"

	"github.com/cloudfoundry/gosteno"
	"github.com/cloudfoundry/sonde-go/events"
	"github.com/gogo/protobuf/proto"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var msg []byte

func init() {
	for i := 0; i < 1000; i++ {
		msg = append(msg, 'a')
	}
}

var _ = FDescribe("DroppedMessages", func() {
	// build/run metron
	// start the fake doppler
	// write to metron (forward to fake doppler)
	// stop reading in fake doppler
	// start reading in fake doppler
	// see messages about dropped messages

	var (
		testDoppler  *metrics.TCPTestDoppler
		metronInput  net.Conn
		stopAnnounce chan (chan bool)
	)
	// THINK ABOUT MAKING TEST_TCP_DOPPLER AND TEST_UDP_DOPPLER
	BeforeEach(func() {
		dopplerPort := rand.Intn(55536) + 10000
		dopplerAddr := fmt.Sprintf("127.0.0.1:%d", dopplerPort)
		testDoppler = metrics.NewTCPTestDoppler(dopplerAddr)
		go testDoppler.Start()

		dopplerConfig := &dopplerconfig.Config{
			Index:           "0",
			JobName:         "job",
			Zone:            "z9",
			IncomingTCPPort: uint32(dopplerPort),
		}

		stopAnnounce = dopplerservice.Announce("127.0.0.1", time.Minute, dopplerConfig, etcdAdapter, gosteno.NewLogger("test"))

		metronRunner.Protocols = []config.Protocol{"tcp"}
		metronRunner.Start()

		var err error
		metronInput, err = net.Dial("udp4", metronRunner.MetronAddress())
		Expect(err).ToNot(HaveOccurred())

	})

	AfterEach(func() {
		metronInput.Close()
		testDoppler.Stop()
		close(stopAnnounce)
		metronRunner.Stop()
	})

	It("emits logs when it drops messages due to a slow doppler", func() {

		go func() {
			for i := 0; i < 1000; i++ {
				writeToMetron(metronInput)
			}
		}()

		envelopes := []*events.Envelope{}

		timer := time.NewTimer(10 * time.Second)

	loop:
		for {
			select {
			case <-timer.C:
				break loop
			case envelope := <-testDoppler.MessageChan:
				envelopes = append(envelopes, envelope)
			}
		}

		var found *events.Envelope
		for _, ev := range envelopes {
			if ev.GetEventType() == events.Envelope_CounterEvent && ev.CounterEvent.GetName() == "MessageBuffer.droppedMessageCount" {
				found = ev
				break
			}
		}
		Expect(found).ToNot(BeNil())
		log.Printf("Found dropped count %#v", found)
		//
		//		Expect(envelopes).To(HaveLen(1000))
		//
		//		Eventually(testDoppler.MessageChan).Should(Receive(MatchSpecifiedContents(&events.Envelope{
		//			EventType: events.Envelope_CounterEvent.Enum(),
		//			CounterEvent: &events.CounterEvent{
		//				Name: proto.String("MessageBuffer.droppedMessageCount"),
		//			},
		//		})))
	})
})

func writeToMetron(metronInput net.Conn) error {
	dropsondeMessage := &events.Envelope{
		Origin:    proto.String("fake-origin-2"),
		EventType: events.Envelope_LogMessage.Enum(),
		LogMessage: &events.LogMessage{
			//Message:     []byte("some test log message"),
			Message:     msg,
			MessageType: events.LogMessage_ERR.Enum(),
			Timestamp:   proto.Int64(1),
		},
	}
	message, err := proto.Marshal(dropsondeMessage)
	if err != nil {
		log.Printf("Got error %s", err)
		return err
	}
	_, err = metronInput.Write(message)
	return err
}
