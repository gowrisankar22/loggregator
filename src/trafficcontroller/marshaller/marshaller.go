package marshaller

import (
	"time"

	"github.com/cloudfoundry/dropsonde/emitter"
	"github.com/cloudfoundry/sonde-go/events"
	"github.com/gogo/protobuf/proto"
)

type MessageGenerator func(string, string) []byte

func DropsondeLogMessage(messageString string, appId string) []byte {
	currentTime := time.Now()
	logMessage := &events.LogMessage{
		Message:     []byte(messageString),
		MessageType: events.LogMessage_ERR.Enum(),
		Timestamp:   proto.Int64(currentTime.UnixNano()),
		SourceType:  proto.String("DOP"),
		AppId:       &appId,
	}

	envelope, _ := emitter.Wrap(logMessage, "doppler")

	msg, _ := proto.Marshal(envelope)
	return msg
}
