package logcounter

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
	"tools/logcounterapp/cflib"
	"tools/logcounterapp/config"

	"github.com/cloudfoundry/sonde-go/events"
)

const TOTAL_RETRY = 10

var (
	prefixEnd int
	guidEnd   int
	sepEnd    int
)

type Identity struct {
	appID string
	runID string
}

type logCounter struct {
	uaa         cflib.UAA
	cc          cflib.CC
	cfg         *config.Config
	server      *http.Server
	listener    net.Listener
	counterLock sync.Mutex
	counters    map[Identity]map[string]bool
}

//go:generate hel --type UAA --output mock_uaa_test.go

type UAA interface {
	GetAuthToken() (string, error)
}

//go:generate hel --type CC --output mock_cc_test.go

type CC interface {
	GetAppName(guid, authToken string) string
}

func New(uaa UAA, cc CC, cfg *config.Config) *logCounter {
	prefixEnd = len(cfg.MessagePrefix + " guid: ")
	guidEnd = prefixEnd + len("xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx")
	sepEnd = guidEnd + len(" msg: ")

	l := &logCounter{
		cfg:      cfg,
		counters: make(map[Identity]map[string]bool),
	}

	l.server = &http.Server{
		Handler: http.HandlerFunc(l.routeHandler),
	}

	return l
}

func (l *logCounter) Start() error {
	var err error
	l.listener, err = net.Listen("tcp", ":"+l.cfg.Port)
	if err != nil {
		return err
	}
	// this retry is to make sure the previous server session is terminated gracefully
	for retry := 0; retry < TOTAL_RETRY; retry++ {
		err = l.server.Serve(l.listener)
		if err == nil {
			break
		}
		time.Sleep(time.Millisecond * 500)
	}

	return err
}

func (l *logCounter) Stop() error {
	if err := l.listener.Close(); err != nil {
		return err
	}

	return nil
}

func (l *logCounter) HandleMessages(msgs <-chan *events.Envelope) {
	i := 0
	for msg := range msgs {
		i++
		if i%1000 == 0 {
			go fmt.Printf(".")
		}
		// do we need to spin up a go routine per msg
		go l.processEnvelope(msg)
	}
}

func (l *logCounter) routeHandler(rw http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		rw.WriteHeader(http.StatusNotFound)
		return
	}

	rw.WriteHeader(http.StatusOK)
	r.ParseForm()
	if _, ok := r.Form["report"]; ok {
		if err := l.dumpReport(rw); err != nil {
			log.Printf("Error dumping report: %s", err)
		}
	}
}

func (l *logCounter) dumpReport(w io.Writer) error {
	l.counterLock.Lock()
	defer l.counterLock.Unlock()
	authToken, err := l.uaa.GetAuthToken()
	if err != nil {
		authToken = ""
	}
	if _, err := io.WriteString(w, "Report:\n"); err != nil {
		return err
	}
	if len(l.counters) == 0 {
		_, err := io.WriteString(w, "No messages received")
		return err
	}
	for id, messages := range l.counters {
		var total, max int
		for msgID := range messages {
			msgMax, err := strconv.Atoi(msgID)
			if err != nil {
				if _, err := io.WriteString(w, fmt.Sprintf("Cannot parse message ID %s\n", msgID)); err != nil {
					return err
				}
				continue
			}
			if msgMax > max {
				max = msgMax
			}
			total++
		}
		msg := fmt.Sprintf("guid: %s app: %s total: %d max: %d\n", id.runID, l.cc.GetAppName(id.appID, authToken), total, max+1)
		if _, err := io.WriteString(w, msg); err != nil {
			return err
		}
	}
	return nil
}

func (l *logCounter) processEnvelope(env *events.Envelope) {
	if env.GetEventType() != events.Envelope_LogMessage {
		return
	}
	logMsg := env.GetLogMessage()

	msg := string(logMsg.GetMessage())
	if strings.HasPrefix(msg, "mismatched prefix") {
		return
	}
	if !strings.HasPrefix(msg, l.cfg.MessagePrefix) {
		fmt.Printf("mismatched prefix: log message %s did not match prefix: %s\n", string(logMsg.GetMessage()), string(l.cfg.MessagePrefix))
		return
	}

	if len(msg) < sepEnd {
		fmt.Printf("Cannot parse message %s\n", msg)
		return
	}

	id := Identity{
		appID: logMsg.GetAppId(),
		runID: msg[prefixEnd:guidEnd],
	}

	l.counterLock.Lock()
	defer l.counterLock.Unlock()
	counter, ok := l.counters[id]
	if !ok {
		counter = make(map[string]bool)
		l.counters[id] = counter
	}
	counter[msg[sepEnd:]] = true
}
