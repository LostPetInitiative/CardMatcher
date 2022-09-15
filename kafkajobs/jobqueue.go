package kafkajobs

import (
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Job = []byte

type Worker = struct {
	client kafka.Consumer
}

func (w *Worker) TryGetNextJob(pollingInterval time.Duration) (Job,err) {
	event := w.client.Poll(int(pollingInterval.Milliseconds()))
	event.
}

func (w *Worker) GetNextJob() (Job,err) {
	
}
