package kafkajobs

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Job = []byte

type Worker struct {
	client *kafka.Consumer
}

// Does a lookup for the envVar. If it is defined, parses this value as uint, otherwise return default value
func tryEnvVarSecifiedConfig(envVar string, defaultVal uint) uint {
	s, ok := os.LookupEnv(envVar)
	if !ok {
		fmt.Printf("WARN: \"%s\" env var is not set. Using default value %d\n", envVar, defaultVal)
		return defaultVal
	} else {
		i, err := strconv.ParseUint(s, 0, 64)
		if err != nil {
			panic(fmt.Sprintf("Failed to parse \"%s\" env var as uint. the value failed to parse is \"%s\"\n", envVar, s))
		}
		fmt.Printf("INFO: Using value %d as defined in %s env var\n", i, envVar)
		return uint(i)
	}
}

// numPartitions 0 means "try read from KAFKA_NUM_PARTITIONS env var", if it is not defined, use default.
// replicationFactor = 0 means "try read from KAFKA_REPLICATION_FACTOR env var", if it is not defined, use default.
// retentionHours = 0 means "try read from KAFKA_RETENTION_HOURS env var", if it is not defined, use default.
func EnsureTopicExists(bootstrapServers string, topicName string, numPartitions uint, replicationFactor uint, retentionHours uint) {
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})

	if err != nil {
		panic(err)
	}

	if numPartitions < 1 {
		numPartitions = tryEnvVarSecifiedConfig("KAFKA_NUM_PARTITIONS", 8)
	}
	if replicationFactor < 1 {
		replicationFactor = tryEnvVarSecifiedConfig("KAFKA_REPLICATION_FACTOR", 1)
	}
	if retentionHours < 1 {
		retentionHours = tryEnvVarSecifiedConfig("KAFKA_RETENTION_HOURS", 168) // 168 hours = 1 week
	}

	topicConfig := make(map[string]string)
	topicConfig["retention.ms"] = fmt.Sprintf("%d", retentionHours*60*60*1000)

	var ctx context.Context

	topicSpec := kafka.TopicSpecification{
		Topic:             topicName,
		NumPartitions:     int(numPartitions),
		ReplicationFactor: int(replicationFactor),
		Config:            topicConfig,
	}

	res, err := adminClient.CreateTopics(ctx, []kafka.TopicSpecification{topicSpec})
	if err != nil {
		fmt.Printf("INFO: topic \"%s\" already exists\n", topicName)
	} else {
		fmt.Printf("INFO: topic \"%s\" creation reault: %v\n", topicName, res[0])
	}
}

func NewWorker(bootstrapServers string, groupId string, topicName string) Worker {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"group.id":          groupId,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err)
	}
	worker := Worker{client: consumer}

	err = worker.client.Subscribe(topicName, nil)
	if err != nil {
		panic(err)
	}

	return worker
}

func (w *Worker) TryGetNextJob(pollingInterval time.Duration) (Job, error) {
	mess, err := w.client.ReadMessage(pollingInterval)
	if err != nil {
		if err.(kafka.Error).Code() == kafka.ErrTimedOut {
			return nil, nil
		} else {
			return nil, err
		}
	}
	return Job(mess.Value), nil
}

func (w *Worker) GetNextJob(pollingInterval time.Duration) Job {
	for {
		job, err := w.TryGetNextJob(pollingInterval)
		if err != nil {
			fmt.Printf("Consumer error: %v (%v)\n", err, job)
		} else if job != nil {
			return job
		}
	}
}

// Run the worker loop. The jobs are pushed to the channel "c".
func (w *Worker) Run(c chan<- Job, stopChannel <-chan int) {
	for {
		select {
		case <-stopChannel:
			// signal to stop. closig jobs channel and exiting
			close(c)
			return
		default:
			job, err := w.TryGetNextJob(time.Duration(1e9))
			if err != nil {
				fmt.Printf("Consumer error: %v (%v)\n", err, job)
			}
			if job != nil {
				c <- job
			}
		}
	}
}
