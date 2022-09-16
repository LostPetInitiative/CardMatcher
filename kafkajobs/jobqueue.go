package kafkajobs

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const appName = "CardMatcher"

type Job = []byte

type JobQueueWorker struct {
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

	var ctx context.Context = context.Background()

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

func NewJobQueueWorker(bootstrapServers string, groupId string, topicName string, maxPermitedJobProcessing time.Duration) JobQueueWorker {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":    bootstrapServers,
		"group.id":             groupId,
		"auto.offset.reset":    "earliest",
		"client.id":            appName,
		"enable.auto.commit":   false,
		"max.poll.interval.ms": maxPermitedJobProcessing.Milliseconds(),
	})
	if err != nil {
		panic(err)
	}
	worker := JobQueueWorker{client: consumer}

	err = worker.client.Subscribe(topicName, nil)
	if err != nil {
		panic(err)
	}

	return worker
}

func (w *JobQueueWorker) TryGetNextJob(pollingInterval time.Duration) (Job, *kafka.Message, error) {
	mess, err := w.client.ReadMessage(pollingInterval)
	if err != nil {
		if err.(kafka.Error).Code() == kafka.ErrTimedOut {
			return nil, nil, nil
		} else {
			return nil, nil, err
		}
	}
	return Job(mess.Value), mess, nil
}

// Run the worker loop. The jobs are pushed to the channel "c".
func (w *JobQueueWorker) Run(c chan<- Job, confirmationChannel <-chan int, stopChannel <-chan int) {
	for {
		select {
		case <-stopChannel:
			// signal to stop. closig jobs channel and exiting
			close(c)
			return
		default:
			job, message, err := w.TryGetNextJob(time.Duration(1e9))
			if err != nil {
				fmt.Printf("Consumer error: %v (%v)\n", err, job)
			}
			if job != nil {
				fmt.Printf("Got job key %v (partition %v; %v)\n", message.Key, message.TopicPartition, message.Timestamp)
				c <- job
				<-confirmationChannel
				w.client.CommitMessage(message)
				fmt.Printf("Comitted processing of %v (partition %v; %v)\n", message.Key, message.TopicPartition, message.Timestamp)
			}
		}
	}
}

type JobQueueProducer struct {
	client *kafka.Producer
	// deliveryChannel chan kafka.Event
	topicName string
}

func NewJobQueueProducer(bootstrapServers string, topicName string) JobQueueProducer {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"client.id":         appName,
		"max.request.size":  32 * 1024 * 1024, // 32 Mb
		"acks":              "all",
		"retries":           10,
		"compression.type":  "gzip",
	})
	if err != nil {
		panic(err)
	}
	jobProducer := JobQueueProducer{
		client: producer,
		// deliveryChannel: make(chan kafka.Event),
		topicName: topicName,
	}

	return jobProducer
}

// Blockes until the job delivery is received from the cluster
func (p *JobQueueProducer) Enqueue(jobKey string, jobBody []byte) {
	fmt.Printf("Submitting job \"%s\" (%d bytes)...\n", jobKey, len(jobBody))
	go p.client.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &p.topicName,
			Partition: kafka.PartitionAny,
		},
		Value: jobBody,
	}, nil)
	for {
		leftToSend := p.client.Flush(60 * 1000) // 1 min
		if leftToSend == 0 {
			break
		}
		fmt.Printf("%d messages are still to send...\n", leftToSend)
	}

	fmt.Printf("job \"%s\"  submitted successfully", jobKey)
}
