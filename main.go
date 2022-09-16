package main

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/LostPetInitiative/CardMatcher/kafkajobs"
)

const consumerGroup = "cardMatcher"

func main() {
	kafkaBootstrapServers, ok := os.LookupEnv("KAFKA_URL")
	if !ok {
		log.Fatalln("KAFKA_URL env var is not set")
	}

	inputTopic, ok := os.LookupEnv("INPUT_QUEUE")
	if !ok {
		log.Fatalln("INPUT_QUEUE env var is not set")
	}

	outputTopic, ok := os.LookupEnv("OUTPUT_QUEUE")
	if !ok {
		log.Fatalln("OUTPUT_QUEUE env var is not set")
	}

	kafkajobs.EnsureTopicExists(kafkaBootstrapServers, inputTopic, 0, 0, 0)
	kafkajobs.EnsureTopicExists(kafkaBootstrapServers, outputTopic, 0, 0, 0)

	log.Printf("Initialization complete. Starting main loop")

	//producer := kafkajobs.NewJobQueueProducer(kafkaBootstrapServers, outputTopic)
	worker := kafkajobs.NewJobQueueWorker(kafkaBootstrapServers, consumerGroup, inputTopic, time.Duration(1e9*60*10))
	defer worker.Close()

	jobChannel := make(chan []byte)
	confirmationChannel := make(chan int)
	stopChannel := make(chan int)
	go worker.Run(jobChannel, confirmationChannel, stopChannel)

	for {
		jobBytes := <-jobChannel

		var inputJob CardEmbeddingsJob
		json.Unmarshal(jobBytes, &inputJob)

		// do actual processing
		log.Println("Job " + inputJob.Uid + " has " + fmt.Sprint(len(inputJob.Embeddings)) + " embeddings")

		//embeddings := inputJobToEmbeddings(&inputJob)
		// confirmationChannel <- 1
		//time.Sleep(10 * time.Second)
		return
	}

	log.Println("Done")
}

func inputJobToEmbeddings(job *CardEmbeddingsJob) [][]float32 {
	result := make([][]float32, 0)
	for _, emb := range job.Embeddings {
		embBytes, e := base64.StdEncoding.DecodeString(emb.EmbeddingStr)
		if e != nil {
			fmt.Printf("Failed to recode base64 for embedding\n")
			os.Exit(4)
		}
		// fmt.Printf("Embedding consts of %d bytes\n", len(embBytes))
		r := bytes.NewReader(embBytes)
		var embedding []float32 = make([]float32, len(embBytes)/4)
		binary.Read(r, binary.LittleEndian, &embedding)

		result = append(result, embedding)
		//fmt.Printf("Read %d floats\n", len(embedding))
		//fmt.Println(embedding)
	}
	return result
}

type LocationJson struct {
	Lat float64
	Lon float64
}

type EncodedEmbedding struct {
	HeadCount    uint   `json:"head_count"`
	EmbeddingStr string `json:"embedding"`
}

type CardEmbeddingsJob struct {
	Uid        string             `json:"uid"`
	Animal     string             `json:"animal"`
	Location   LocationJson       `json:"location"`
	CardType   string             `json:"card_type"`
	Embeddings []EncodedEmbedding `json:"image_embeddings"`
	EventTime  time.Time          `json:"event_time"`
}
