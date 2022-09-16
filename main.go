package main

import (
	"fmt"
	"os"

	"github.com/LostPetInitiative/CardMatcher/kafkajobs"
)

func main() {
	kafkaBootstrapServers, ok := os.LookupEnv("KAFKA_URL")
	if !ok {
		fmt.Println("KAFKA_URL env var is not set")
		os.Exit(1)
	}

	inputTopic, ok := os.LookupEnv("INPUT_QUEUE")
	if !ok {
		fmt.Println("INPUT_QUEUE env var is not set")
		os.Exit(1)
	}

	outputTopic, ok := os.LookupEnv("OUTPUT_QUEUE")
	if !ok {
		fmt.Println("OUTPUT_QUEUE env var is not set")
		os.Exit(1)
	}

	kafkajobs.EnsureTopicExists(kafkaBootstrapServers, inputTopic, 0, 0, 0)
	kafkajobs.EnsureTopicExists(kafkaBootstrapServers, outputTopic, 0, 0, 0)
	fmt.Println("Done")
}
