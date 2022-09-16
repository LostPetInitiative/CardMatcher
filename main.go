package main

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"time"
	"unicode"

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

	gatewayAddr, ok := os.LookupEnv("SOLR_ADDR")
	if !ok {
		log.Fatalln("SOLR_ADDR env var is not set")
	}

	matchedImagesSearchURL := fmt.Sprintf("%s/MatchedImagesSearch", gatewayAddr)

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

		similarRequests := inputJobToImageSearchRequest(&inputJob)
		if len(similarRequests) > 0 {
			getSimilarImages(matchedImagesSearchURL, &similarRequests[0])
		}
		// confirmationChannel <- 1
		//time.Sleep(10 * time.Second)
		return
	}

	log.Println("Done")
}

func decodeEmbedding(encoded *EncodedEmbedding) []float32 {
	embBytes, e := base64.StdEncoding.DecodeString(encoded.EmbeddingStr)
	if e != nil {
		log.Fatalf("Failed to recode base64 for embedding\n")
	}
	// fmt.Printf("Embedding consts of %d bytes\n", len(embBytes))
	r := bytes.NewReader(embBytes)
	var embedding []float32 = make([]float32, len(embBytes)/4)
	binary.Read(r, binary.LittleEndian, &embedding)

	return embedding

}

func normalize(vector []float32) []float64 {
	doubleArr := make([]float64, len(vector))
	var sumSqr float64
	for i, v := range vector {
		doubleV := float64(v)
		doubleArr[i] = doubleV
		sumSqr += doubleV * doubleV

	}
	vectorLength := math.Sqrt(sumSqr)
	// actually normalizing
	for i := 0; i < len(vector); i++ {
		doubleArr[i] /= vectorLength
	}
	return doubleArr
}

func capitalize(s string) string {
	r := []rune(s)
	return string(append([]rune{unicode.ToUpper(r[0])}, r[1:]...))
}

func inputJobToImageSearchRequest(job *CardEmbeddingsJob) []SimilarImageRequestJson {
	var N = len(job.Embeddings)
	var result = make([]SimilarImageRequestJson, N)
	for i, embedding := range job.Embeddings {
		normalizedEmbedding := normalize(decodeEmbedding(&embedding))
		result[i] = SimilarImageRequestJson{
			Lat:           job.Location.Lat,
			Lon:           job.Location.Lon,
			Animal:        capitalize(job.Animal),
			EventTime:     job.EventTime.Format(time.RFC3339),
			EventType:     capitalize(job.CardType),
			Features:      normalizedEmbedding,
			FeaturesIdent: "calvin_zhirui_embedding",
			FilterFar:     true,
			FilterLongAgo: true,
		}
	}

	return result
}

func dotProduct(v1 []float64, v2 []float64) float64 {
	if len(v1) != len(v2) {
		log.Fatalf("Can't calculate dot product as vectors have different lengths: %d and %d\n", len(v1), len(v2))
	}
	N := len(v1)
	var result float64
	for i := 0; i < N; i++ {
		result += v1[i] * v2[i]
	}
	return result
}

func getSimilarImages(imageSearchURL string, request *SimilarImageRequestJson) {
	encodedBytes, err := json.MarshalIndent(*request, "", "   ")
	if err != nil {
		log.Fatalf("Failed to marshal json: %v", err)
	}

	httpClient := http.Client{
		Timeout: time.Minute * 20,
	}

	req, err := http.NewRequest(http.MethodPost, imageSearchURL, bytes.NewReader(encodedBytes))
	if err != nil {
		log.Fatalf("Failed to constract request for similar images: %v", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")

	log.Printf("Qeuring for similarities...\n")
	res, postErr := httpClient.Do(req)
	if postErr != nil {
		log.Fatalf("Failed to get similar images: %v", postErr)
	}

	if res.Body != nil {
		defer res.Body.Close()
	}
	log.Printf("Got similarity serach result: %d\n", res.StatusCode)

	body, readErr := io.ReadAll(res.Body)
	if readErr != nil {
		log.Fatalf("Failed to read the HTTP body response:%v\n", readErr)
	}

	var searchRes SimilarImageSearchResultJson
	err = json.Unmarshal(body, &searchRes)
	if err != nil {
		log.Fatalf("Failed to parse search result json: %v;\n", err)
	}
	fmt.Printf("Found %d docs\n", len(searchRes.Response.Docs))
	for _, doc := range searchRes.Response.Docs {
		embStr := doc.EmbeddingStr
		embedding := make([]float64, len(embStr))
		for i, v := range embStr {
			embedding[i], err = strconv.ParseFloat(v, 64)
			if err != nil {
				log.Fatalf("failed to parse float string: %s\n", err)
			}
		}
		sim := dotProduct(embedding, request.Features)
		log.Printf("Doc: %s; similarity: %v\n", doc.Id, sim)
	}
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

type SimilarImageRequestJson struct {
	Lat           float64
	Lon           float64
	Animal        string
	EventTime     string
	EventType     string
	Features      []float64
	FeaturesIdent string
	FilterFar     bool
	FilterLongAgo bool
}

type SimilarImageDocsJson struct {
	Id           string   `json:"id"`
	EmbeddingStr []string `json:"calvin_zhirui_embedding"`
}

type SimilarImageResponseJson struct {
	Docs []SimilarImageDocsJson `json:"docs"`
}

type SimilarImageSearchResultJson struct {
	Response SimilarImageResponseJson `json:"response"`
}
