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
	"sort"
	"strconv"
	"sync"
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

	simThresholdStr, ok := os.LookupEnv("SIMILARITY_THRESHOLD")
	if !ok {
		log.Fatalln("SIMILARITY_THRESHOLD env var is not set")
	}
	simThreshold, err := strconv.ParseFloat(simThresholdStr, 64)
	if err != nil {
		log.Fatalf("Can't parse %s (SIMILARITY_THRESHOLD env var) as float\n", simThresholdStr)
	}

	matchedImagesSearchURL := fmt.Sprintf("%s/MatchedImagesSearch", gatewayAddr)

	kafkajobs.EnsureTopicExists(kafkaBootstrapServers, inputTopic, 0, 0, 0)
	kafkajobs.EnsureTopicExists(kafkaBootstrapServers, outputTopic, 0, 0, 0)

	log.Printf("Initialization complete. Starting main loop")

	producer := kafkajobs.NewJobQueueProducer(kafkaBootstrapServers, outputTopic)
	defer producer.Close()
	worker := kafkajobs.NewJobQueueWorker(kafkaBootstrapServers, consumerGroup, inputTopic, time.Duration(1e9*60*30))
	defer worker.Close()

	jobChannel := make(chan []byte)
	confirmationChannel := make(chan int)
	stopChannel := make(chan int)
	go worker.Run(jobChannel, confirmationChannel, stopChannel)

	for {
		log.Println("Waiting for the next job...")
		jobBytes := <-jobChannel

		var inputJob CardEmbeddingsJob
		json.Unmarshal(jobBytes, &inputJob)

		// do actual processing
		log.Println("Job " + inputJob.Uid + " has " + fmt.Sprint(len(inputJob.Embeddings)) + " embeddings")

		similarRequests := inputJobToImageSearchRequest(&inputJob)

		similarImageChans := make([]<-chan FoundSimilarImage, len(similarRequests))
		for i, request := range similarRequests {
			simImagesChan := make(chan FoundSimilarImage)
			similarImageChans[i] = simImagesChan
			go getSimilarIntoChannel(matchedImagesSearchURL, request, simImagesChan, i)
		}

		similarImages := make([]FoundSimilarImage, 0)
		for img := range merge(similarImageChans...) {
			// fmt.Printf("Gathered similar image: %v\n", img)
			if img.CosSimilarity >= simThreshold {
				similarImages = append(similarImages, img)
			}
		}

		sort.Slice(similarImages, func(a, b int) bool { return similarImages[a].CosSimilarity > similarImages[b].CosSimilarity })

		fmt.Printf("Got %d similar images\n", len(similarImages))

		for _, img := range similarImages {
			log.Printf("%v\n", img)
		}
		if len(similarImages) > 0 {
			outputJob := SimilaritySearchResult{
				TargetID:        inputJob.Uid,
				PossibleMatches: similarImages,
			}
			outputJobBytes, err := json.MarshalIndent(outputJob, "", " ")
			if err != nil {
				log.Fatalf("Could not encode output job json: %v\n", err)
			}

			producer.Enqueue(inputJob.Uid, outputJobBytes)
		}

		confirmationChannel <- 1
		//time.Sleep(10 * time.Second)
	}
}

func merge[T interface{}](cs ...<-chan T) <-chan T {
	var wg sync.WaitGroup
	out := make(chan T)

	// Start an output goroutine for each input channel in cs.  output
	// copies values from c to out until c is closed, then calls wg.Done.
	output := func(c <-chan T) {
		for n := range c {
			out <- n
		}
		wg.Done()
	}
	wg.Add(len(cs))
	for _, c := range cs {
		go output(c)
	}

	// Start a goroutine to close out once all the output goroutines are
	// done.  This must start after the wg.Add call.
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

// to be run as gorutinne. Closes the resChan upon submitting all fetched similar images
func getSimilarIntoChannel(
	imageSearchURL string,
	request SimilarImageRequestJson,
	resChan chan FoundSimilarImage,
	imNum int,
) {
	res := getSimilarImages(imageSearchURL, &request, imNum)
	for _, s := range res {
		resChan <- s
	}
	close(resChan)
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

func getSimilarImages(imageSearchURL string, request *SimilarImageRequestJson, imNum int) []FoundSimilarImage {
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
	N := len(searchRes.Response.Docs)
	fmt.Printf("Found %d docs\n", N)
	result := make([]FoundSimilarImage, N)
	for i, doc := range searchRes.Response.Docs {
		embStr := doc.EmbeddingStr
		embedding := make([]float64, len(embStr))
		for j, v := range embStr {
			embedding[j], err = strconv.ParseFloat(v, 64)
			if err != nil {
				log.Fatalf("failed to parse float string: %s\n", err)
			}
		}
		sim := dotProduct(embedding, request.Features)
		// log.Printf("Doc: %s; similarity: %v\n", doc.Id, sim)
		result[i] = FoundSimilarImage{DocumentID: doc.Id, CosSimilarity: sim, TargetImageNum: imNum}
	}
	return result
}

type FoundSimilarImage struct {
	DocumentID     string
	TargetImageNum int
	CosSimilarity  float64
}

type SimilaritySearchResult struct {
	TargetID        string
	PossibleMatches []FoundSimilarImage
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
