# syntax=docker/dockerfile:1

## Build
FROM golang:1.22-alpine AS build

# AA kafka lib requires GCC
RUN apk add build-base

WORKDIR /app

COPY go.mod ./
COPY go.sum ./

RUN go mod download

COPY *.go ./
COPY kafkajobs ./kafkajobs

# as "-tags musl - must be specified when building on/for musl-based Linux distros, such as Alpine. Will use the bundled static musl build of librdkafka."
RUN go build -tags musl -o /cardMatcher


## Deploy
FROM alpine as final
WORKDIR /
COPY --from=build /cardMatcher /cardMatcher

# ENV KAFKA_URL=xxxx
# ENV INPUT_QUEUE=xxx
# ENV OUTPUT_QUEUE=xxx
# ENV SOLR_ADDR=xxx
ENV SIMILARITY_THRESHOLD="0.3"

ENTRYPOINT ["/cardMatcher"]