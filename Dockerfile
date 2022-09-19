# syntax=docker/dockerfile:1

## Build
FROM golang:1.19-alpine AS build

WORKDIR /app

COPY go.mod ./
COPY go.sum ./

RUN go mod download -tags musl

# this image is to be run as a job or cronjob

COPY *.go ./
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