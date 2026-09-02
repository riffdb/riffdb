package main

import (
	"encoding/json"
	"fmt"
	"os"
)

type corpusDocument struct {
	Operations []struct {
		Tag    uint64 `json:"tag"`
		Bounds []struct {
			Target  string `json:"target"`
			Maximum uint64 `json:"maximum"`
			Cases   []struct {
				EncodedBytes uint64 `json:"encoded_bytes"`
			} `json:"cases"`
		} `json:"bounds"`
	} `json:"operations"`
}

type observation struct {
	Tag          uint64 `json:"tag"`
	Target       string `json:"target"`
	EncodedBytes uint64 `json:"encoded_bytes"`
	ErrorClass   string `json:"error_class"`
}

func classify(target string, encodedBytes uint64, maximum uint64) string {
	if encodedBytes <= maximum {
		return "none"
	}
	if target == "encoded_request_bytes" {
		return "request_too_large"
	}
	return "response_too_large"
}

func run() error {
	source, err := os.ReadFile(os.Args[1])
	if err != nil {
		return err
	}
	var corpus corpusDocument
	if err := json.Unmarshal(source, &corpus); err != nil {
		return err
	}
	observations := make([]observation, 0)
	for _, operation := range corpus.Operations {
		for _, bound := range operation.Bounds {
			for _, entry := range bound.Cases {
				observations = append(observations, observation{
					Tag:          operation.Tag,
					Target:       bound.Target,
					EncodedBytes: entry.EncodedBytes,
					ErrorClass:   classify(bound.Target, entry.EncodedBytes, bound.Maximum),
				})
			}
		}
	}
	return json.NewEncoder(os.Stdout).Encode(observations)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
