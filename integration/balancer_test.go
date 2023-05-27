package integration

import (
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"
)

const baseAddress = "http://balancer:8090"

var client = http.Client{
	Timeout: 3 * time.Second,
}

func TestBalancer(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	// test server1
	server1, err := client.Get(fmt.Sprintf("%s/check", baseAddress))
	if err != nil {
		t.Error(err)
	}

	server1Header := server1.Header.Get("lb-from")
	if server1Header != "server1:8080" {
		t.Errorf("Wrong server for server1 - %s", server1Header)
	}

	// test server2
	server2, err := client.Get(fmt.Sprintf("%s/check4", baseAddress))
	if err != nil {
		t.Error(err)
	}

	server2Header := server2.Header.Get("lb-from")
	if server2Header != "server2:8080" {
		t.Errorf("Wrong server for server 2 - %s", server2Header)
	}

	// test server3
	server3, err := client.Get(fmt.Sprintf("%s/check2", baseAddress))
	if err != nil {
		t.Error(err)
	}

	server3Header := server3.Header.Get("lb-from")
	if server3Header != "server3:8080" {
		t.Errorf("Wrong server for server3 - %s", server3Header)
	}

	// test repeated request
	server1Repeat, err := client.Get(fmt.Sprintf("%s/check", baseAddress))
	if err != nil {
		t.Error(err)
	}

	server1RepeatHeader := server1Repeat.Header.Get("lb-from")
	if server1Header != server1RepeatHeader {
		t.Errorf("Headers are not equal. origin - %s, repeat - %s", server1Header, server1RepeatHeader)
	}
}

func BenchmarkBalancer(b *testing.B) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		b.Skip("Integration test is not enabled")
	}

	for i := 0; i < b.N; i++ {
		_, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
		if err != nil {
			b.Error(err)
		}
	}
}
