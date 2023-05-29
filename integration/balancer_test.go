package integration

import (
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type IntegrationSuite struct{}

var _ = Suite(&IntegrationSuite{})

const baseAddress = "http://balancer:8090"

var client = http.Client{
	Timeout: 3 * time.Second,
}

func (s *IntegrationSuite) TestBalancer(c *C) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		c.Skip("Integration test is not enabled")
	}

	// test server1
	server1, err := client.Get(fmt.Sprintf("%s/check", baseAddress))
	if err != nil {
		c.Error(err)
	}

	server1Header := server1.Header.Get("lb-from")
	c.Check(server1Header, Equals, "server1:8080")

	// test server2
	server2, err := client.Get(fmt.Sprintf("%s/check4", baseAddress))
	if err != nil {
		c.Error(err)
	}

	server2Header := server2.Header.Get("lb-from")
	c.Check(server2Header, Equals, "server2:8080")

	// test server3
	server3, err := client.Get(fmt.Sprintf("%s/check2", baseAddress))
	if err != nil {
		c.Error(err)
	}

	server3Header := server3.Header.Get("lb-from")
	c.Check(server3Header, Equals, "server3:8080")

	// test repeated request
	server1Repeat, err := client.Get(fmt.Sprintf("%s/check", baseAddress))
	if err != nil {
		c.Error(err)
	}

	server1RepeatHeader := server1Repeat.Header.Get("lb-from")
	c.Check(server1RepeatHeader, Equals, server1Header)
}

func (s *IntegrationSuite) BenchmarkBalancer(c *C) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		c.Skip("Integration test is not enabled")
	}

	for i := 0; i < c.N; i++ {
		_, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
		if err != nil {
			c.Error(err)
		}
	}
}
