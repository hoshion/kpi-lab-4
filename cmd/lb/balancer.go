package main

import (
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/roman-mazur/design-practice-2-template/httptools"
	"github.com/roman-mazur/design-practice-2-template/signal"
)

var (
	port       = flag.Int("port", 8090, "load balancer port")
	timeoutSec = flag.Int("timeout-sec", 3, "request timeout time in seconds")
	https      = flag.Bool("https", false, "whether backends support HTTPs")

	traceEnabled = flag.Bool("trace", false, "whether to include tracing information into responses")
)

var (
	timeout     = time.Duration(*timeoutSec) * time.Second
	serversPool = []string{
		"server1:8080",
		"server2:8080",
		"server3:8080",
	}
	healthyServers = make([]string, 3)
)

func scheme() string {
	if *https {
		return "https"
	}
	return "http"
}

func health(dst string) bool {
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s://%s/health", scheme(), dst), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	if resp.StatusCode != http.StatusOK {
		return false
	}
	return true
}

func forward(dst string, rw http.ResponseWriter, r *http.Request) error {
	ctx, _ := context.WithTimeout(r.Context(), timeout)
	fwdRequest := r.Clone(ctx)
	fwdRequest.RequestURI = ""
	fwdRequest.URL.Host = dst
	fwdRequest.URL.Scheme = scheme()
	fwdRequest.Host = dst

	resp, err := http.DefaultClient.Do(fwdRequest)
	if err == nil {
		for k, values := range resp.Header {
			for _, value := range values {
				rw.Header().Add(k, value)
			}
		}
		if *traceEnabled {
			rw.Header().Set("lb-from", dst)
		}
		log.Println("fwd", resp.StatusCode, resp.Request.URL)
		rw.WriteHeader(resp.StatusCode)
		defer resp.Body.Close()
		_, err := io.Copy(rw, resp.Body)
		if err != nil {
			log.Printf("Failed to write response: %s", err)
		}
		return nil
	} else {
		log.Printf("Failed to get response from %s: %s", dst, err)
		rw.WriteHeader(http.StatusServiceUnavailable)
		return err
	}
}

func main() {
	healthChecker := &HealthChecker{}
	healthChecker.health = health
	healthChecker.serversPool = serversPool
	healthChecker.healthyServers = healthyServers
	healthChecker.checkInterval = 10 * time.Second

	balancer := &Balancer{}
	balancer.healthChecker = healthChecker
	balancer.forward = forward

	balancer.Start()
}

type Balancer struct {
	healthChecker *HealthChecker
	forward       func(string, http.ResponseWriter, *http.Request) error
}

func (b *Balancer) GetServerIndex(url string) int {
	hasher := fnv.New32()
	_, _ = hasher.Write([]byte(url))
	return int(hasher.Sum32() % uint32(len(b.healthChecker.healthyServers)))
}

func (b *Balancer) Start() {
	flag.Parse()

	b.healthChecker.StartHealthCheck()

	frontend := httptools.CreateServer(*port, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		b.healthChecker.mu.Lock()
		index := b.GetServerIndex(r.URL.Path)
		server := b.healthChecker.healthyServers[index]
		b.healthChecker.mu.Unlock()
		_ = b.forward(server, rw, r)
	}))

	log.Println("Starting load balancer...")
	log.Printf("Tracing support enabled: %t", *traceEnabled)
	frontend.Start()
	signal.WaitForTerminationSignal()
}

type HealthChecker struct {
	health         func(string) bool
	serversPool    []string
	healthyServers []string
	checkInterval  time.Duration
	mu             sync.Mutex
}

func (hc *HealthChecker) StartHealthCheck() {
	for i, server := range hc.serversPool {
		server := server
		i := i
		go func() {
			for range time.Tick(hc.checkInterval) {
				isHealthy := hc.health(server)
				if !isHealthy {
					hc.serversPool[i] = ""
				} else {
					hc.serversPool[i] = server
				}

				hc.mu.Lock()
				hc.healthyServers = hc.healthyServers[:0]

				for _, value := range hc.serversPool {
					if value != "" {
						hc.healthyServers = append(hc.healthyServers, value)
					}
				}
				hc.mu.Unlock()

				log.Println(server, isHealthy)
			}
		}()
	}
}
