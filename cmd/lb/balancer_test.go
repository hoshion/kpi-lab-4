package main

import (
	"testing"
	"time"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type BalancerSuite struct{}

var _ = Suite(&BalancerSuite{})

func (s *BalancerSuite) TestBalancer(c *C) {

	healthChecker := &HealthChecker{}
	healthChecker.healthyServers = []string{"4", "5", "6"}

	balancer := &Balancer{}
	balancer.healthChecker = healthChecker

	index1 := balancer.GetServerIndex("/check")
	index1secondTime := balancer.GetServerIndex("/check")
	index2 := balancer.GetServerIndex("/check2")
	index3 := balancer.GetServerIndex("/check4")

	c.Assert(index1, Equals, 0)
	c.Assert(index1, Equals, index1secondTime)
	c.Assert(index3, Equals, 1)
	c.Assert(index2, Equals, 2)
}

func (s *BalancerSuite) TestHealthChecker(c *C) {
	healthChecker := &HealthChecker{}
	healthChecker.health = func(s string) bool {
		if s == "1" {
			return false
		} else {
			return true
		}
	}

	healthChecker.serversPool = []string{"1", "2", "3"}
	healthChecker.healthyServers = []string{"4", "5", "6"}
	healthChecker.checkInterval = 1 * time.Second

	healthChecker.StartHealthCheck()

	time.Sleep(2 * time.Second)

	c.Assert(healthChecker.healthyServers[0], Equals, "2")
	c.Assert(healthChecker.healthyServers[1], Equals, "3")
	c.Assert(len(healthChecker.healthyServers), Equals, 2)
}
