// Copyright (C) 2017 Kasper Kronborg Isager
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
package partitionist

import "fmt"

type State struct {
	// Whether or not the state is concurrent, i.e. can be accessed by multiple
	// servers simultaneously.
	Concurrent bool `json:"concurrent"`

	// Whether the state is internal or external to the server. Internal state is
	// state that is destroyed alongside the server on which the state lives when
	// the server itself is destroyed whereas external state can survive
	// destruction of the server.
	External bool `json:"external"`

	// Whether the state is replicated to more than one server. This is used for
	// validating whether servers with internal state can actually be upgraded
	// without discarding their associated state.
	Replicated bool `json:"replicated"`
}

type Service struct {
	// An ID for uniquely identifying instances of the service across servers.
	// This is also used as the partition key when partitioning servers during
	// deployment planning.
	ID string `json:"id"`

	// The availability tolerance of the service, that is the number of servers
	// running the service that are allowed to be unavailable at any given moment
	// during a deployment. Tolerances must be greater than zero.
	Tolerance int `json:"tolerance"`

	// The optional state associated with the service. This is used for inferring
	// the lifecycle of the servers onto which the service will run.
	State *State `json:"state"`
}

type Allocation struct {
	// The service being allocated to the associated server.
	Service Service `json:"service"`

	// The number of instances of the service allocated to the associated server.
	// Instance counts must be greater than zero.
	Instances int `json:"instances"`
}

type Server struct {
	// An ID for uniquely identifying the server. This is primarily meant as a
	// means of letting external tools identify servers in upgrade plans.
	ID string `json:"id"`

	// The service allocations for the server.
	Allocations []Allocation `json:"allocations"`

	// The relative weight of the server. Weights are used for distributing
	// servers evenly across upgrade steps. Weights must be greater than or equal
	// to zero.
	Weight float32 `json:"weight"`
}

type Cluster struct {
	Servers []Server `json:"servers"`
	Surge   int      `json:"surge"`
}

type Lifecycle struct {
	RebuildBeforeDispose bool `json:"rebuildBeforeDispose"`
}

// Validate checks that a state is legal.
func (state State) Validate() error {
	if !state.External && !state.Replicated {
		return fmt.Errorf("Internal state must be replicated")
	}

	return nil
}

// Validate checks that a service is legal.
func (service Service) Validate() error {
	if service.Tolerance <= 0 {
		return fmt.Errorf(
			"%s: Tolerance (%d) must be greater than 0",
			service.ID,
			service.Tolerance,
		)
	}

	if service.State != nil {
		if err := service.State.Validate(); err != nil {
			return err
		}
	}

	return nil
}

// Validate checks that an allocation is legal.
func (allocation Allocation) Validate() error {
	if allocation.Instances <= 0 {
		return fmt.Errorf(
			"%s: Number of instances (%d) must be greater than 0",
			allocation.Service.ID,
			allocation.Instances,
		)
	}

	if err := allocation.Service.Validate(); err != nil {
		return err
	}

	return nil
}

// Validate checks that a server is legal.
func (server Server) Validate() error {
	if server.Weight < 0 {
		return fmt.Errorf(
			"%s: Weight (%d) must be greater than or equal to 0",
			server.ID,
			server.Weight,
		)
	}

	for _, allocation := range server.Allocations {
		if err := allocation.Validate(); err != nil {
			return err
		}
	}

	return nil
}

// Validate checks that a cluster is legal.
func (cluster Cluster) Validate() error {
	if cluster.Surge <= 0 {
		return fmt.Errorf("Surge (%d) must be greater than 0", cluster.Surge)
	}

	// Keep track of the total number of allocations of each service, that is the
	// number of servers containing one or more instances of each service.
	allocations := make(map[string]int)

	for _, server := range cluster.Servers {
		if err := server.Validate(); err != nil {
			return err
		}

		for _, allocation := range server.Allocations {
			allocations[allocation.Service.ID]++
		}
	}

	for _, server := range cluster.Servers {
		for _, allocation := range server.Allocations {
			service := allocation.Service
			allocated := allocations[service.ID]

			if service.Tolerance >= allocated {
				return fmt.Errorf(
					"%s: Not enough allocations (%d) to satisfy tolerance (%d)",
					service.ID,
					allocated,
					service.Tolerance,
				)
			}
		}
	}

	return nil
}

// Lifecycle computes the lifecycle of a server.
func (server Server) Lifecycle() Lifecycle {
	lifecycle := Lifecycle{
		RebuildBeforeDispose: true,
	}

	for _, allocation := range server.Allocations {
		state := allocation.Service.State

		if state == nil {
			continue;
		}

		// In case of external, nonconcurrent, nonreplicated state, the new server
		// cannot be built before the old one is diposed as the two servers cannot
		// access the same external state simultaneously.
		if state.External && !state.Replicated && !state.Concurrent {
			lifecycle.RebuildBeforeDispose = false
		}
	}

	return lifecycle
}

// Tolerance computes the tolerance of a server.
func (server Server) Tolerance() (int, bool) {
	var tolerance int

	if len(server.Allocations) == 0 {
		return tolerance, false
	}

	for _, allocation := range server.Allocations {
		service := allocation.Service

		if tolerance == 0 || service.Tolerance < tolerance {
			tolerance = service.Tolerance
		}
	}

	return tolerance, true
}

// Tolerance computes the maximum tolerance of a list of servers.
func MaxTolerance(servers []Server) int {
	length := len(servers)
	tolerance := 0

	for _, server := range servers {
		t, ok := server.Tolerance()

		if ok && t > tolerance {
			tolerance = t
		}
	}

	if tolerance == 0 || length < tolerance {
		tolerance = length
	}

	return tolerance
}

// Tolerance computes the minimum tolerance of a list of servers.
func MinTolerance(servers []Server) int {
	length := len(servers)
	tolerance := length

	for _, server := range servers {
		t, ok := server.Tolerance()

		if ok && t < tolerance {
			tolerance = t
		}
	}

	return tolerance
}

// Instances computes the total number of instances of all services allocated
// to a server.
func (server Server) Instances() int {
	var count int

	for _, allocation := range server.Allocations {
		count += allocation.Instances
	}

	return count
}

// InstancesOf computes the total number of a instances of a specific service
// allocated to a server.
func (server Server) InstancesOf(service Service) int {
	for _, allocation := range server.Allocations {
		if allocation.Service.ID == service.ID {
			return allocation.Instances
		}
	}

	return 0
}
