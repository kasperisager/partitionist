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

import (
	"sort"

	pqueue "github.com/kasperisager/pqueue"
	union "github.com/kasperisager/union"
)

type Plan struct {
	Groups []Group `json:"groups"`
}

type Group struct {
	Servers   []Server  `json:"servers"`
	Lifecycle Lifecycle `json:"lifecycle"`
}

// Partition creates a partitioned upgrade plan for a cluster.
func (cluster Cluster) Partition() (*Plan, error) {
	if err := cluster.Validate(); err != nil {
		return nil, err
	}

	return &Plan{
		Group{Servers: cluster.Servers}.partitionByLifecycle(cluster),
	}, nil
}

func sortByWeight(servers []Server) {
	sort.Slice(servers, func(i int, j int) bool {
		return servers[i].Weight >= servers[j].Weight
	})
}

func assignToGroup(queue pqueue.PriorityQueue, groups []Group, server Server, index int) {
	weight, _ := queue.Priority(index)
	group := &groups[index]

	group.Servers = append(group.Servers, server)
	queue.Update(index, weight+server.Weight+1)
}

func (g Group) partitionByLifecycle(cluster Cluster) []Group {
	length := len(g.Servers)

	if length == 0 {
		return []Group{}
	}

	fst := Group{Lifecycle: Lifecycle{true}}
	snd := Group{}

	for _, server := range g.Servers {
		lifecycle := server.Lifecycle()

		if lifecycle.RebuildBeforeDispose {
			fst.Servers = append(fst.Servers, server)
		} else {
			snd.Servers = append(snd.Servers, server)
		}
	}

	return append(
		fst.partitionBySurge(cluster),
		snd.partitionByService(cluster)...,
	)
}

func (group Group) partitionByService(cluster Cluster) []Group {
	length := len(group.Servers)

	if length == 0 {
		return []Group{}
	}

	representatives := make(map[string]int)
	components := union.New()

	// First pass: Join servers in groups according to their associated services
	// such that servers running the same services will be grouped. This ensures
	// that the resulting groups can be treated indenpently as no two groups will
	// contain servers running the same services.
	for i, server := range group.Servers {
		for _, allocation := range server.Allocations {
			service := allocation.Service

			if j, ok := representatives[service.ID]; ok {
				components.Join(i, j)
			} else {
				representatives[service.ID] = i
			}
		}
	}

	parts := make(map[int][]Server)

	// Second pass: Partition the servers into their associated groups.
	for i, server := range group.Servers {
		j := components.Find(i)
		parts[j] = append(parts[j], server)
	}

	groups := make([]Group, 0, len(parts))

	for _, servers := range parts {
		groups = append(
			groups,
			Group{servers, group.Lifecycle}.partitionByTolerance(cluster)...,
		)
	}

	return groups
}

func (group Group) partitionBySurge(cluster Cluster) []Group {
	length := len(group.Servers)

	if length == 0 {
		return []Group{}
	}

	sortByWeight(group.Servers)

	surge := cluster.Surge

	if length < surge {
		surge = length
	}

	queue := pqueue.New(pqueue.Ascending)
	groups := make([]Group, surge)

	for i := range groups {
		queue.Push(i, 0)
		groups[i].Lifecycle = group.Lifecycle
	}

	for _, server := range group.Servers {
		index, _ := queue.Peek()
		assignToGroup(queue, groups, server, index)
	}

	return groups
}

func (group Group) partitionByTolerance(cluster Cluster) []Group {
	length := len(group.Servers)

	if length == 0 {
		return []Group{}
	}

	sortByWeight(group.Servers)

	tolerance := MaxTolerance(group.Servers)
	queue := pqueue.New(pqueue.Ascending)
	groups := make([]Group, tolerance)

	for i := range groups {
		queue.Push(i, 0)
		groups[i].Lifecycle = group.Lifecycle
	}

	for _, server := range group.Servers {
		t, ok := server.Tolerance()

		if !ok {
			t = tolerance
		}

		var index int

		if j, _ := queue.Peek(); j <= t {
			index = j
		} else {
			for j := 1; j < t; j++ {
				pi, _ := queue.Priority(index)
				pj, _ := queue.Priority(j)

				if pi > pj {
					index = j
				}
			}
		}

		assignToGroup(queue, groups, server, index)
	}

	return groups
}
