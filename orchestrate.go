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

import "sync"

type Orchestrator interface {
	Rebuild(server Server)
	Dispose(server Server)
}

func Orchestrate(orchestrator Orchestrator, plan Plan) {
	var wait sync.WaitGroup
	defer wait.Wait()

	wait.Add(len(plan.Groups))

	for _, group := range plan.Groups {
		go func(group Group) {
			defer wait.Done()

			for _, server := range group.Servers {
				if group.Lifecycle.RebuildBeforeDispose {
					orchestrator.Rebuild(server)
					orchestrator.Dispose(server)
				} else {
					orchestrator.Dispose(server)
					orchestrator.Rebuild(server)
				}
			}
		}(group)
	}
}
