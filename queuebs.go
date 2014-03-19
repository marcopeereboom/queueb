/*
 * Copyright (c) 2014 Marco Peereboom <marco@peereboom.us>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package queueb

import (
	"container/heap"
	"fmt"
	"sync"
	"sync/atomic"
)

type QueuebMessageError struct {
	Error   error       // error
	Message interface{} // original message
}

type QueuebMessage struct {
	From    string      // From queue
	To      []string    // To queue
	Message interface{} // Queueb agnostic message that is passed around

	counter  uint64 // running counter to keep same prio in order
	priority int    // message priority, inherented from QueuebChannelPair
}

// implement heap.Interface
type PriorityQueue []*QueuebMessage

func (pq PriorityQueue) Len() int {
	return len(pq)
}

func (pq PriorityQueue) Less(i, j int) bool {
	if pq[i].priority == pq[j].priority {
		return pq[i].counter < pq[j].counter
	}

	return pq[i].priority > pq[j].priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *PriorityQueue) Push(x interface{}) {
	*pq = append(*pq, x.(*QueuebMessage))
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	x := old[n-1]
	*pq = old[0 : n-1]
	return x
}

type QueuebChannelPair struct {
	stopped  int32          // in the process of stopping
	wg       sync.WaitGroup // wait until done stopping
	priority int

	From chan *QueuebMessage // incomming channel
	To   chan *QueuebMessage // outgoing channel
	Name string              // queue name
}

type Queueb struct {
	Name    string                        // queue context name
	Queuebs map[string]*QueuebChannelPair // registered queues

	pq         *PriorityQueue // used to handle overflow messages
	depth      uint           // depth of sink before queueing
	pending    uint           // number of outstanding messages
	counter    uint64         //running counter
	queuebsMtx sync.RWMutex   // mutex for Queuebs
}

// Allocate a new Queueb context.
func New(name string, depth uint) (*Queueb, error) {
	q := Queueb{
		Name:    name,
		Queuebs: make(map[string]*QueuebChannelPair),
		depth:   depth,
		pq:      &PriorityQueue{},
	}

	heap.Init(q.pq)

	return &q, nil
}

// Route message to recipients.
func (q *Queueb) queuebMessageRoute(qcp *QueuebChannelPair, m *QueuebMessage, done chan bool) {
	q.queuebsMtx.RLock()
	defer q.queuebsMtx.RUnlock()

	for _, v := range m.To {
		to, found := q.Queuebs[v]
		if found {
			to.To <- m
			continue
		}

		// route error back to caller
		to, found = q.Queuebs[m.From]
		if found {
			// reuse m
			e := QueuebMessageError{
				Error: fmt.Errorf("could not deliver "+
					"message to %v", v),
				Message: m,
			}
			m.Message = e
			to.To <- m
			continue
		}

		// From no longer registered, silently ignore
	}

	done <- true // signal caller
}

// Mutex must be held
func (q *Queueb) findQueuebChannelPair(name string) (*QueuebChannelPair, error) {
	qcp, found := q.Queuebs[name]
	if !found {
		return nil, fmt.Errorf("queue not found: %v", name)
	}

	if atomic.LoadInt32(&qcp.stopped) != 0 {
		return nil, fmt.Errorf("queue stopped: %v", name)
	}

	return qcp, nil
}

// Send message from "name" to "to" string array.
func (q *Queueb) Send(name string, to []string, msg interface{}) error {
	m := QueuebMessage{
		From:    name,
		To:      to,
		Message: msg,
	}

	q.queuebsMtx.RLock()
	defer q.queuebsMtx.RUnlock()

	qcp, err := q.findQueuebChannelPair(name)
	if err != nil {
		return err
	}

	m.priority = qcp.priority // set priority based on QueuebChannelPair

	qcp.From <- &m

	return nil
}

// Blocking receive message from "name" queue.
func (q *Queueb) Receive(name string) (*QueuebMessage, error) {
	q.queuebsMtx.RLock()
	qcp, err := q.findQueuebChannelPair(name)
	q.queuebsMtx.RUnlock() //must unlock early since channel read is blocking
	if err != nil {
		return nil, err
	}

	// this is ok, even if To is closed
	m, ok := <-qcp.To
	if !ok {
		return nil, fmt.Errorf("%v.From channel closed", name)
	}

	return m, nil
}

// Register new queuw named "name" with a priority.
// Higher priority number means higher priority.
func (q *Queueb) Register(name string, priority int) error {
	qcp := QueuebChannelPair{
		Name:     name,
		From:     make(chan *QueuebMessage),
		To:       make(chan *QueuebMessage),
		priority: priority,
	}

	q.queuebsMtx.Lock()
	defer q.queuebsMtx.Unlock()

	_, found := q.Queuebs[name]
	if found {
		return fmt.Errorf("queue already exists: %v", name)
	}
	q.Queuebs[name] = &qcp

	qcp.wg.Add(1)
	go func() {
		defer qcp.wg.Done()
		done := make(chan bool)
		for {
			select {
			case m, ok := <-qcp.From:
				if !ok {
					// disconnected.
					return
				}
				if q.pending < q.depth {
					// sink
					q.pending++
					go q.queuebMessageRoute(&qcp, m, done)
				} else {
					// do later
					m.counter = q.counter
					q.counter++
					heap.Push(q.pq, m)
				}
			case _, ok := <-done:
				if !ok {
					// disconnected.
					return
				}
				q.pending--
				if q.pq.Len() > 0 {
					// start sinking from priority queue
					m, ok := heap.Pop(q.pq).(*QueuebMessage)
					if !ok {
						panic("can't assert *QueuebMessage type")
					}
					q.pending++
					go q.queuebMessageRoute(&qcp, m, done)
				}
			}
		}
	}()

	return nil
}

func (q *Queueb) Unregister(name string) {
	q.queuebsMtx.Lock()
	defer q.queuebsMtx.Unlock()

	qcp, err := q.findQueuebChannelPair(name)
	if err != nil {
		return
	}

	if atomic.AddInt32(&qcp.stopped, 1) != 1 {
		return
	}

	close(qcp.From)
	defer qcp.wg.Wait()
	close(qcp.To)
	delete(q.Queuebs, name)
}
