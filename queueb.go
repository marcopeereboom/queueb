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

const (
	AnonTag = "anonymous"
)

// QueuebMessageError is returned to sender if delivery does not occur.
// When an error occurs the callers receives a QueuebMessage and Message is set
// to QueuebMessageError.  The original Messages is returned as the Message in
// QueuebMessageError.  The sender is responsible for adding identification of
// the original Message, if need be.
type QueuebMessageError struct {
	Error   error       // error
	Message interface{} // original message
}

// QueuebMessage is the generic wrapper used to send messages around subsystems.
// All messages passed around are of type QueuebMessage.
// The sender is responsible for filling out From, To and Message.
// Tag is sent depending on which Send function is called.
// The Tag field is for the caller to be able to add an id of sorts to messages.
// Message is of type interface{} so the sender has flexibility in what to
// send.
// It is recommended to have an identifier in the Message in case one needs
// to deal with errors.
type QueuebMessage struct {
	From    string      // From queue
	To      []string    // To queue
	Message interface{} // Queueb agnostic message that is passed around
	Tag     string      // User specified tag, used for identification

	counter  uint64 // running counter to keep same prio in FIFO order
	priority int    // message priority, inherented from queuebChannelPair
}

// priorityQueue implements the heap interface.
type priorityQueue []*QueuebMessage

// Implement Len, required by the heap interface.
func (pq priorityQueue) Len() int {
	return len(pq)
}

// Implement Less, required by the heap interface.
// Note that this Less function has two sorting fields.
// The idea is to keep same priority messages in order while higher priority
// messages get pushed in front of lower priority ones.
func (pq priorityQueue) Less(i, j int) bool {
	if pq[i].priority == pq[j].priority {
		return pq[i].counter < pq[j].counter
	}

	return pq[i].priority > pq[j].priority
}

// Implement Swap, required by the heap interface.
func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

// Implement Push, required by the heap interface.
func (pq *priorityQueue) Push(x interface{}) {
	*pq = append(*pq, x.(*QueuebMessage))
}

// Implement Pop, required by the heap interface.
func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	x := old[n-1]
	*pq = old[0 : n-1]
	return x
}

// The queuebChannelPair strucutre is used internally to represent a subsytem.
// Each subsystem has bidirectional communication channels.
type queuebChannelPair struct {
	stopped  int32          // channel pair in the process of stopping
	wg       sync.WaitGroup // wait until done stopping
	priority int            // priority of this channel pair

	From chan *QueuebMessage // incomming channel
	To   chan *QueuebMessage // outgoing channel
	Name string              // queue name
}

// The Queueb (pronounced cube) type is a context for N queuebChannelPairs.
// Messages can be exchanged between queuebChannelPairs, a.k.a. subsystems.
// Under normal use messages are sent directly to the other subsystem however,
// if the pending counter reaches depth the messages are pushed onto a
// priority queue.
// The higher the priority of the queue the earlier their messages get processed.
// Once a message gets delivered the highest priority messages is popped of the
// queue and is sent.
// This process continues until the queue is fully drained.
// Name is only used for human readability of various contexts.
type Queueb struct {
	Name string // queue context name

	pq         *priorityQueue // used to handle overflow messages
	depth      uint           // depth of sink before queueing
	pending    uint           // number of outstanding messages
	counter    uint64         // running counter, yep it can overflow
	queuebsMtx sync.RWMutex   // mutex for queuebs
	dataMtx    sync.Mutex     // mutex for fields in Queueb

	queuebs map[string]*queuebChannelPair // registered queues
}

// New allocates a Queueb context.
// Name is a human readable string to identify the context.
// It is not used in the package.
// The depth parameter indicates how many messages can be outstanding before
// messages are going to be pushed onto the priority queue.
// Note that depth measures From messages only; this means that if there are
// M recipients for the message it only counts once.
func New(name string, depth uint) (*Queueb, error) {
	q := Queueb{
		Name:    name,
		queuebs: make(map[string]*queuebChannelPair),
		depth:   depth,
		pq:      &priorityQueue{},
	}

	heap.Init(q.pq)

	return &q, nil
}

// Route message to recipients as they come in.
// This function must be called as a go routine without the mutex held.
func (q *Queueb) queuebMessageRoute(qcp *queuebChannelPair, m *QueuebMessage,
	done chan bool) {
	q.queuebsMtx.RLock()
	defer q.queuebsMtx.RUnlock()

	for _, v := range m.To {
		to, found := q.queuebs[v]
		if found {
			to.To <- m
			continue
		}

		// route error back to caller
		to, found = q.queuebs[m.From]
		if found {
			// reuse m
			e := &QueuebMessageError{
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

// Find the named queue.
// Mutex must be held when calling this function.
func (q *Queueb) findQueuebChannelPair(name string) (*queuebChannelPair, error) {
	qcp, found := q.queuebs[name]
	if !found {
		return nil, fmt.Errorf("queueb not found: %v", name)
	}

	if atomic.LoadInt32(&qcp.stopped) != 0 {
		return nil, fmt.Errorf("queueb stopped: %v", name)
	}

	return qcp, nil
}

// generic send routine.
func (q *Queueb) send(name, tag string, to []string, msg interface{}) error {
	m := QueuebMessage{
		From:    name,
		To:      to,
		Tag:     tag,
		Message: msg,
	}

	q.queuebsMtx.RLock()
	defer q.queuebsMtx.RUnlock()

	qcp, err := q.findQueuebChannelPair(name)
	if err != nil {
		return err
	}

	m.priority = qcp.priority // set priority based on queuebChannelPair

	qcp.From <- &m

	return nil
}

// Len returns the number currently registered queuebs.
func (q *Queueb) Len() int {
	q.queuebsMtx.RLock()
	defer q.queuebsMtx.RUnlock()
	return len(q.queuebs)
}

// Send message "msg" from "name" queueb to "to" queuebs.
// This function is non blocking.
func (q *Queueb) Send(name string, to []string, msg interface{}) error {
	return q.send(name, AnonTag, to, msg)
}

// Send message "msg" from "name" queueb to "to" queuebs with tag.
// This function is non blocking.
func (q *Queueb) SendTagged(name, tag string, to []string,
	msg interface{}) error {
	return q.send(name, tag, to, msg)
}

// Receive message from "name" queueb.
// This function is blocks.
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

// This function determines if the received messages was a QueuebMessageError.
// If it is a QueuebMessageError then return the embedded error.
func (qm *QueuebMessage) Error() error {
	m, ok := qm.Message.(*QueuebMessageError)
	if !ok {
		return nil
	}
	return m.Error
}

// Register new queueb named "name" with priority "priority".
// Higher priority queuebs are handled before lower priority ones until fully
// drained.
// The queueb name MUST be unique and is used to identify the queueb.
func (q *Queueb) Register(name string, priority int) error {
	qcp := queuebChannelPair{
		Name:     name,
		From:     make(chan *QueuebMessage),
		To:       make(chan *QueuebMessage),
		priority: priority,
	}

	q.queuebsMtx.Lock()
	defer q.queuebsMtx.Unlock()

	_, found := q.queuebs[name]
	if found {
		return fmt.Errorf("queue already exists: %v", name)
	}
	q.queuebs[name] = &qcp

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
				q.dataMtx.Lock()
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
				q.dataMtx.Unlock()
			case _, ok := <-done:
				if !ok {
					// disconnected.
					return
				}
				q.dataMtx.Lock()
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
				q.dataMtx.Unlock()
			}
		}
	}()

	return nil
}

// Unregister immediately destroys the named queueb and removes it from the
// context.
// Messages will no longer be routed and the queueb is NOT drained.
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
	delete(q.queuebs, name)
}
