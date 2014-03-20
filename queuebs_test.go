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
	"fmt"
	"sync"
	"testing"
)

const (
	Audio   = "audio"
	Network = "network"
)

var (
	q *Queueb
)

func TestQueueb(t *testing.T) {
	var err error
	q, err = New("myqueueb", 10)
	if err != nil {
		t.Error(err)
		return
	}

	err = q.Register("subsystem", 10)
	if err != nil {
		t.Error(err)
		return
	}

	// should fail
	err = q.Register("subsystem", 10)
	if err == nil {
		t.Error("duplicate QueuebChannelPair not detected")
		return
	}

	q.Unregister("subsystem")
	if q.Len() != 0 {
		t.Error("invalid QueuebChannelPair count")
		return
	}

	// should fail
	q.Unregister("subsystem")
	if err == nil {
		t.Error("QueuebChannelPair should not exist")
		return
	}
}

func TestQueuebMessage(t *testing.T) {
	err := q.Register(Audio, 10)
	if err != nil {
		t.Error(err)
		return
	}

	err = q.Register(Network, 20)
	if err != nil {
		t.Error(err)
		return
	}

	err = q.Send(Audio, []string{Network}, "Hello world!")
	if err != nil {
		t.Error(err)
		return
	}

	m, err := q.Receive(Network)
	if err != nil {
		t.Error(err)
		return
	}
	if m.Error() != nil {
		t.Error("unexpected error message")
		return
	}

	if m.Message.(string) != "Hello world!" {
		t.Error("invalid message")
		return
	}

	// make sure we don't crash
	q.Unregister(Audio)
	err = q.Send(Audio, []string{Network}, "Hello world!")
	// should fail
	if err == nil {
		t.Error("queue should have been deleted")
		return
	}
	_, err = q.Receive(Audio)
	// should fail
	if err == nil {
		t.Error("receive should have failed")
		return
	}

	err = q.Send(Network, []string{Audio}, "Hello world!")
	if err != nil {
		t.Error("source queue should exist")
		return
	}
	m, err = q.Receive(Network)
	// get error
	if err != nil {
		t.Error("receive error failed")
		return
	}

	// should fail
	if m.Error() == nil {
		t.Error("not an error message")
		return
	}

	mm, ok := m.Message.(*QueuebMessageError)
	if !ok {
		t.Error("could not type assert m")
		return
	}
	t.Log(mm.Error)
}

func TestQueuebPrioQueueSamePrio(t *testing.T) {
	err := q.Register(Audio, 10)
	if err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < 15; i++ {
		err := q.Send(Audio, []string{Network}, fmt.Sprintf("%v", i))
		if err != nil {
			t.Error(err)
			return
		}
	}
	for i := 0; i < 15; i++ {
		m, err := q.Receive(Network)
		if err != nil {
			t.Error(err)
			return
		}
		if m.Error() != nil {
			t.Error("unexpected error message")
			return
		}
		t.Log("got %v expected %v", m.Message, fmt.Sprintf("%v", i))
		if m.Message != fmt.Sprintf("%v", i) {
			t.Error("out of order", m.Message, fmt.Sprintf("%v", i))
			return
		}
	}
}

func TestQueuebPrioQueueDifferentPrio(t *testing.T) {
	for i := 0; i < 15; i++ {
		err := q.Send(Audio, []string{Network}, fmt.Sprintf("%v", i))
		if err != nil {
			t.Error(err)
			return
		}
	}
	for i := 15; i < 30; i++ {
		err := q.Send(Network, []string{Audio}, fmt.Sprintf("%v", i))
		if err != nil {
			t.Error(err)
			return
		}
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 15; i++ {
			m, err := q.Receive(Network)
			if err != nil {
				t.Error(err)
				return
			}
			if m.Error() != nil {
				t.Error("unexpected error message")
				return
			}
			t.Log("got %v expected %v", m.Message, fmt.Sprintf("%v", i))
			if m.Message != fmt.Sprintf("%v", i) {
				t.Error("out of order", m.Message, fmt.Sprintf("%v", i))
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 15; i < 30; i++ {
			m, err := q.Receive(Audio)
			if err != nil {
				t.Error(err)
				return
			}
			if m.Error() != nil {
				t.Error("unexpected error message")
				return
			}
			t.Log("got %v expected %v", m.Message, fmt.Sprintf("%v", i))
			if m.Message != fmt.Sprintf("%v", i) {
				t.Error("out of order", m.Message, fmt.Sprintf("%v", i))
				return
			}
		}
	}()
	wg.Wait()
}
