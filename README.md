queueb
=======

Package queueb (pronounced cube) is a priority queue based messaging system.

All messages are sent as they come in until the set number of pending
messages is exhausted.
At that point a priority queue is used to schedule messages for later
delivery.
Higher priority queues are handled completely before lower priority ones.
The priority queue is automatically drained.

The receiver must listen on the return channel in order to catch errors.
Failing to do so will result in deadlock panics.

This code uses the liberal ISC license.
