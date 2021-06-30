package streampvc

// Defines the chan_queue type
// this is used to maintain order
// when reading from the streaming server

import (
	"container/list"
	"fmt"
	"strings"
)

//list is underlying implementation of a queue
//handler is the function that listenQueue
//will call when it dequeues
type chanQueue struct {
	list    *list.List
}

type message struct {
	data []byte
	pipe *pipeBody
}

//Initializes a new chanQueue with a doubly linked list
//As its underlying data type
//Dont intitialize handler as it is unknown at creation time
func newChanQueue() *chanQueue {
	return &chanQueue{
		list: list.New()}
}

//For debugging purposes
func (cq chanQueue) String() string {
	var b strings.Builder
	for e := cq.list.Front(); e != nil; e = e.Next() {
		b.WriteString(fmt.Sprintf("%v<-", e.Value))
	}
	return b.String()
}

//Adds to back of queue
func (cq *chanQueue) enqueue(chanM chan message) chan message {
	e := cq.list.PushBack(chanM)
	return e.Value.(chan message)
}

//Removes from front of queue
func (cq *chanQueue) dequeue() chan message {
	front := cq.list.Front()
	v := cq.list.Remove(front)
	return v.(chan message)
}

//Listen to queue waits for channel in front to be ready
//cq will listen until it can read from front.
//Once it reads from front, it will pass to nex pipe
func (cq *chanQueue) listenQueue() {
	for {
		if cq.list.Len() == 0 {
			continue
		}
		select {
		case v := <-cq.list.Front().Value.(chan message):
			passThrough(v.pipe, v.data)
			cq.dequeue()
		default:
		}
	}
}
