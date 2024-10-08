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
	list *list.List
}

type message struct {
	data []byte
	pipe pipeBody
}

//Initializes a new chanQueue with a doubly linked list
//As its underlying data type
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

func (cq *chanQueue) isEmpty() bool {
	return cq.list.Len() == 0 
}

//Listen to queue waits for channel in front to be ready
//cq will listen until it can read from front.
//Once it reads from front, it will pass to nex pipe
func listenQueue(cq *chanQueue, done <-chan struct{}) {
	for {
		if cq.isEmpty() {
			//Should only return when queue is empty
			//When queue is empty it means there are no handlers running
			select {
			case <-done:
				return
			default:
				continue
			}
		}
		select {
		case v := <-cq.list.Front().Value.(chan message):
			passThroughSync(v.pipe, v.data)
			cq.dequeue()
		default:
		}
	}
}

func listenQueueTail(cq *chanQueue, done <-chan struct{}) {
	for {
		if cq.isEmpty() {
			//Should only return when queue is empty
			//When queue is empty it means there are no handlers running
			select {
			case <-done:
				return
			default:
				continue
			}
		}
		select {
		case v := <-cq.list.Front().Value.(chan message):
			passThroughOutSync(v.pipe, v.data, nil)
			cq.dequeue()
		default:
		}
	}
}
