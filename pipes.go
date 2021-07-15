//Defines pipe primitives, and interfaces
package streampvc

import (
	"errors"
	"log"
)

type pipeHandler func([]byte) []byte

//General Pipe interface
//WriteToMethod starts streaming through the pipe
type Piper interface {
	WriteToPipe([]byte) error
	closePipe()
}

//Interface that pipe building blocks implement
//A pipe can be made of different pipeBodys
//pipe body should be pointer to pipe or pipeWQ
type pipeBody interface {
	build(pipeHandler) pipeBody
}

//TODO can pipe be array instead of linked list
//Simple pipe only has a handler a pointer to next
type pipe struct {
	handle pipeHandler
	next   pipeBody
}

func (p *pipe) build(h pipeHandler) pipeBody {
	pb := pipeBody(&pipe{handle: h})
	return pb
}

//Pipe with Queue has a listening channel
type pipeWQ struct {
	handle      pipeHandler
	handleQueue *chanQueue
	done        chan struct{} //Close when done
	next        pipeBody
}

func (p *pipeWQ) build(h pipeHandler) pipeBody {
	return pipeBody(&pipeWQ{
		handleQueue: newChanQueue(),
		done:        make(chan struct{}),
		handle:      h})
}

//Only cares about Head
type AsyncPipe struct {
	Head pipeBody
}

//Sync only happens at end of pipe
type OutSyncPipe struct {
	Head pipeBody
	//Keep pointer to tail to access chan_queue
	Tail pipeBody
}

//Sync happens at every step of pipe
type SyncPipe struct {
	Head pipeBody
}

const (
	AsyncP   = "AsyncPipe"
	OutSyncP = "OutSyncPipe"
	SyncP    = "SyncPipe"
)

//Factory to choose which pipe to build
//Returns an error if pipe is of invalid type
func BuildPipe(pipe string, funcs ...func([]byte) []byte) (Piper, error) {
	if len(funcs) == 0 {
		return nil, errors.New("Must have at least one pipe")
	}
	switch pipe {
	case AsyncP:
		return buildAsyncPipe(funcs...), nil
	case SyncP:
		return buildSyncPipe(funcs...), nil
	case OutSyncP:
		return buildOutSyncPipe(funcs...), nil
	default:
		return nil, errors.New("Invalid pipe type")
	}
}

//An outSync Pipe only has a listening queue at the end of the pipe
//Last Pipe is always executed serially
func buildOutSyncPipe(funcs ...func([]byte) []byte) Piper {
	pipeHead := new(pipe).build(funcs[0])
	currPipe := pipeHead.(*pipe)
	for i := 1; i < len(funcs)-1; i++ {
		currPipe.next = currPipe.build(funcs[i])
		currPipe = currPipe.next.(*pipe)
	}
	//Only tail has listening queue
	//done channel closes listening routine
	//TODO check if len == 1
	currPipe.next = new(pipeWQ).build(funcs[len(funcs)-1])
	tail := currPipe.next.(*pipeWQ)
	tail.done = make(chan struct{})
	tail.handleQueue = newChanQueue()
	os_pipe := OutSyncPipe{
		Head: pipeHead,
		Tail: tail}
	go listenQueueTail(tail.handleQueue, tail.done)
	return &os_pipe
}

//A SyncPipe maintains order at every step of the pipe
//Memory heavy but safe
//Passing between pipes is done serially
//But handling is done concurrently
//Last pipe is always executed serially
func buildSyncPipe(funcs ...func([]byte) []byte) Piper {
	pipeHead := new(pipeWQ).build(funcs[0])
	currPipe := pipeHead.(*pipeWQ)
	for i := 1; i < len(funcs); i++ {
		//Create a routine that listens to queue
		//for each func create a rountine
		go listenQueue(currPipe.handleQueue, currPipe.done)
		currPipe.next = currPipe.build(funcs[i])
		currPipe = currPipe.next.(*pipeWQ)
	}
	//tail has no listening queue
	return &SyncPipe{Head: pipeHead}
}

//An AsyncPipe has no ordering at all
//Messages are processed ephemerally by go routines.
func buildAsyncPipe(funcs ...func([]byte) []byte) Piper {
	pipeHead := new(pipe).build(funcs[0])
	currPipe := pipeHead.(*pipe)
	for i := 1; i < len(funcs); i++ {
		currPipe.next = new(pipe).build(funcs[i])
		currPipe = currPipe.next.(*pipe)
	}
	return &AsyncPipe{Head: pipeHead}
}

//Sends signal to all listening go routines
//to return

//Write to first pipe body
//Pipe then propagates on its own
func (asp *AsyncPipe) WriteToPipe(b []byte) error {
	passThroughAsync(asp.Head, b)
	//TODO How to handle error
	return nil
}

func (asp *AsyncPipe) closePipe() {}

//Async Write
//If one routine is taking longer than others,
//Streaming order will be lost
//Might be useful when order is not necesary
func passThroughAsync(pb pipeBody, b []byte) {
	next := pb.(*pipe).next
	//If end of pipe just execute handler
	if next == nil {
		go func() {
			pb.(*pipe).handle(b)
		}()
		return
	}
	//Pass to next pipe as soon as pf.handle is done
	go func() {
		passThroughAsync(next, pb.(*pipe).handle(b))
	}()
	return
}

//WriteTo method for OutSyncPipe
func (osp *OutSyncPipe) WriteToPipe(b []byte) error {
	//Enqueing here guarantees order is maintained
	ch := make(chan message)
	osp.Tail.(*pipeWQ).handleQueue.enqueue(ch)
	//Need to passthrough with ch so penultimate writes to handler
	passThroughOutSync(osp.Head, b, ch)
	//TODO How to handle error
	return nil
}

//Close the tail
func (osp *OutSyncPipe) closePipe() {
	//Wait till queue is empty
	//Should be fast since stream is closed
	//Only need to close tail
	for !osp.Tail.(*pipeWQ).handleQueue.isEmpty() {
	}
	close(osp.Tail.(*pipeWQ).done)
	log.Println("Finished all pipe handlers")
}

//Recursive function for outSyncPipe
func passThroughOutSync(pf pipeBody, b []byte, ch chan<- message) {
	switch pf.(type) {
	//if pf is pwq, its tail
	case *pipeWQ:
		pf.(*pipeWQ).handle(b)
		return
	}
	//else its pipe
	nextPipe := pf.(*pipe).next
	switch nextPipe.(type) {
	//If next is pwq, its tail
	//so write to chail channel to sync
	case *pipeWQ:
		go func() {
			msg := message{
				data: pf.(*pipe).handle(b),
				pipe: nextPipe}
			ch <- msg
			close(ch)
		}()
	case *pipe:
		go func() {
			passThroughOutSync(nextPipe, pf.(*pipe).handle(b), ch)
		}()
	}
	return
}

func (sp *SyncPipe) WriteToPipe(b []byte) error {
	passThroughSync(sp.Head, b)
	//TODO How to handle error
	return nil
}

func (sp SyncPipe) closePipe() {
	//TODO Make sure data is not lost
	//Check for length of queue
	for cp := sp.Head; cp.(*pipeWQ).next != nil; cp = cp.(*pipeWQ).next {
		//Wait till queue is empty
		//Should be fast since stream is closed
		for !cp.(*pipeWQ).handleQueue.isEmpty() {
		}
		close(cp.(*pipeWQ).done)
	}
	log.Println("Finished all pipe handlers")
}

//Pass Through function for SyncPipe
func passThroughSync(pb pipeBody, b []byte) {
	nextP := pb.(*pipeWQ).next
	//If end of pipe just execute handler
	//Cant be concurrent to maintain order
	if nextP == nil {
		pb.(*pipeWQ).handle(b)
		return
	}
	ch := make(chan message)
	pb.(*pipeWQ).handleQueue.enqueue(ch) //Binds the channel to pf
	go func() {
		msg := message{
			data: pb.(*pipeWQ).handle(b),
			pipe: nextP}
		ch <- msg
		close(ch)
	}()
}
