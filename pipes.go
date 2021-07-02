//Defines pipe primitives, and interfaces
package streampvc

import (
	"errors"
)

type pipeHandler func([]byte) []byte

type streamPipe struct {
	head *pipeBody
}

type pipeBody struct {
	handle      pipeHandler
	NextPipe    *pipeBody
	handleQueue *chanQueue
}

//Initialize a new pipe body that will
//handle data with f
func NewPipeBody(f func([]byte) []byte) *pipeBody {
	return &pipeBody{
		handle:      f,
		NextPipe:    nil}
}

//Write to first pipe body
//Pipe then propagates on its own
func (sp *streamPipe) Write(b []byte) (int, error) {
	passThrough(sp.head, b)
	//TODO How to handle error
	return len(b), nil
}


//Helper function to rapidly construct pipe
func BuildPipe(funcs ...func([]byte) []byte) (*streamPipe, error) {
	if len(funcs) == 0 {
		return nil, errors.New("Must have at least one pipe")
	}
	pipeHead := NewPipeBody(funcs[0])
	currPipe := pipeHead
	for i := 1; i < len(funcs); i++ {
		currPipe.NextPipe = NewPipeBody(funcs[i])
		//Make the queue write to next
		cq := newChanQueue()
		currPipe.handleQueue = cq
		go cq.listenQueue()
		currPipe = currPipe.NextPipe
	}
	return &streamPipe{head: pipeHead}, nil
}

//Async Write
//If one routine is taking longer than others,
//Streaming order will be lost
//Might be useful when 
func passThrough2(pf *pipeBody, b []byte) {
	next := pf.NextPipe
	//If end of pipe just execute handler
	if next == nil {
		go func(data []byte) {
			pf.handle(data)
		}(b)
		return
	}
	go func(next *pipeBody, data []byte) {
		passThrough(next, data)
	}(next, pf.handle(b))
	return
}

//Pass bytes b through pipeBody
func passThrough(pf *pipeBody, b []byte) {
	next := pf.NextPipe
	//If end of pipe just execute handler
	if next == nil {
		pf.handle(b)
		return
	}
	go func() {
		ch := make(chan message)
		pf.handleQueue.enqueue(ch)
		msg := message{
			data: pf.handle(b),
			pipe: next}
		ch <- msg
		close(ch)
	}()
}
