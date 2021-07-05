//package streampvc provides interface to manage a suite of streams.
//Each stream si guaranteed to run on an independent go routime
// Easy to pipe handlers to the stream
//TODO think about if want pool stream chache
//Maintains a cache of recent connections
package streampvc

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type ConnId string

const MAXCONS = 10

type connection struct {
	socket   *websocket.Conn
	done     chan struct{} //signals to calling routine when done
	canWrite chan struct{} //Signals when connection is writing
	to *streamPipe //Pipe that this connection is writing
}

type streamManager struct {
	connections        map[string]*connection
	runningConnections []connection
	numConnections     int
}

//Returns a zero initialized streamManager
func NewStreamManager() *streamManager {
	sm := new(streamManager)
	sm.connections = make(map[string]*connection)
	sm.runningConnections = make([]connection, MAXCONS)
	return sm
}

var mu sync.RWMutex

//Add a connection to the stream mananger
//Dissalows duplicate connections
//Returns error Dial is not succesfull
func (sm *streamManager) AddConn(stream_name string) (ConnId, error) {
	if sm.numConnections >= MAXCONS {
		return ConnId(""), errors.New("Aready at max connections")
	}
	if _, exists := sm.connections[stream_name]; exists {
		return ConnId(""), errors.New("This connection already exists")
	}
	log.Printf("connecting to %s", stream_name)
	//TODO handle response from dial
	c, _, err := websocket.DefaultDialer.Dial(stream_name, nil)
	if err != nil {
		return ConnId(""), err
	}
	//Buffered channel ensures write to stream is
	//made one at a time
	conn := &connection{
		done:     make(chan struct{}),
		canWrite: make(chan struct{}, 1),
		socket:   c}
	conn.canWrite <- struct{}{}
	mu.Lock()
	sm.connections[stream_name] = conn
	mu.Unlock()
	sm.numConnections++
	return ConnId(stream_name), nil
}

//Initializes the go routine reading from stream writing to out
//Read until web socket closes or when routine receives close
//XXX DO NOT CALL IN GO ROUTINE
func (sm *streamManager) StreamFromTo(stream_name ConnId, out *streamPipe) {
	//TODO
	//Check if connections is active first
	//If active, return error
	go func() {
		conn := sm.connections[string(stream_name)]
		conn.to = out
		for {
			select {
			case <-conn.done:
				return
			default:
				_, message, err := conn.socket.ReadMessage()
				if err != nil {
					log.Println("read:", err)
					//TODO handle read error from connection
					//Could be reading from a closed channel
					//close(conn.done)
				}
				//If out is of type Pipe, handle
				_, err = out.Write(message)
				if err != nil {
					log.Println("Write from stream", err)
				}
			}
		}
	}()
}

//Internal function to handle concurrent writes
func write(conn connection, msg_type int, msg []byte) error {
	//wait until conn finishes writing
	<-conn.canWrite
	conn.socket.SetWriteDeadline(time.Now().Add(time.Second*1))
	err := conn.socket.WriteMessage(websocket.TextMessage, msg)
	if err != nil {
		log.Println("write error:", err)
		return err
	}
	conn.canWrite <- struct{}{}
	return nil
}

//Sequential wrapper for async write
func (sm *streamManager) WriteTextTo(streamId ConnId, msg []byte) error {
	conn := sm.connections[string(streamId)]
	//TODO check if connections is active

	//internal write through channel
	//used to buffer writes to conn
	go write(*conn, websocket.TextMessage, msg)
	return nil
}

func (sm *streamManager) WriteBinTo(streamId ConnId, msg []byte) error {
	conn := sm.connections[string(streamId)]
	//TODO check if connections is active

	//blocks until conn can read
	//Todo check if can write to comm
	//internal write through channel
	//used to buffer writes to conn
	go write(*conn, websocket.BinaryMessage, msg)
	return nil
}

//Stops go routine reading from stream
//Remove streams from active connections
func (sm *streamManager) StopStream(streamId ConnId) {
	//TODO
	//add a close handler
	conn := sm.connections[string(streamId)]
	close(conn.done)
}

//Sends singal to isDone when stream is safely closed
//User must make that all writing is done
//TODO eventually handle this with a queue
func (sm *streamManager) CloseStream(streamId ConnId) chan struct{} {
	//close stream with wait
	done := make(chan struct{})
	go func () { 
		conn := sm.connections[string(streamId)]
		close(conn.done)    //Signals reading routine
		conn.to.closePipe() //Blocks until all messages are processed
		defer delete(sm.connections, string(streamId))
		defer close(done) //Signal Calling process
		write(*conn,
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	}()
	return done
}
