//package streampvc provides interface to manage a suite of streams.
//Each stream is guaranteed to run on an independent go routime
// Easy to pipe handlers to the stream
//TODO think about if want pool stream chache
//Maintains a cache of recent connections
package streampvc

import (
	"errors"
	"io"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type ConnId string

const MAXCONS = 10

type connection struct {
	socket  *websocket.Conn
	comm    chan []byte   //Channel to write to socket
	writing chan bool     //signals when it is writing
	done    chan struct{} //signals
}

type streamManager struct {
	connections        map[string]connection
	runningConnections []connection
	numConnections     int
}

//Returns a zero initialized streamManager
func NewStreamManager() *streamManager {
	sm := new(streamManager)
	sm.connections = make(map[string]connection)
	sm.runningConnections = make([]connection, MAXCONS)
	return sm
}

var mu sync.Mutex

//Add a connection to the stream mananger
//Dissalows duplicate connections
//Returns error Dial is not succesfull
func (sm *streamManager) AddConn(stream_name string) (ConnId, error) {
	mu.Lock()
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
	conn := connection{
		comm:    make(chan []byte, 1),
		done:    make(chan struct{}),
		writing: make(chan bool),
		socket:  c}
	conn.writing <-false
	sm.connections[stream_name] = conn
	sm.numConnections++
	mu.Unlock()
	return ConnId(stream_name), nil
}

//Initializes the go routine reading from stream writing to out
//Read until web socket closes or when routine receives close
//XXX DO NOT CALL IN GO ROUTINE
func (sm *streamManager) StreamFromTo(stream_name ConnId, out io.Writer) {
	//TODO
	//Check if connections is active first
	//If active, return error
	//Think if this is secure for go routines
	go func() {
		conn := sm.connections[string(stream_name)]
		for {
			select {
			case <-conn.done:
				log.Println("Exiting routine")
				return
			default:
				_, message, err := conn.socket.ReadMessage()
				if err != nil {
					log.Println("read:", err)
					//TODO handle read error from connection
					//Could be calling on a closed channel
					//close(conn.done)
				}
				_, err = out.Write(message)
				if err != nil {
					log.Println("Write from stream", err)
				}
			}
		}
	}()
}

//Internal function to handle concurrent writes
func write(conn connection, msg_type int) error {
	//Wait until writing is false
	for {
		select {
		case writing, _ := <- conn.writing:
			if writing {
				continue
			}else {
				val, ok := <-conn.comm
				conn.writing <- true
				if !ok {
					return errors.New("Writing to closed channel")
				}
				err := conn.socket.WriteMessage(websocket.TextMessage, val)
				if err != nil {
					log.Println("write error:", err)
					return err
				}
				conn.writing <- false
				return nil
			}
		default:
			continue
		}
	}
}

//Write through channel
func (sm *streamManager) WriteTextTo(streamId ConnId, msg []byte) error {
	log.Println("here")
	conn := sm.connections[string(streamId)]
	//TODO check if connections is active

	//blocks until conn can read
	//Todo check if can write to comm
	//internal write through channel
	//used to buffer writes to conn
	conn.comm <- msg
	go write(conn, websocket.TextMessage)
	return nil
}

func (sm *streamManager) WriteBinTo(streamId ConnId, msg []byte) error {
	conn := sm.connections[string(streamId)]
	//TODO check if connections is active

	//blocks until conn can read
	//Todo check if can write to comm
	//internal write through channel
	//used to buffer writes to conn
	go write(conn, websocket.BinaryMessage)
	conn.comm <- msg
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

//NOT safe for concurrency
//Sends singal to isDone when stream is safely closed
func (sm *streamManager) CloseStream(streamId ConnId, isDone chan struct{}) error {
	conn := sm.connections[string(streamId)]
	close(conn.done)    //Signals reading routine
	defer close(isDone) //signals callling process
	defer delete(sm.connections, string(streamId))
	//close stream with wait
	go write(conn, websocket.CloseMessage)
	conn.comm <- websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")
	log.Println("Waiting")
	time.Sleep(time.Second * 1)
	return nil
}
