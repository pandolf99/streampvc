//test stream interface
package streampvc_test

import (
	"fmt"
	"os"
	"testing"
	//"time"

	"github.com/pandolf99/streampvc"
)

func TestStream(t *testing.T) {
	sm := streampvc.NewStreamManager()
	conId, err := sm.AddConn("wss://echo.websocket.org")
	if err != nil {
		t.Fatal(err)
	}
	sm.StreamFromTo(conId, os.Stdout)
	for i := 0; i < 10; i++ {
		sm.WriteTextTo(conId, []byte(fmt.Sprintf("hello %v\n", i)))
	}
	done := make(chan struct{})
	sm.CloseStream(conId, done)
	<-done
	return
}
