//test stream interface
package streampvc_test

import (
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pandolf99/streampvc"
)

//func TestStream(t *testing.T) {
	//sm := streampvc.NewStreamManager()
	//s := timeStreamServer()
	//defer s.Close()
	//u := "ws" + strings.TrimPrefix(s.URL, "http")
	//conId, err := sm.AddConn(u)
	//if err != nil {
		//t.Fatal(err)
	//}
	//sm.StreamFromTo(conId, os.Stdout)
	//done := make(chan struct{})
	//time.Sleep(time.Second * 30)
	//sm.CloseStream(conId, done)
	//<-done
	//return
//}

func TestPiping(t *testing.T) {
	sm := streampvc.NewStreamManager()
	s := intStreamServer()
	defer s.Close()
	u := "ws" + strings.TrimPrefix(s.URL, "http")
	conId, err := sm.AddConn(u)
	if err != nil {
		t.Fatal(err)
	}
	h1 := func(b []byte) []byte {
		i, _ := strconv.Atoi(string(b))
		i++
		return []byte(strconv.Itoa(i))
	}
	h2 := func(b []byte) []byte {
		i, _ := strconv.Atoi(string(b))
		if i%3 == 0{
			time.Sleep(time.Second*2)
		}
		i++
		return []byte(strconv.Itoa(i))
	}
	h3 := func(b []byte) []byte {
		os.Stdout.Write(b)
		return b
	}
	ph, err := streampvc.BuildPipe(h1, h2, h3)
	if err != nil {
		t.Fatal("Could not build pipe")
	}
	sm.StreamFromTo(conId, ph)
	done := make(chan struct{})
	time.Sleep(time.Second * 7)
	sm.CloseStream(conId, done)
	<-done
	return
}
