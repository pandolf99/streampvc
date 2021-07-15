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

func TestPipe(t *testing.T) {
	sm := streampvc.NewStreamManager()
	s := intStreamServer()
	defer s.Close()
	u := "ws" + strings.TrimPrefix(s.URL, "http")
	h1 := func(b []byte) []byte {
		i, _ := strconv.Atoi(string(b))
		i++
		return []byte(strconv.Itoa(i))
	}
	h2 := func(b []byte) []byte {
		i, _ := strconv.Atoi(string(b))
		if i%3 == 0 {
			time.Sleep(time.Second * 2)
		}
		i++
		return []byte(strconv.Itoa(i))
	}
	h3 := func(b []byte) []byte {
		os.Stdout.Write(b)
		return b
	}
	pipes := []string{"SyncPipe", "AsyncPipe", "OutSyncPipe"}
	for _, p := range pipes {
		t.Run(p, func(t *testing.T) {
			conId, err := sm.AddConn(u)
			if err != nil {
				t.Errorf("Could not connect in %s", p)
				return
			}
			ph, err := streampvc.BuildPipe(p, h1, h2, h3)
			if err != nil {
				t.Errorf("Could not build %s", p)
				return
			}
			sm.StreamFromTo(conId, ph)
			time.Sleep(time.Second * 7)
			//Measure how long they take to close
			defer func(start time.Time, name string) {
				elapsed := time.Since(start)
				t.Logf("%s took %s to close", name, elapsed)
			}(time.Now(), p)
			done := sm.CloseStream(conId)
			<-done
		})

	}
	return
}
