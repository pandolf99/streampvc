package streampvc_test

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{}

func timeStreamServer() *httptest.Server {
	handler := func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		ticker := time.NewTicker(time.Second * 3)
		for _ = range ticker.C {
			date := time.Now().Format("Mon Jan 2 15:04:05")
			date += "\n"
			err = c.WriteMessage(websocket.TextMessage, []byte(date))
			if err != nil {
				break
			}
		}
	}
	return httptest.NewServer(http.HandlerFunc(handler))
}

func intStreamServer() *httptest.Server {
	handler := func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		for i := 0; ; i++ {
			istr := strconv.Itoa(i)
			err = c.WriteMessage(websocket.TextMessage, []byte(istr))
			if err != nil {
				break
			}
			time.Sleep(time.Second * 1)
		}
	}
	return httptest.NewServer(http.HandlerFunc(handler))
}
