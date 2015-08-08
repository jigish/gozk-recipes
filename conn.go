package recipes

import (
	"errors"
	"fmt"
	gozk "launchpad.net/gozk"
	"time"
)

type ZkConn struct {
	*gozk.Conn
}

func GetZk(url string) (*ZkConn, <-chan gozk.Event, error) {
	return getZk(url, false)
}

func GetPanicingZk(url string) *ZkConn {
	zk, eventChan, _ := getZk(url, true)
	PanicConnection(eventChan)
	return zk
}

func WaitOnConnect(zkEventChan <-chan gozk.Event) {
	for {
		event := <-zkEventChan
		if !event.Ok() {
			err := errors.New(fmt.Sprintf("Zookeeper error: %s\n", event.String()))
			panic(err)
		}
		if event.Type == gozk.EVENT_SESSION {
			switch event.State {
			case gozk.STATE_EXPIRED_SESSION:
				panic("Zookeeper connection expired!")
			case gozk.STATE_AUTH_FAILED:
				panic("Zookeeper auth failed!")
			case gozk.STATE_CLOSED:
				panic("zookeeper connection closed!")
			case gozk.STATE_CONNECTED:
				return
			}
		}
	}
}

func PanicConnection(zkEventChan <-chan gozk.Event) {
	WaitOnConnect(zkEventChan)
	go func() {
		for {
			event := <-zkEventChan
			if !event.Ok() {
				err := errors.New(fmt.Sprintf("Zookeeper error: %s\n", event.String()))
				panic(err)
			}
			if event.Type == gozk.EVENT_SESSION {
				switch event.State {
				case gozk.STATE_EXPIRED_SESSION:
					panic("Zookeeper connection expired!")
				case gozk.STATE_AUTH_FAILED:
					panic("Zookeeper auth failed!")
				case gozk.STATE_CLOSED:
					panic("Zookeeper connection closed!")
				case gozk.STATE_CONNECTING:
					panic("Zookeeper reconnecting!")
				}
			}
		}
	}()
}

func getZk(url string, die bool) (zk *ZkConn, zkEventChan <-chan gozk.Event, err error) {
	conn, zkEventChan, err := gozk.Dial(url, time.Minute)
	zk = &ZkConn{conn}
	if err != nil && die {
		panic(err)
	}
	return
}
