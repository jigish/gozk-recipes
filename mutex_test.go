package recipes

import (
	. "launchpad.net/gocheck"
	"time"
)

func tryToLock(c *C, path string, doneChan chan bool) {
	m := NewMutex(zk.Conn, path)
	c.Assert(m.Lock(), IsNil)
	c.Assert(m.Unlock(), IsNil)
	doneChan <- true
}

func (s *RecipesSuite) TestMutex(c *C) {
	zk.RecursiveDelete("/test")
	zk.Touch("/test/testmutex")
	m := NewMutex(zk.Conn, "/test/testmutex")
	c.Assert(m.Lock(), IsNil)
	doneChan := make(chan bool)
	go tryToLock(c, "/test/testmutex", doneChan)
	timerChan := time.After(5*time.Second)
	select {
	case <-doneChan:
		c.Error("should not be able to lock")
	case <-timerChan:
	}
	c.Assert(m.Unlock(), IsNil)
	timerChan = time.After(5*time.Second)
	select {
	case <-doneChan:
	case <-timerChan:
		c.Error("should be able to lock")
	}
}

func tryToRLock(c *C, path string, doneChan chan bool) {
	m := NewRWMutex(zk.Conn, path)
	c.Assert(m.RLock(), IsNil)
	c.Assert(m.RUnlock(), IsNil)
	doneChan <- true
}

func tryToWLock(c *C, path string, doneChan chan bool) {
	m := NewRWMutex(zk.Conn, path)
	c.Assert(m.Lock(), IsNil)
	c.Assert(m.Unlock(), IsNil)
	doneChan <- true
}

func (s *RecipesSuite) TestRWMutex(c *C) {
	// Hold write lock, try to write lock
	zk.RecursiveDelete("/test")
	zk.Touch("/test/testrwmutex")
	m := NewRWMutex(zk.Conn, "/test/testrwmutex")
	c.Assert(m.Lock(), IsNil)
	doneChan := make(chan bool)
	go tryToWLock(c, "/test/testrwmutex", doneChan)
	timerChan := time.After(5*time.Second)
	select {
	case <-doneChan:
		c.Error("should not be able to lock")
	case <-timerChan:
	}
	c.Assert(m.Unlock(), IsNil)
	timerChan = time.After(5*time.Second)
	select {
	case <-doneChan:
	case <-timerChan:
		c.Error("should be able to lock")
	}

	// Hold write lock, try to read lock
	zk.RecursiveDelete("/test")
	zk.Touch("/test/testrwmutex")
	m = NewRWMutex(zk.Conn, "/test/testrwmutex")
	c.Assert(m.Lock(), IsNil)
	doneChan = make(chan bool)
	go tryToRLock(c, "/test/testrwmutex", doneChan)
	timerChan = time.After(5*time.Second)
	select {
	case <-doneChan:
		c.Error("should not be able to lock")
	case <-timerChan:
	}
	c.Assert(m.Unlock(), IsNil)
	timerChan = time.After(5*time.Second)
	select {
	case <-doneChan:
	case <-timerChan:
		c.Error("should be able to lock")
	}

	// Hold read locks, try to write lock
	zk.RecursiveDelete("/test")
	zk.Touch("/test/testrwmutex")
	m = NewRWMutex(zk.Conn, "/test/testrwmutex")
	c.Assert(m.RLock(), IsNil)
	m2 := NewRWMutex(zk.Conn, "/test/testrwmutex")
	c.Assert(m2.RLock(), IsNil)
	doneChan = make(chan bool)
	go tryToWLock(c, "/test/testrwmutex", doneChan)
	timerChan = time.After(5*time.Second)
	select {
	case <-doneChan:
		c.Error("should not be able to lock")
	case <-timerChan:
	}
	c.Assert(m.RUnlock(), IsNil)
	timerChan = time.After(5*time.Second)
	select {
	case <-doneChan:
		c.Error("should not be able to lock")
	case <-timerChan:
	}
	c.Assert(m2.RUnlock(), IsNil)
	timerChan = time.After(5*time.Second)
	select {
	case <-doneChan:
	case <-timerChan:
		c.Error("should be able to lock")
	}
}
