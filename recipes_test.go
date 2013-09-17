package recipes

import (
	. "launchpad.net/gocheck"
	"testing"
)

func TestRecipes(t *testing.T) { TestingT(t) }

type RecipesSuite struct{}

var _ = Suite(&RecipesSuite{})

var (
	zkServer *ZkTestServer
	zk *ZkConn
)

func (s *RecipesSuite) SetUpSuite(c *C) {
	zkServer = NewZkTestServer()
	if err := zkServer.Init(); err != nil {
		c.Errorf("could not set up test zk server: %v", err)
	}
	zk = zkServer.Zk
}

func (s *RecipesSuite) TearDownSuite(c *C) {
	if err := zkServer.Destroy(); err != nil {
		c.Errorf("could not destroy zk server: %v", err)
	}
}
