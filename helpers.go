package recipes

import (
	"errors"
	"fmt"
	gozk "launchpad.net/gozk"
	"path"
	"strings"
)

// Touch + Set data to a node
func (zk *ZkConn) TouchAndSet(nodePath, data string) (*gozk.Stat, error) {
	zk.Touch(nodePath)
	return zk.Set(nodePath, data, -1)
}

// Create a node if it doesn't exist, otherwise do nothing
func (zk *ZkConn) Touch(nodePath string) (string, error) {
	if stat, err := zk.Exists(nodePath); err == nil && stat != nil {
		return nodePath, nil
	}
	// touch all directores above nodePath because zookeeper sucks
	dir := path.Dir(nodePath)
	if dir != "/" && dir != "." {
		zk.Touch(dir)
	}
	return zk.Create(nodePath, "", 0, gozk.WorldACL(gozk.PERM_ALL))
}

/*
 * recursive wrapper around Delete
 */
func (zk *ZkConn) RecursiveDelete(dirPath string) error {
	if stat, err := zk.Exists(dirPath); err != nil || stat == nil {
		return errors.New(fmt.Sprintf("Node at %s does not exist!", dirPath))
	}
	children, _, err := zk.Children(dirPath)
	if err != nil {
		return errors.New(fmt.Sprintf("Unable to list children. Error: %s.", err.Error()))
	}
	for _, child := range children {
		if err := zk.RecursiveDelete(dirPath + "/" + child); err != nil {
			return err
		}
	}
	return zk.Delete(dirPath, -1)
}

/*
 * Filters out hidden nodes
 */
func FilterHidden(nodes []string) []string {
	if nodes == nil || len(nodes) == 0 {
		return nodes
	}
	out := []string{}
	for _, item := range nodes {
		if !strings.HasPrefix(item, ".") {
			out = append(out, item)
		}
	}
	return out
}

/*
 * Lists only those children that are not hidden
 */
func (zk *ZkConn) VisibleChildren(dir string) ([]string, *gozk.Stat, error) {
	list, stat, err := zk.Children(dir)
	if err != nil {
		return list, stat, err
	}
	return FilterHidden(list), stat, nil
}
