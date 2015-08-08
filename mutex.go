package recipes

import (
	"errors"
	gozk "launchpad.net/gozk"
	"path"
	"sort"
	"strings"
)

/** From http://zookeeper.apache.org/doc/r3.1.2/recipes.html#sc_recipes_Locks
 * Clients wishing to obtain a lock do the following:
 * 1. Call create() with a pathname of "_locknode_/lock-" and the sequence and ephemeral flags set.
 * 2. Call getChildren() on the lock node without setting the watch flag (this is important to avoid the herd
 *    effect).
 * 3. If the pathname created in step 1 has the lowest sequence number suffix, the client has the lock and the
 *    client exits the protocol.
 * 4. The client calls exists() with the watch flag set on the path in the lock directory with the next lowest
 *    sequence number.
 * 5. if exists() returns false, go to step 2. Otherwise, wait for a notification for the pathname from the
 *    previous step before going to step 2.
 *
 * The unlock protocol is very simple:
 *  clients wishing to release a lock simply delete the node they created in step 1
 *
 * Note that this is not goroutine-safe and will not work for directories with other children!
 */
type Mutex struct {
	conn *gozk.Conn
	lock string
	Path string
}

func NewMutex(c *gozk.Conn, p string) *Mutex {
	return &Mutex{conn: c, Path: p}
}

func (m *Mutex) Lock() (err error) {
	m.lock, err = rlock(m.conn, m.Path, "lock-", []string{"lock-"})
	return err
}

func (m *Mutex) Unlock() error {
	if m.lock == "" {
		return nil // lockPath isn't set, we haven't locked anything
	}
	err := m.conn.Delete(m.lock, -1)
	if err != nil {
		return err
	}
	m.lock = ""
	return nil
}

/** From http://zookeeper.apache.org/doc/r3.1.2/recipes.html#Shared+Locks
 * Obtaining a read lock:
 * 1. Call create() to create a node with pathname "_locknode_/read-". This is the lock node use later in the
 *    protocol. Make sure to set both the sequence and ephemeral flags.
 * 2. Call getChildren() on the lock node without setting the watch flag - this is important, as it avoids the
 *    herd effect.
 * 3. If there are no children with a pathname starting with "write-" and having a lower sequence number than
 *    the node created in step 1, the client has the lock and can exit the protocol.
 * 4. Otherwise, call exists(), with watch flag, set on the node in lock directory with pathname staring with
 *    "write-" having the next lowest sequence number.
 * 5. If exists() returns false, goto step 2. Otherwise, wait for a notification for the pathname from the
 *    previous step before going to step 2
 *
 * Obtaining a write lock:
 * 1. Call create() to create a node with pathname "_locknode_/write-". This is the lock node spoken of later
 *    in the protocol. Make sure to set both sequence and ephemeral flags.
 * 2. Call getChildren() on the lock node without setting the watch flag - this is important, as it avoids the
 *    herd effect.
 * 3. If there are no children with a lower sequence number than the node created in step 1, the client has
 *    the lock and the client exits the protocol.
 * 4. Call exists(), with watch flag set, on the node with the pathname that has the next lowest sequence
 *    number.
 * 5. If exists() returns false, goto step 2. Otherwise, wait for a notification for the pathname from the
 *    previous step before going to step 2.
 *
 * Note that this is not goroutine-safe and will not work for directories with other children!
 */
type RWMutex struct {
	conn *gozk.Conn
	lock string
	Path string
}

func NewRWMutex(c *gozk.Conn, p string) *RWMutex {
	return &RWMutex{conn: c, Path: p}
}

func (m *RWMutex) RLock() (err error) {
	m.lock, err = rlock(m.conn, m.Path, "read-", []string{"write-"})
	return err
}

func (m *RWMutex) RUnlock() error {
	if m.lock == "" {
		return nil // lockPath isn't set, we haven't locked anything
	}
	err := m.conn.Delete(m.lock, -1)
	if err != nil {
		return err
	}
	m.lock = ""
	return nil
}

func (m *RWMutex) Lock() (err error) {
	m.lock, err = rlock(m.conn, m.Path, "write-", []string{"read-", "write-"})
	return err
}

func (m *RWMutex) Unlock() error {
	if m.lock == "" {
		return nil // lockPath isn't set, we haven't locked anything
	}
	err := m.conn.Delete(m.lock, -1)
	if err != nil {
		return err
	}
	m.lock = ""
	return nil
}

func rlock(conn *gozk.Conn, basePath, prefix string, checkPrefix []string) (lock string, err error) {
	// step 1
	lock, err = conn.Create(path.Join(basePath, prefix), "", gozk.EPHEMERAL|gozk.SEQUENCE,
		gozk.WorldACL(gozk.PERM_ALL))
	if err != nil {
		return lock, err
	}
	for {
		// step 2
		children, _, err := conn.Children(basePath)
		if err != nil {
			return lock, err
		}
		// step 3
		if children == nil || len(children) == 0 {
			return lock, errors.New("get children didn't return my lock")
		}
		// filter out non-lock children and extract sequence numbers
		filteredChildren := map[string]string{}
		filteredChildrenKeys := []string{}
		for _, v := range children {
			for _, pfx := range checkPrefix {
				if strings.HasPrefix(v, pfx) {
					seqNum := strings.Replace(v, pfx, "", 1)
					filteredChildren[seqNum] = v
					filteredChildrenKeys = append(filteredChildrenKeys, seqNum)
					break
				}
			}
		}
		sort.Strings(filteredChildrenKeys)
		prevLock := ""
		for _, seqNum := range filteredChildrenKeys {
			if path.Base(lock) == filteredChildren[seqNum] {
				break
			}
			prevLock = path.Join(basePath, filteredChildren[seqNum])
		}
		if prevLock == "" {
			return lock, nil
		}
		// step 4
		stat, watchCh, err := conn.ExistsW(prevLock)
		if err != nil {
			return lock, err
		}
		// step 5
		if stat == nil {
			continue
		}
		<-watchCh
	}
	return lock, nil
}
