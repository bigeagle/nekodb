/*
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) Justin Wong, 2014
 */

package main

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/bigeagle/nekodb/nekolib"
)

type nekoRingNode struct {
	nekodPeer
	Key  uint32
	Next *nekoRingNode
	Prev *nekoRingNode
}

type nekoBackendRing struct {
	Head                        *nekoRingNode
	peer_count, real_peer_count int32
	real_peers                  map[string]bool
	m                           sync.RWMutex
}

func newNekoBackendRing() *nekoBackendRing {
	ring := new(nekoBackendRing)
	ring.Head = nil
	ring.peer_count = 0
	ring.real_peer_count = 0
	ring.real_peers = make(map[string]bool)
	return ring
}

// Insert a new nekodPeerInfo, if existed, replace it with a new one
func (r *nekoBackendRing) Insert(p *nekolib.NekodPeerInfo) {
	n := new(nekoRingNode)
	np := newNekodPeerFromInfo(p)
	np.Init()
	n.nekodPeer = *np
	n.Key = nekolib.Hash32([]byte(n.Name))

	r.m.Lock()
	defer r.m.Unlock()
	if r.Head == nil {
		n.Prev = n
		n.Next = n
		r.Head = n
		atomic.AddInt32(&r.peer_count, 1)
		atomic.AddInt32(&r.real_peer_count, 1)
		r.real_peers[n.RealName] = true
	} else if r.Head.Key >= n.Key {
		if r.Head.Name == n.Name {
			r.Head.nekodPeer.Close()
			r.Head.nekodPeer = *np
		} else {
			prev, next := r.Head.Prev, r.Head
			prev.Next = n
			n.Next = next
			next.Prev = n
			n.Prev = prev
			r.Head = n
			atomic.AddInt32(&r.peer_count, 1)
			if _, found := r.real_peers[n.RealName]; !found {
				r.real_peers[n.RealName] = true
				atomic.AddInt32(&r.real_peer_count, 1)
			}
		}
	} else {
		var prev *nekoRingNode
		for prev = r.Head; (prev.Next != r.Head) && (prev.Next.Key < n.Key); prev = prev.Next {
		}
		next := prev.Next
		for cur := next; cur.Key == n.Key; cur = cur.Next {
			if cur.Name == n.Name {
				cur.nekodPeer.Close()
				cur.nekodPeer = *np
				return
			}
		}
		prev.Next = n
		n.Next = next
		next.Prev = n
		n.Prev = prev
		atomic.AddInt32(&r.peer_count, 1)
		if _, found := r.real_peers[n.RealName]; !found {
			r.real_peers[n.RealName] = true
			atomic.AddInt32(&r.real_peer_count, 1)
		}
	}

}

func (r *nekoBackendRing) UpdateInfo(name string, p *nekolib.NekodPeerInfo) {
	if node, ok := r.Get(name); ok {
		// logger.Debug("%v", node)
		node.nekodPeer.CopyInfo(p)
	} else {
		logger.Debug("Not Found: %s", name)
		r.Insert(p)
	}
}

func (r *nekoBackendRing) ResetPeer(name string, p *nekolib.NekodPeerInfo) {
	if node, ok := r.Get(name); ok {
		node.nekodPeer.CopyInfo(p)
		node.nekodPeer.Reset()
	} else {
		r.Insert(p)
	}

}

func (r *nekoBackendRing) Remove(name string) {
	r.m.Lock()
	defer r.m.Unlock()

	for cur := r.Head; cur != nil; cur = cur.Next {
		if cur.Name == name {
			if r.Head == cur {
				if cur.Next == cur {
					r.Head = nil
				} else {
					r.Head = cur.Next
				}
			}
			cur.Prev.Next = cur.Next
			cur.Next.Prev = cur.Prev
			cur.Next = nil
			cur.Prev = nil
			delete(r.real_peers, cur.Name)
			atomic.AddInt32(&r.peer_count, -1)
			atomic.AddInt32(&r.real_peer_count, -1)
			break
		}
		if cur.Next == r.Head {
			break
		}
	}

}

func (r *nekoBackendRing) ForEach(op func(n *nekoRingNode)) {
	for cur := r.Head; cur != nil; cur = cur.Next {
		op(cur)
		if cur.Next == r.Head {
			break
		}
	}
}

func (r *nekoBackendRing) ForEachSafe(op func(n *nekoRingNode)) {
	r.m.Lock()
	defer r.m.Unlock()
	r.ForEach(op)
}

func (r *nekoBackendRing) Get(name string) (*nekoRingNode, bool) {
	for cur := r.Head; cur != nil; cur = cur.Next {
		if cur.Name == name {
			return cur, true
		}

		if cur.Next == r.Head {
			break
		}
	}
	return nil, false
}

func (r *nekoBackendRing) GetByKey(key uint32) (*nekoRingNode, error) {
	if (key < r.Head.Key) || (key > r.Head.Prev.Key) {
		return r.Head, nil
	} else {
		for cur := r.Head; cur != nil; cur = cur.Next {
			if (cur.Key < key) && (cur.Next.Key >= key) {
				return cur.Next, nil
			}
			if cur.Next == r.Head {
				break
			}
		}
	}
	return nil, errors.New("Not Found")
}

func (r *nekoBackendRing) String() string {
	nodes := make([]string, 0)
	for cur := r.Head; cur != nil; {
		nodes = append(nodes, fmt.Sprintf("{%d: %s}", cur.Key, cur.Name))
		if cur.Next == r.Head {
			break
		}
		cur = cur.Next
	}
	return "[" + strings.Join(nodes, "->") + "]"
}
