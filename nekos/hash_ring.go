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
    "fmt"
    "strings"
    "sync"
    "github.com/bigeagle/nekodb/nekolib"
)

type nekodPeer struct {
    Name string
    RealName string
    Hostname string
    Port int
}

func newNekodPeer(name, realName, hostname string, port int) *nekodPeer {
    p := new(nekodPeer)
    p.Name = name
    p.RealName = realName
    p.Hostname = hostname
    p.Port = port
    return p
}

type nekoRingNode struct {
    nekodPeer
    Key uint32
    Next *nekoRingNode
    Prev *nekoRingNode
}

type nekoBackendRing struct {
    Head *nekoRingNode
    m sync.RWMutex
}

func newNekoBackendRing() *nekoBackendRing {
    ring := new(nekoBackendRing)
    ring.Head = nil
    return ring
}


func (r *nekoBackendRing) Insert(p *nekolib.NekodPeerInfo) {
    n := new(nekoRingNode)
    n.Name = p.Name
    n.RealName = p.RealName
    n.Hostname = p.Hostname
    n.Port = p.Port
    n.Key = nekolib.Hash32([]byte(n.Name))

    r.m.Lock()
    defer r.m.Unlock()
    if r.Head == nil {
        n.Prev = n
        n.Next = n
        r.Head = n
    } else if r.Head.Key >= n.Key {
        if r.Head.Name == n.Name {
            r.Head.RealName = n.RealName
            r.Head.Hostname = n.Hostname
            r.Head.Port = n.Port
        } else {
            prev, next := r.Head.Prev, r.Head
            prev.Next = n
            n.Next = next
            next.Prev = n
            n.Prev = prev
            r.Head = n
        }
    } else {
        var prev *nekoRingNode
        for prev = r.Head; (prev.Next != r.Head) && (prev.Next.Key < n.Key) ; prev = prev.Next {}
        next := prev.Next
        for cur := next; cur.Key == n.Key; cur = cur.Next {
            if cur.Name == n.Name {
                cur.RealName = n.RealName
                cur.Hostname = n.Hostname
                cur.Port = n.Port
                return
            }
        }
        prev.Next = n
        n.Next = next
        next.Prev = n
        n.Prev = prev
    }

}

func (r *nekoBackendRing) Remove(name string) {
    r.m.Lock()
    defer r.m.Unlock()

    if r.Head == nil {
        return
    } else {
        cur := r.Head
        for {
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
                break
            }
            if cur.Next == r.Head {
                break
            }
            cur = cur.Next
        }
    }

}

func (r *nekoBackendRing) String() string {
    nodes := make([]string, 0)
    for cur := r.Head ; cur != nil ; {
        nodes = append(nodes, fmt.Sprintf("{%d: %s}", cur.Key, cur.Name))
        if cur.Next == r.Head {
            break
        }
        cur = cur.Next
    }
    return "[" + strings.Join(nodes, "->") + "]"
}
