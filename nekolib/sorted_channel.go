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

package nekolib

import (
	"sort"
	"sync"
)

type SCNode interface {
	Key() int64
}

type sBuffer []SCNode

func (s sBuffer) Len() int           { return len(s) }
func (s sBuffer) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s sBuffer) Less(i, j int) bool { return s[i].Key() < s[j].Key() }

type SortedChannel struct {
	mutex      sync.Mutex
	flushSize  int
	npubs      int32
	buffers    map[string]([]SCNode)
	publishers map[string]bool
	out        chan SCNode
}

func NewSortedChannel(flushSize int, out chan SCNode) *SortedChannel {
	b := new(SortedChannel)
	b.flushSize = flushSize
	b.npubs = 0
	b.buffers = make(map[string]([]SCNode))
	b.publishers = make(map[string]bool)
	b.out = out
	return b
}

func (b *SortedChannel) AddPublisher(name string) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if _, found := b.publishers[name]; !found {
		b.npubs++
		b.publishers[name] = true
		b.buffers[name] = make([]SCNode, 0, b.flushSize*2)
	}
}

func (b *SortedChannel) RemovePublisher(name string) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.flush(name)
	if present, ok := b.publishers[name]; present && ok {
		// delete(b.buffers, name)
		b.publishers[name] = false
		b.npubs--
		if b.npubs <= 0 {
			b.flushAll()
		}
	}
}

func (b *SortedChannel) Pub(name string, n SCNode) {
	b.mutex.Lock()
	b.buffers[name] = append(b.buffers[name], n)

	if len(b.buffers[name]) >= b.flushSize {
		b.flush(name)
	}
	b.mutex.Unlock()
}

func (b *SortedChannel) flush(name string) {
	// fmt.Println(name, b.publishers)

	mname := name
	min := int64(1<<63 - 1)

	for cn, buf := range b.buffers {
		if len(buf) == 0 {
			if b.publishers[cn] {
				return
			} else {
				delete(b.buffers, cn)
				continue
			}
		}

		m := buf[len(buf)-1]
		if m.Key() < min {
			mname = cn
			min = m.Key()
		}
	}

	cursors := make(map[string]int)
	cursors[mname] = len(b.buffers[mname])
	c := len(b.buffers[mname])
	for cn, buf := range b.buffers {
		if cn != mname {
			for i := len(buf) - 1; i >= 0; i-- {
				if buf[i].Key() < min {
					c += i + 1
					cursors[cn] = i + 1
					break
				}
			}
		}
	}

	// fmt.Println(cursors)
	// fmt.Println(b.buffers)

	sbuf := make([]SCNode, 0, c)
	for cn, cur := range cursors {
		sbuf = append(sbuf, b.buffers[cn][:cur]...)
		b.buffers[cn] = b.buffers[cn][cur:]
	}

	sort.Sort(sBuffer(sbuf))
	for _, node := range sbuf {
		b.out <- node
	}
}

func (b *SortedChannel) flushAll() {
	sbuf := make([]SCNode, 0)
	for _, buf := range b.buffers {
		if len(buf) > 0 {
			sbuf = append(sbuf, buf...)
		}
	}

	sort.Sort(sBuffer(sbuf))
	for _, node := range sbuf {
		b.out <- node
	}
	close(b.out)
}
