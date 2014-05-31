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
	"sync"
	"time"

	"github.com/bigeagle/nekodb/nekolib"
)

type bufferNode struct {
	records []*nekolib.NekodRecord
	ts      time.Time
	next    *bufferNode
	prev    *bufferNode
}

type bufferList struct {
	head  *bufferNode
	mutex sync.Mutex
	count int
}

func newBufferList() *bufferList {
	l := new(bufferList)
	l.head = new(bufferNode)
	l.head.next = l.head
	l.head.prev = l.head
	l.count = 0
	return l
}

func (l *bufferList) push(records []*nekolib.NekodRecord) {
	// logger.Debug("Push %d records", len(records))
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if len(records) < 1 {
		return
	}

	ts, _ := nekolib.Bytes2Time(records[0].Ts)
	node := &bufferNode{records, ts, nil, nil}

	uninserted := true

	for cur := l.head.prev; cur != l.head; cur = cur.prev {
		if cur.ts.Before(ts) {
			uninserted = false
			node.next = cur.next
			node.prev = cur
			cur.next = node
			node.next.prev = node
			break
		}
	}

	if uninserted {
		node.next = l.head.next
		node.prev = l.head
		l.head.next = node
		node.next.prev = node
	}
	l.count++
}

func (l *bufferList) pop() []*nekolib.NekodRecord {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.count == 0 {
		return nil
	}

	node := l.head.next
	l.head.next = node.next
	node.next.prev = l.head
	l.count--
	return node.records
}
