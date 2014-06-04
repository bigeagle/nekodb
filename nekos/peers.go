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
	//    "strings"
	"sync"

	"github.com/bigeagle/nekodb/nekolib"
	zmq "github.com/pebbe/zmq4"
)

type nekodPeer struct {
	m        sync.RWMutex
	Name     string `json:"name"`
	RealName string `json:"real_name"`
	Hostname string `json:"hostname"`
	Port     int    `json:"port"`
	State    int    `json:"state"`
	ReqPool  *nekolib.ReqPool
}

func newNekodPeer(name, realName, hostname string, port, state int) *nekodPeer {
	p := new(nekodPeer)
	p.Name = name
	p.RealName = realName
	p.Hostname = hostname
	p.Port = port
	p.State = state
	return p
}

func newNekodPeerFromInfo(p *nekolib.NekodPeerInfo) *nekodPeer {
	return newNekodPeer(p.Name, p.RealName, p.Hostname, p.Port, p.State)
}

func (p *nekodPeer) CopyInfo(i *nekolib.NekodPeerInfo) {
	p.m.Lock()
	defer p.m.Unlock()
	p.Name = i.Name
	p.RealName = i.RealName
	p.Hostname = i.Hostname
	p.Port = i.Port
	p.State = i.State
}

func (p *nekodPeer) Init() {
	p.ReqPool = nekolib.NewRequestPool(
		fmt.Sprintf("tcp://%s:%d", p.Hostname, p.Port), 8)
}

func (p *nekodPeer) Close() {
	if p.ReqPool != nil {
		p.ReqPool.Close()
	}
}

func (p *nekodPeer) Reset() {
	p.m.Lock()
	defer p.m.Unlock()
	p.Close()
	p.Init()
}

func (p *nekodPeer) Request(socketHandler func(s *zmq.Socket) error) error {
	sock := p.ReqPool.Get()
	defer p.ReqPool.Return(sock)
	return socketHandler(sock)
}
