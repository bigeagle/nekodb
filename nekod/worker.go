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
	"bytes"

	"github.com/bigeagle/nekodb/nekolib"
	zmq "github.com/pebbe/zmq4"
)

const workerAddr = "inproc://workers"

type nekodWorker struct {
	id   int
	srv  *nekoBackendServer
	sock *zmq.Socket
}

var ReqHandlerMap = map[uint8](func(*nekodWorker, []byte) error){
	nekolib.OP_NEW_SERIES: ReqNewSeries,
	nekolib.OP_PING:       ReqPing,
}

func (w *nekodWorker) serveForever() {
	for {
		packBytes, _ := w.sock.RecvBytes(0)
		// logger.Debug("%v", packBytes)
		opcode := packBytes[0]
		if handler, ok := ReqHandlerMap[uint8(opcode)]; ok {
			handler(w, packBytes)
		}
	}
}

func startWorker(id int, s *nekoBackendServer) {
	w := new(nekodWorker)
	w.id = id
	w.srv = s
	w.sock, _ = zmq.NewSocket(zmq.REP)
	w.sock.Connect(workerAddr)
	w.serveForever()
}

func (w *nekodWorker) processRequest(packBytes []byte) {
	logger.Debug("worker %d: %v", w.id, packBytes)
	w.sock.Send("reply", 0)
}

func ReqNewSeries(w *nekodWorker, packBytes []byte) error {
	sInfo := new(nekolib.NekoSeriesInfo)
	sInfo.FromBytes(bytes.NewBuffer(packBytes[1:]))
	// logger.Debug("%v", packBytes[1:])
	logger.Debug("worker %d: %v", w.id, sInfo)

	err := w.srv.NewSeries(sInfo.Name, sInfo.Id, sInfo.FragLevel)
	if err != nil {
		w.sock.Send("ERROR", 0)
		return err
	}
	w.sock.Send("OK", 0)
	return nil
}

func ReqPing(w *nekodWorker, packBytes []byte) error {
	buf := []byte{byte(nekolib.OP_PONG)}
	w.sock.SendBytes(buf, 0)
	return nil
}
