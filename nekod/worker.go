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
    zmq "github.com/pebbe/zmq4"
)

const workerAddr = "inproc://workers"

type nekodWorker struct {
    id int
    srv *nekoBackendServer
    sock *zmq.Socket
}

func (w *nekodWorker) serveForever() {
    for {
        packBytes, _ := w.sock.RecvBytes(0)
        w.processRequest(packBytes)
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
