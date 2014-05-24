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
    zmq "github.com/pebbe/zmq4"
)

type ReqPool struct {
    Size int
    Pool chan *zmq.Socket
}

func NewRequestPool(target string, size int) *ReqPool {
    pool := new(ReqPool)
    pool.Size = size
    pool.Pool = make(chan *zmq.Socket, size)

    for i := 0; i < size; i++ {
        sock, _ := zmq.NewSocket(zmq.REQ)
        sock.Connect(target)
        pool.Pool <- sock
    }

    return pool
}

func (p *ReqPool) Get() *zmq.Socket {
    return <- p.Pool
}

func (p *ReqPool) Return(s *zmq.Socket) {
    p.Pool <- s
}

func (p *ReqPool) Close() {
    close(p.Pool)
}
