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
	// "encoding/binary"
	"github.com/bigeagle/nekodb/nekolib"
	zmq "github.com/pebbe/zmq4"
)

const workerAddr = "inproc://workers"

var ReqHandlerMap = map[uint8](func(*nekoWorker, []byte) error){
	nekolib.OP_NEW_SERIES:    ReqNewSeries,
	nekolib.OP_IMPORT_SERIES: ReqImportSeries,
	nekolib.OP_FIND_RANGE:    ReqFindByRange,
}

type nekoWorker struct {
	id   int
	srv  *nekoServer
	sock *zmq.Socket
}

func (w *nekoWorker) serveForever() {
	for {
		packBytes, _ := w.sock.RecvBytes(0)
		if len(packBytes) < 1 {
			continue
		}
		opcode := packBytes[0]
		if handler, ok := ReqHandlerMap[uint8(opcode)]; ok {
			err := handler(w, packBytes)
			if err != nil {
				w.sock.SendBytes(
					nekolib.MakeResponse(nekolib.REP_ERR, err.Error()), 0)
			} else {
				w.sock.SendBytes(nekolib.MakeResponse(nekolib.REP_OK, "Success"), 0)
			}
		} else {
			logger.Debug("%v", packBytes)
		}
	}
}

func startWorker(id int, s *nekoServer) {
	w := new(nekoWorker)
	w.id = id
	w.srv = s
	w.sock, _ = zmq.NewSocket(zmq.REP)
	w.sock.Connect(workerAddr)
	w.serveForever()
}

func ReqNewSeries(w *nekoWorker, packBytes []byte) error {
	series := new(nekolib.NekoSeriesInfo)
	series.FromBytes(bytes.NewBuffer(packBytes[1:]))
	logger.Debug("%v", packBytes[1:])
	logger.Debug("worker %d: %v", w.id, series)
	return newSeries(series)
}

func ReqImportSeries(w *nekoWorker, packBytes []byte) error {
	reqHdr := new(nekolib.ReqImportSeriesHdr)
	reqHdr.FromBytes(bytes.NewBuffer(packBytes[1:]))
	logger.Debug("worker %d: %v", w.id, *reqHdr)
	return importSeries(reqHdr.SeriesName, w.sock)
}

func ReqFindByRange(w *nekoWorker, packBytes []byte) error {
	reqHdr := new(nekolib.ReqFindByRangeHdr)
	reqHdr.FromBytes(bytes.NewBuffer(packBytes[1:]))
	logger.Debug("Find By Range Request: %#v", *reqHdr)
	return findByRange(reqHdr, w.sock)
}
