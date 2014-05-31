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
	nekolib.OP_NEW_SERIES:   ReqNewSeries,
	nekolib.OP_PING:         ReqPing,
	nekolib.OP_INSERT_BATCH: ReqInsertBatch,
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

func ReqInsertBatch(w *nekodWorker, packBytes []byte) error {
	var reqHdr nekolib.ReqInsertBlockHdr

	buf := bytes.NewBuffer(packBytes[1:])
	err := (&reqHdr).FromBytes(buf)
	if err != nil {
		w.sock.SendBytes(
			nekolib.MakeResponse(nekolib.REP_ERR, err.Error()), 0)
		logger.Error(err.Error())
		return err
	}

	series, _ := w.srv.GetSeries(reqHdr.SeriesName)
	series.ReverseHash(reqHdr.HashValue, reqHdr.StartTs, reqHdr.EndTs)

	for more, _ := w.sock.GetRcvmore(); more; more, _ = w.sock.GetRcvmore() {
		msg, err := w.sock.RecvBytes(0)
		if err != nil {
			w.sock.SendBytes(
				nekolib.MakeResponse(
					nekolib.REP_ERR, "Error receiving message"), 0)
			logger.Error(err.Error())
			return err
		}
		records := make([]*nekolib.NekodRecord, 0)

		for buf := bytes.NewBuffer(msg); buf.Len() > 0; {
			r := new(nekolib.NekodRecord)
			r.FromBytes(buf)
			records = append(records, r)
		}

		err = series.InsertBatch(records, reqHdr.Priority)
		if err != nil {
			w.sock.SendBytes(
				nekolib.MakeResponse(nekolib.REP_ERR, err.Error()), 0)
			logger.Error(err.Error())
			return err
		}
	}
	w.sock.SendBytes(
		nekolib.MakeResponse(nekolib.REP_OK, "Success"), 0)
	return nil
}

func ReqPing(w *nekodWorker, packBytes []byte) error {
	buf := []byte{byte(nekolib.OP_PONG)}
	w.sock.SendBytes(buf, 0)
	return nil
}
