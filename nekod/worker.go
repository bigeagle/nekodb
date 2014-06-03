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
	"fmt"

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
	nekolib.OP_FIND_RANGE:   ReqGetRange,
	nekolib.OP_SERIES_INFO:  ReqSeriesMeta,
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

func ReqSeriesMeta(w *nekodWorker, packBytes []byte) error {
	reqHdr := new(nekolib.ReqSeriesMetaHdr)
	reqHdr.FromBytes(bytes.NewBuffer(packBytes[1:]))

	series, found := w.srv.GetSeries(reqHdr.SeriesName)
	if !found {
		err := fmt.Errorf("No Series %s", reqHdr.SeriesName)
		w.sock.SendBytes(
			nekolib.MakeResponse(nekolib.REP_ERR, err.Error()), 0)
		return err
	}

	count, _ := series.Count()
	sm := nekolib.NekodSeriesInfo{
		Count: int64(count),
	}
	buf := bytes.NewBuffer(make([]byte, 0, 9))
	buf.WriteByte(byte(nekolib.REP_OK))
	buf.Write(sm.ToBytes())
	w.sock.SendBytes(buf.Bytes(), 0)

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

func ReqGetRange(w *nekodWorker, packBytes []byte) error {
	var reqHdr nekolib.ReqFindByRangeHdr

	buf := bytes.NewBuffer(packBytes[1:])
	err := (&reqHdr).FromBytes(buf)
	if err != nil {
		w.sock.SendBytes(
			nekolib.MakeResponse(nekolib.REP_ERR, err.Error()), 0)
		logger.Error(err.Error())
		return err
	}

	series, _ := w.srv.GetSeries(reqHdr.SeriesName)
	start, _ := nekolib.Bytes2Time(reqHdr.StartTs)
	end, _ := nekolib.Bytes2Time(reqHdr.EndTs)
	logger.Debug("Start Querying Series: %s from %v to %v", series.Name, start, end)

	w.sock.SendBytes(nekolib.MakeResponse(nekolib.REP_ACK, "starting"), zmq.SNDMORE)

	blk_lower := int64(1<<63 - 1)
	blk_upper := int64(-1 << 63)
	buf = bytes.NewBuffer(make([]byte, 0, 256))

	series.RangeOp(reqHdr.StartTs, reqHdr.EndTs, reqHdr.Priority, func(key, value []byte) {
		r := &nekolib.NekodRecord{key, value}
		ts := nekolib.Bytes2TimeSec(key)

		if !(ts < blk_upper && ts >= blk_lower) {
			if buf.Len() > 0 {
				w.sock.SendBytes(buf.Bytes(), zmq.SNDMORE)
			}
			buf = bytes.NewBuffer(make([]byte, 0, 256))
			blk_lower, blk_upper = nekolib.TsBoundary(ts, series.FragLevel)
		}
		buf.Write(r.ToBytes())
	})

	if buf.Len() > 0 {
		w.sock.SendBytes(buf.Bytes(), zmq.SNDMORE)
	}

	w.sock.SendBytes([]byte{0, 0}, zmq.SNDMORE)
	logger.Debug("Done")
	w.sock.SendBytes(
		nekolib.MakeResponse(nekolib.REP_OK, "Done"), 0)

	return nil
}

func ReqPing(w *nekodWorker, packBytes []byte) error {
	buf := []byte{byte(nekolib.OP_PONG)}
	w.sock.SendBytes(buf, 0)
	return nil
}
