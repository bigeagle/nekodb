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
	"encoding/json"
	"time"
	// "encoding/binary"
	"github.com/bigeagle/nekodb/nekolib"
	zmq "github.com/pebbe/zmq4"
)

const workerAddr = "inproc://workers"

var ReqHandlerMap = map[uint8](func(*nekoWorker, []byte) ([]byte, error)){
	nekolib.OP_NEW_SERIES:    ReqNewSeries,
	nekolib.OP_IMPORT_SERIES: ReqImportSeries,
	nekolib.OP_FIND_RANGE:    ReqFindByRange,
	nekolib.OP_LIST_SERIES:   ReqListSeries,
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
			msg, err := handler(w, packBytes)
			if err != nil {
				w.sock.SendBytes(
					nekolib.MakeResponse(nekolib.REP_ERR, err.Error()), 0)
			} else {
				w.sock.SendBytes(nekolib.MakeResponse(nekolib.REP_OK, msg), 0)
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

func ReqNewSeries(w *nekoWorker, packBytes []byte) ([]byte, error) {
	series := new(nekolib.NekoSeriesInfo)
	series.FromBytes(bytes.NewBuffer(packBytes[1:]))
	logger.Debug("%v", packBytes[1:])
	logger.Debug("worker %d: %v", w.id, series)
	err := newSeries(series)
	if err != nil {
		return []byte{}, err
	} else {
		return []byte("success"), err
	}
}

func ReqImportSeries(w *nekoWorker, packBytes []byte) ([]byte, error) {
	reqHdr := new(nekolib.ReqImportSeriesHdr)
	reqHdr.FromBytes(bytes.NewBuffer(packBytes[1:]))
	logger.Debug("worker %d: %v", w.id, *reqHdr)
	err := importSeries(reqHdr.SeriesName, w.sock)
	if err != nil {
		return []byte{}, err
	} else {
		return []byte("success"), err
	}
}

func ReqFindByRange(w *nekoWorker, packBytes []byte) ([]byte, error) {
	reqHdr := new(nekolib.ReqFindByRangeHdr)
	reqHdr.FromBytes(bytes.NewBuffer(packBytes[1:]))

	bench_start := time.Now()
	bench_peers := map[string](map[string]int){}
	bench := map[string]interface{}{
		"total_time":  0,
		"bench_peers": bench_peers,
	}

	msgChan := make(chan map[string]interface{}, 256)
	recordChan := make(chan nekolib.SCNode, 1024)
	done := make(chan struct{})
	go func() {
		w.sock.SendBytes(
			nekolib.MakeResponse(nekolib.REP_ACK, "Starting Query"),
			zmq.SNDMORE,
		)
		for record := range recordChan {
			w.sock.SendBytes(record.(*nekolib.NekodRecord).ToBytes(),
				zmq.SNDMORE)
		}
		w.sock.SendBytes([]byte{0, 0}, zmq.SNDMORE)
		bench["total_time"] = time.Since(bench_start).Nanoseconds()
		//logger.Debug("Sending Stream End")
		close(msgChan)
	}()

	getRangeToChan(reqHdr, recordChan, msgChan)

	go func() {
		for r := range msgChan {
			peer := r["peer"].(string)
			if _, found := bench_peers[peer]; found {
				bench_peers[peer]["count"] += int(r["count"].(float64))
				bench_peers[peer]["duration"] += int(r["duration"].(float64))
				bench_peers[peer]["full_duration"] += int(r["full_duration"].(float64))
			} else {
				bench_peers[peer] = map[string]int{
					"count":         int(r["count"].(float64)),
					"duration":      int(r["duration"].(float64)),
					"full_duration": int(r["full_duration"].(int64)),
				}
			}
		}
		close(done)
	}()

	<-done
	return json.Marshal(bench)
}

func ReqListSeries(w *nekoWorker, packBytes []byte) ([]byte, error) {

	s := getServer()
	list := []*nekolib.NekoSeriesMeta{}
	for _, sinfo := range s.collection.coll {
		smeta, _ := getSeriesMeta(sinfo.Name)
		list = append(list, smeta)
	}

	j, _ := json.Marshal(list)
	return j, nil
}
