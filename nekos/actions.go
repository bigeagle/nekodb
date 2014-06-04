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
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/bigeagle/nekodb/nekolib"
	zmq "github.com/pebbe/zmq4"
)

func ensureSeries(sname string) {
	s := getServer()
	c := s.collection

	if _, ok := c.getSeries(sname); !ok {
		newSeries(&nekolib.NekoSeriesInfo{sname, sname, nekolib.SLICE_FRAG_LEVEL_DEFAULT})
	}
}

// Add Series
func newSeries(series *nekolib.NekoSeriesInfo) error {
	s := getServer()

	sjson, _ := json.Marshal(series)
	key := fmt.Sprintf("%s/%s", nekolib.ETCD_SERIES_DIR, series.Name)
	s.ec.Set(key, string(sjson), 0)

	buf := bytes.NewBuffer(make([]byte, 0, 16))
	buf.WriteByte(byte(nekolib.OP_NEW_SERIES))
	buf.Write(series.ToBytes())
	msg := buf.Bytes()

	visited := make(map[string]bool)

	// npeers := s.backends.real_peer_count
	// logger.Debug("%d", npeers)
	var wg sync.WaitGroup

	s.backends.ForEachSafe(func(n *nekoRingNode) {
		if _, found := visited[n.RealName]; !found {
			visited[n.RealName] = true
			wg.Add(1)
			go func() {
				defer wg.Done()
				// logger.Debug(n.RealName)
				n.Request(func(s *zmq.Socket) error {
					if _, err := s.SendBytes(msg, 0); err != nil {
						logger.Error(err.Error())
						return err
					}
					reply, err := s.Recv(0)
					if err != nil {
						logger.Error(err.Error())
						return err
					}
					logger.Debug("Peer %s: %v\n", n.RealName, reply)
					return nil
				})
			}()
		}
	})

	wg.Wait()
	return nil
}

func importSeries(sname string, sock *zmq.Socket) error {
	s := getServer()

	sinfo, found := s.collection.getSeries(sname)
	if !found {
		return errors.New("Series Not Found")
	}
	var wg sync.WaitGroup
	// flush block to coresponding peer
	flushBlock := func(block []*nekolib.NekodRecord, lower, upper int64) {
		defer wg.Done()
		if len(block) < 1 {
			return
		}

		hkey := nekolib.TimeSec2Bytes(lower)
		hs := nekolib.Hash32(hkey)
		peer, _ := s.backends.GetByKey(hs)
		start_ts, end_ts := nekolib.TimeBoundary(block[0].Ts, sinfo.FragLevel)

		reqHdr := &nekolib.ReqInsertBlockHdr{
			SeriesName: sname,
			HashValue:  hs,
			StartTs:    start_ts,
			EndTs:      end_ts,
			Priority:   uint8(0),
			Count:      uint16(len(block)),
		}

		peer.Request(func(psock *zmq.Socket) error {
			buf := bytes.NewBuffer(make([]byte, 0))
			buf.WriteByte(byte(nekolib.OP_INSERT_BATCH))
			buf.Write(reqHdr.ToBytes())
			psock.SendBytes(buf.Bytes(), zmq.SNDMORE)

			buf = bytes.NewBuffer(make([]byte, 0, 1024))
			for _, r := range block {
				buf.Write(r.ToBytes())
			}
			psock.SendBytes(buf.Bytes(), 0)
			msg, _ := psock.RecvBytes(0)
			if uint8(msg[0]) != nekolib.REP_OK {
				logger.Error("peer %s: %s", peer.Name, string(msg[1:]))
			}
			// logger.Debug("peer %s: %s", peer.Name, string(msg[1:]))
			return nil
		})

	}

	blk_lower := int64(1<<63 - 1)
	blk_upper := int64(-1 << 63)
	var record_blk []*nekolib.NekodRecord

	for more, _ := sock.GetRcvmore(); more; more, _ = sock.GetRcvmore() {
		msg, err := sock.RecvBytes(0)
		if err != nil {
			logger.Error(err.Error())
			return err
		}

		for buf := bytes.NewBuffer(msg); buf.Len() > 0; {
			r := new(nekolib.NekodRecord)
			if err := r.FromBytes(buf); err != nil {
				if err == nekolib.EndOfStream {
					break
				}
				logger.Error(err.Error())
				return err
			}

			ts := nekolib.Bytes2TimeSec(r.Ts)
			if !(ts < blk_upper && ts >= blk_lower) {
				wg.Add(1)
				go flushBlock(record_blk, blk_lower, blk_upper)
				// Reset Block Cache and Time Range
				record_blk = make([]*nekolib.NekodRecord, 0, 32)
				blk_lower, blk_upper = nekolib.TsBoundary(ts, sinfo.FragLevel)
				record_blk = append(record_blk, r)
			}
			record_blk = append(record_blk, r)
		}
	}
	wg.Add(1)
	flushBlock(record_blk, blk_lower, blk_upper)
	wg.Wait()
	return nil
}

func getRangeToChan(reqHdr *nekolib.ReqFindByRangeHdr, recordChan chan nekolib.SCNode, msgChan chan map[string]interface{}) error {
	s := getServer()
	sortedChannel := nekolib.NewSortedChannel(128, recordChan)

	visited := make(map[string]bool)
	s.backends.ForEachSafe(func(n *nekoRingNode) {
		sortedChannel.AddPublisher(n.RealName)
	})

	buf := bytes.NewBuffer(make([]byte, 0, 16))
	buf.WriteByte(byte(nekolib.OP_FIND_RANGE))
	buf.Write(reqHdr.ToBytes())
	reqMsg := buf.Bytes()

	s.backends.ForEachSafe(func(n *nekoRingNode) {
		if _, found := visited[n.RealName]; !found {
			visited[n.RealName] = true
			go func() {
				// logger.Debug(n.RealName)
				n.Request(func(psock *zmq.Socket) error {
					bench_start := time.Now()
					if _, err := psock.SendBytes(reqMsg, 0); err != nil {
						logger.Error(err.Error())
						return err
					}
					ack, _ := psock.RecvBytes(0)
					if uint8(ack[0]) != nekolib.REP_ACK {
						logger.Error("peer %s: %s", n.Name, string(ack[1:]))
						return errors.New(string(ack[1:]))
					}

				READ_STREAM:
					for more, _ := psock.GetRcvmore(); more; more, _ = psock.GetRcvmore() {
						msg, err := psock.RecvBytes(0)
						// logger.Debug("yes")
						if err != nil {
							logger.Error(err.Error())
							return err
						}

						for buf := bytes.NewBuffer(msg); buf.Len() > 0; {
							r := new(nekolib.NekodRecord)
							if err := r.FromBytes(buf); err != nil {
								if err == nekolib.EndOfStream {
									break READ_STREAM
								}
								logger.Error(err.Error())
								return err
							}
							// logger.Debug("%#v", r)
							sortedChannel.Pub(n.RealName, r)
						}
					}
					msg, _ := psock.RecvBytes(0)
					sortedChannel.RemovePublisher(n.RealName)
					if uint8(msg[0]) != nekolib.REP_OK {
						logger.Error("peer %s", n.Name)
						return errors.New(string(msg[1:]))
					}
					if msgChan != nil {
						var r map[string]interface{}
						if err := json.Unmarshal(msg, &r); err == nil {
							r["full_duration"] = time.Since(bench_start).Nanoseconds()
							msgChan <- r
						}
					}
					// logger.Debug("peer %s: %s", n.Name, string(msg[1:]))
					return nil
				})
			}()
		}
	})

	return nil
}

func getSeriesMeta(sname string) (*seriesMeta, error) {

	s := getServer()
	sinfo, found := s.collection.getSeries(sname)
	if !found {
		return nil, fmt.Errorf("series %s not found", sname)
	}

	buf := bytes.NewBuffer(make([]byte, 0, 8))
	buf.WriteByte(byte(nekolib.OP_SERIES_INFO))
	reqHdr := &nekolib.ReqSeriesMetaHdr{
		SeriesName: sname,
	}
	buf.Write(reqHdr.ToBytes())
	reqMsg := buf.Bytes()

	var wg sync.WaitGroup
	var mutex sync.Mutex
	psinfo := make([]peerSeriesInfo, 0)
	visited := make(map[string]bool)
	s.backends.ForEachSafe(func(n *nekoRingNode) {
		if _, found := visited[n.RealName]; found {
			return
		}
		visited[n.RealName] = true
		wg.Add(1)
		go func() {
			defer wg.Done()
			n.Request(func(psock *zmq.Socket) error {
				if _, err := psock.SendBytes(reqMsg, 0); err != nil {
					logger.Error(err.Error())
					return err
				}
				msg, err := psock.RecvBytes(0)
				if err != nil {
					logger.Error(err.Error())
					return err
				}
				if uint8(msg[0]) != nekolib.REP_OK {
					logger.Error("peer %s", n.Name)
					return errors.New(string(msg[1:]))
				}
				// logger.Debug("peer %s: Done", n.Name)

				ps := new(nekolib.NekodSeriesInfo)
				ps.FromBytes(bytes.NewBuffer(msg[1:]))
				mutex.Lock()
				defer mutex.Unlock()
				psinfo = append(psinfo, peerSeriesInfo{n.RealName, int(ps.Count)})
				return nil
			})
		}()
	})

	wg.Wait()

	return &seriesMeta{*sinfo, psinfo}, nil
}
