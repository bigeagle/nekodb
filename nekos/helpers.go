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
	"errors"

	"github.com/bigeagle/nekodb/nekolib"
	zmq "github.com/pebbe/zmq4"
)

func getRangeToChan(reqHdr *nekolib.ReqFindByRangeHdr, recordChan chan nekolib.SCNode) error {
	s := getServer()
	sortedChannel := nekolib.NewSortedChannel(16, recordChan)

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
					} else {
						logger.Debug("peer %s: %s", n.Name, string(msg[1:]))
					}
					return nil
				})
			}()
		}
	})

	return nil
}
