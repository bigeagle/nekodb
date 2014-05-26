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
    "bytes"
    "encoding/json"
    zmq "github.com/pebbe/zmq4"
    "github.com/bigeagle/nekodb/nekolib"
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
    done := make(chan struct{})

    s.backends.ForEachSafe(func (n *nekoRingNode){
        if _, found := visited[n.RealName]; !found {
            visited[n.RealName] = true
            go func(){
                // logger.Debug(n.RealName)
                n.Request(func (s *zmq.Socket) error {
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
                done <- struct{}{}
            }()
        }
    })

    for i := 0; i < int(s.backends.real_peer_count); i++ {
        <-done
    }
    return nil
}


// func importSeries(sname )



