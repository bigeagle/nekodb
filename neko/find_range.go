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
	"fmt"
	"os"
	"time"

	"github.com/bigeagle/nekodb/nekolib"
	"github.com/codegangsta/cli"
)

func commandFindDataPoints(c *cli.Context) {
	sname := c.String("series")
	start_t, err := time.Parse(nekolib.ISO8601, c.String("start"))
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	end_t, err := time.Parse(nekolib.ISO8601, c.String("end"))
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	reqHdr := nekolib.ReqFindByRangeHdr{
		SeriesName: sname,
		StartTs:    nekolib.Time2Bytes(start_t),
		EndTs:      nekolib.Time2Bytes(end_t),
		Priority:   uint8(0),
	}

	buf := bytes.NewBuffer(make([]byte, 0, 16))
	buf.WriteByte(byte(nekolib.OP_FIND_RANGE))
	buf.Write(reqHdr.ToBytes())

	s := getSocket(srvHost, srvPort)
	s.SendBytes(buf.Bytes(), 0)

	rep, _ := s.RecvBytes(0)
	if uint8(rep[0]) != nekolib.REP_ACK {
		fmt.Println("Error", string(rep[1:]))
		return
	}
	count := 0
READ_STREAM:
	for more, _ := s.GetRcvmore(); more; more, _ = s.GetRcvmore() {
		msg, err := s.RecvBytes(0)
		if err != nil {
			fmt.Println("Error", err.Error())
			return
		}

		for buf := bytes.NewBuffer(msg); buf.Len() > 0; {
			r := new(nekolib.NekodRecord)
			if err := r.FromBytes(buf); err != nil {
				if err == nekolib.EndOfStream {
					break READ_STREAM
				}
				fmt.Println("Error", err.Error())
				return
			}

			ts, _ := nekolib.Bytes2Time(r.Ts)
			fmt.Printf("%s, %s\n", ts.Format(nekolib.ISO8601), string(r.Value))
			count++
		}

	}

	rep, err = s.RecvBytes(0)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	if uint8(rep[0]) != nekolib.REP_OK {
		fmt.Fprintln(os.Stderr, "Error", string(rep[1:]))
	} else {
		bench := map[string]interface{}{}
		json.Unmarshal(rep[1:], &bench)
		fmt.Fprintln(os.Stderr, "Profile")
		fmt.Fprintln(os.Stderr, "Total Time: ",
			time.Duration(int(bench["total_time"].(float64)))*time.Nanosecond,
			"Total Count: ", count,
		)

		for peer, ipbench := range bench["bench_peers"].(map[string]interface{}) {
			fmt.Fprintf(os.Stderr, "%s: ", peer)
			pbench := ipbench.(map[string]interface{})
			fmt.Fprintf(os.Stderr, "count: %d, scan_time: %s, query_time: %s\n",
				int(pbench["count"].(float64)),
				time.Duration(int(pbench["duration"].(float64)))*time.Nanosecond,
				time.Duration(int(pbench["full_duration"].(float64)))*time.Nanosecond,
			)
		}
	}

}
