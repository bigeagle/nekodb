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
	// "fmt"
	// "os"
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/bigeagle/nekodb/nekolib"
	"github.com/codegangsta/cli"
	zmq "github.com/pebbe/zmq4"
)

func commandImportSeries(c *cli.Context) {
	fmt.Printf("Nekos: %s:%d\n", srvHost, srvPort)
	var err error

	seriesName := c.String("name")
	if seriesName == "" {
		fmt.Printf("Series Name must not be empty")
		return
	}

	seriesFileName := c.Args()[0]
	fi, err := os.Open(seriesFileName)

	if err != nil {
		fmt.Println(err)
		return
	}
	defer fi.Close()

	s := getSocket(srvHost, srvPort)

	reqHdr := &nekolib.ReqImportSeriesHdr{
		SeriesName: seriesName,
	}
	buf := bytes.NewBuffer(make([]byte, 0))
	buf.WriteByte(byte(nekolib.OP_IMPORT_SERIES))
	buf.Write(reqHdr.ToBytes())
	// s.SendBytes(buf.Bytes(), zmq.SNDMORE)
	s.SendBytes(buf.Bytes(), zmq.SNDMORE)

	reader := bufio.NewReader(fi)

	for {
		line, _, err := reader.ReadLine()
		if err != nil {
			break
		}
		tokens := strings.Split(string(line), ",")
		t, _ := time.Parse(time.RFC3339Nano, tokens[0])
		record := nekolib.NekodRecord{
			Ts:    nekolib.Time2Bytes(t),
			Value: []byte(tokens[1]),
		}
		s.SendBytes(record.ToBytes(), zmq.SNDMORE)
	}
	if err == io.EOF || err == nil {
		fmt.Println("EOF")
		s.SendBytes([]byte{0, 0}, 0)
	} else {
		fmt.Println(err.Error())
	}

	// s := getSocket(srvHost, srvPort)

	// series := nekolib.NekoSeriesInfo{
	//     Name: c.String("name"),
	//     Id: c.String("id"),
	//     FragLevel: c.Int("level"),
	// }
	// fmt.Println(series)
	// buf := bytes.NewBuffer(make([]byte, 0, 16))
	// buf.WriteByte(byte(nekolib.OP_NEW_SERIES))
	// buf.Write(series.ToBytes())
	// fmt.Println(buf.Bytes())
	// s.SendBytes(buf.Bytes(), 0)

	b, _ := s.Recv(0)
	fmt.Printf("%v\n", b)

}
