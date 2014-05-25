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
    // "os"
    // "encoding/binary"
    "bytes"
    "github.com/codegangsta/cli"
    "github.com/bigeagle/nekodb/nekolib"
)

func commandNewSeries(c *cli.Context) {
    fmt.Printf("Nekos: %s:%d\n", srvHost, srvPort)
    s := getSocket(srvHost, srvPort)

    series := nekolib.NekoSeriesInfo{
        Name: c.String("name"),
        Id: c.String("id"),
        FragLevel: c.Int("level"),
    }
    fmt.Println(series)
    buf := bytes.NewBuffer(make([]byte, 0, 16))
    buf.WriteByte(byte(nekolib.OP_NEW_SERIES))
    buf.Write(series.ToBytes())
    fmt.Println(buf.Bytes())
    s.SendBytes(buf.Bytes(), 0)

    b, _ := s.Recv(0)
    fmt.Printf("%v\n", b)
}


