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
	"encoding/json"
	"fmt"
	"os"

	"github.com/bigeagle/nekodb/nekolib"
	"github.com/codegangsta/cli"
)

func commandListSeries(c *cli.Context) {
	s := getSocket(srvHost, srvPort)
	s.SendBytes([]byte{nekolib.OP_LIST_SERIES}, 0)

	rep, err := s.RecvBytes(0)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	if uint8(rep[0]) != nekolib.REP_OK {
		fmt.Fprintln(os.Stderr, "Error", string(rep[1:]))
	} else {
		var seriesList []nekolib.NekoSeriesMeta
		json.Unmarshal(rep[1:], &seriesList)
		for _, series := range seriesList {
			fmt.Printf(
				"name: %s, id: %s, count: %d, fragLevel: %d\n",
				series.Name,
				series.Id,
				series.Count,
				series.FragLevel,
			)
		}
	}

}
