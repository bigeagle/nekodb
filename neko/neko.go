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
	"os"

	"github.com/bigeagle/nekodb/nekolib"
	"github.com/codegangsta/cli"
)

var (
	srvHost string
	srvPort int
)

func main() {
	app := cli.NewApp()
	app.Name = "nekoctl"
	app.Usage = "Cli tool for nekodb"
	app.Version = nekolib.VERSION
	app.Flags = []cli.Flag{
		cli.StringFlag{"host, H", "localhost", "Neko Server Host"},
		cli.IntFlag{"port, p", 2345, "Neko Server Port"},
	}
	app.Commands = []cli.Command{
		{
			Name:   "list",
			Usage:  "List Series",
			Flags:  []cli.Flag{},
			Action: commandListSeries,
		},
		{
			Name:  "import",
			Usage: "Import ts file to nekodb",
			Flags: []cli.Flag{
				cli.StringFlag{"name, n", "", "Series Name"},
				cli.StringFlag{"id", "", "Series Id"},
				cli.IntFlag{"level, l", nekolib.SLICE_FRAG_LEVEL_DEFAULT, "Fragmentation Level"},
			},
			Action: commandImportSeries,
		},
		{
			Name:  "new-series",
			Usage: "Create New Series",
			Flags: []cli.Flag{
				cli.StringFlag{"name, n", "", "Series Name"},
				cli.StringFlag{"id", "", "Series Id"},
				cli.IntFlag{"level, l", nekolib.SLICE_FRAG_LEVEL_DEFAULT, "Fragmentation Level"},
			},
			Action: commandNewSeries,
		},
		{
			Name:  "find",
			Usage: "Find data points",
			Flags: []cli.Flag{
				cli.StringFlag{"series, s", "", "Series Name"},
				cli.StringFlag{"start", "", "Start Time, eg: 1970-01-01T00:00:00.000+0800"},
				cli.StringFlag{"end", "", "End Time, eg: 2012-12-21T23:59:59.999+0800"},
			},
			Action: commandFindDataPoints,
		},
	}
	app.Before = func(c *cli.Context) error {
		srvHost = c.String("host")
		srvPort = c.Int("port")
		return nil
	}
	app.Run(os.Args)
}
