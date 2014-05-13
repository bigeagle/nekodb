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
    "flag"
    "os"
    "github.com/bigeagle/nekodb/nekolib"
    gologging "github.com/bigeagle/go-logging"
)

var (
    debug, getVersion bool
    cfgFile string
    logger *gologging.Logger
)

func main() {
    flag.BoolVar(&debug, "debug", false, "Debug Info")
    flag.BoolVar(&getVersion, "version", false, "Print Version")
    flag.StringVar(&cfgFile, "config", "/etc/nekodb/nekos.conf", "Configuration File Path")
    flag.Parse()

    if getVersion {
        nekolib.PrintVersion()
        os.Exit(0)
    }
    nekolib.InitLogger(debug)
    logger = nekolib.GetLogger()

    logger.Info("Starting Nekos Proxy")
    startNekoServer(cfgFile)

}
