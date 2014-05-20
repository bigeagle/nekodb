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
    "flag"
    "io/ioutil"
    "github.com/bigeagle/nekodb/nekolib"
    gologging "github.com/bigeagle/go-logging"
)

var (
    logger *gologging.Logger
)

func main() {
    var cfgFile string
    var showVersion, debug bool

    f := flag.NewFlagSet("nekod", -1)
    f.SetOutput(ioutil.Discard)
    f.BoolVar(&showVersion, "version", false, "Print Version")
    f.StringVar(&cfgFile, "config", "", "Configuration File Path")
    f.BoolVar(&debug, "debug", false, "Debug Mode")
    f.Parse(os.Args[1:])

    if showVersion {
        nekolib.PrintVersion()
        os.Exit(0)
    }

    nekolib.InitLogger(debug)
    logger = nekolib.GetLogger()


    cfg, err := loadConfig(cfgFile, os.Args[1:])
    if err != nil {
        return
    }

    logger.Info("Starting Nekodb Backend")
    startNekoBackendServer(cfg)
}
