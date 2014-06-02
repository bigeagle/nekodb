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
 * Copyright (C) Justin, 2014
 */
package main

import (
	"flag"
	"os"
	"strings"

	"github.com/BurntSushi/toml"
)

type Config struct {
	Addr       string   `toml:"addr"`
	Port       int      `toml:"port"`
	MaxWorkers int      `toml:"max_workers"`
	Name       string   `toml:"name"`
	Domain     string   `toml:"domain"`
	Hostname   string   `toml:"hostname"`
	Virtuals   int      `toml:"virtuals"`
	DataPath   string   `toml:"data_path"`
	Debug      bool     `toml:"debug"`
	EtcdPeers  []string `toml:"etcd_peers"`
}

func loadConfig(cfgFile string, arguments []string) (*Config, error) {
	var etcdPeers string
	var showVersion bool

	hostname, _ := os.Hostname()

	cfg := new(Config)

	cfg.Addr = "127.0.0.1"
	cfg.Port = 1234
	cfg.MaxWorkers = 4
	cfg.Name = hostname
	cfg.Domain = ""
	cfg.Hostname = ""
	cfg.DataPath = "/var/lib/nekodb"
	cfg.Virtuals = 1
	cfg.Debug = false

	if cfgFile != "" {
		if _, err := toml.DecodeFile(cfgFile, cfg); err != nil {
			logger.Error(err.Error())
			return nil, err
		}
		if cfg.Hostname == "" {
			if cfg.Domain == "" {
				cfg.Hostname = "localhost"
			} else {
				cfg.Hostname = hostname + "." + cfg.Domain
			}
		}

	}

	f := flag.NewFlagSet("nekod", flag.ContinueOnError)

	f.StringVar(&cfg.Addr, "addr", cfg.Addr, "Bind Addr")
	f.IntVar(&cfg.Port, "port", cfg.Port, "Bind Port")
	f.IntVar(&cfg.MaxWorkers, "max-workers", cfg.MaxWorkers, "Max worker threads")
	f.StringVar(&cfg.Name, "name", cfg.Name, "Peer Name")
	f.StringVar(&cfg.Domain, "domain name", cfg.Domain, "Domain Name")
	f.StringVar(&cfg.Hostname, "hostname", cfg.Hostname, "Host Name")
	f.IntVar(&cfg.Virtuals, "virtuals", cfg.Virtuals, "Number of virtual nodes")
	f.StringVar(&cfg.DataPath, "data-path", cfg.DataPath, "Path to store data")
	f.StringVar(&etcdPeers, "etcd-peers", "", "Etcd peers")
	f.BoolVar(&cfg.Debug, "debug", cfg.Debug, "Debug Mode")

	// Begin Ignored  (for usage message)
	f.BoolVar(&showVersion, "version", false, "Print Version")
	f.StringVar(&cfgFile, "config", "", "Configuration File Path")
	// End Ignored

	if err := f.Parse(arguments); err != nil {
		return nil, err
	} else {
		if etcdPeers != "" {
			cfg.EtcdPeers = strings.Split(etcdPeers, ",")
		}

		logger.Debug("%v", cfg)
		return cfg, nil
	}

}
