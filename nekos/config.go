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
    "github.com/BurntSushi/toml"
)

type nekosConfig struct {
    Addr string             `toml:"addr"`
    Port int                `toml:"port"`
    MaxWorkers int          `toml:"max_workers"`
    BackendPeers []string   `toml:"backend_peers"`
    EtcdPeers []string      `toml:"etcd_peers"`
}

func parseConfig(filename string) (*nekosConfig, error) {
    cfg := new(nekosConfig)
    if _, err := toml.DecodeFile(filename, cfg); err != nil {
        logger.Error(err.Error())
        return nil, err
    } else {
        logger.Debug("%v", cfg)
        return cfg, nil
    }
}
