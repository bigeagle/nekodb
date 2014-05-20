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
    zmq "github.com/pebbe/zmq4"
    "github.com/coreos/go-etcd/etcd"
//    "github.com/bigeagle/nekodb/nekolib"
)

type nekodPeerInfo struct {
    Name string `json:"name"`
    RealName string `json:"real_name"`
    Hostname string `json:"hostname"`
    Port int `json:"port"`
}

type nekoBackendServer struct {
    cfg *backendServerCfg
    ec *etcd.Client
}

func startNekoBackendServer(cfg *backendServerCfg) (error) {
    srv := new(nekoBackendServer)
    srv.cfg = cfg
    if err := srv.init(); err != nil {
        return err
    }
    srv.serveForever()
    return nil
}

func (s *nekoBackendServer) init() error {
    if err := handleEtcd(s); err != nil {
        return err
    }
    return nil
}


func (s *nekoBackendServer) serveForever() {
    clients, _ := zmq.NewSocket(zmq.ROUTER)
    defer clients.Close()
    clients.Bind(fmt.Sprintf("tcp://%s:%d", s.cfg.Addr, s.cfg.Port))

    workers, _ := zmq.NewSocket(zmq.DEALER)
    defer workers.Close()
    workers.Bind(workerAddr)

    for i :=0; i < s.cfg.MaxWorkers; i++ {
        go startWorker(s)
    }

    err := zmq.Proxy(clients, workers, nil)
    logger.Fatalf("Proxy Exited: %s", err.Error())

}

