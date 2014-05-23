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
    "sync"
    zmq "github.com/pebbe/zmq4"
    "github.com/coreos/go-etcd/etcd"
    // "github.com/bigeagle/nekodb/nekolib"
)


type nekoServer struct {
    m  sync.RWMutex
    cfg *nekosConfig
    ec *etcd.Client
    peerChan, seriesChan chan *etcd.Response
    backends *nekoBackendRing
    collections *nekoCollection
}

func startNekoServer(cfg *nekosConfig) (error) {
    srv = new(nekoServer)
    srv.cfg = cfg
    srv.peerChan = make(chan *etcd.Response)
    srv.seriesChan = make(chan *etcd.Response)
    srv.backends = newNekoBackendRing()
    srv.collections = newNekoCollection()
    if err := srv.init(); err != nil {
        return err
    }
    srv.serveForever()
    return nil
}

func getServer() *nekoServer {
    return srv
}

func (s *nekoServer) init() error {
    if err := initEtcd(); err != nil {
        return err
    }
    return nil

}


func (s *nekoServer) serveForever() {
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

