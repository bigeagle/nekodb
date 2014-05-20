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
//     "github.com/bigeagle/nekodb/nekolib"
    "fmt"
    "sync"
    zmq "github.com/pebbe/zmq4"
    "github.com/coreos/go-etcd/etcd"
)


type nekodPeer struct {
    RealName string
    Hostname string
    Port int
}


type nekoServer struct {
    m  sync.RWMutex
    cfg *nekosConfig
    ec *etcd.Client
    peerChan chan *etcd.Response
    backends map[string]*nekodPeer
}

func startNekoServer(cfg *nekosConfig) (error) {
    srv := new(nekoServer)
    srv.cfg = cfg
    srv.peerChan = make(chan *etcd.Response)
    srv.backends = make(map[string]*nekodPeer)
    if err := srv.init(); err != nil {
        return err
    }
    srv.serveForever()
    return nil
}

func (s *nekoServer) init() error {
    if err := handleEtcd(s); err != nil {
        return err
    }
    logger.Info("%v", s.backends)
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

