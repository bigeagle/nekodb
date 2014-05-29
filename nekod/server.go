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
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bigeagle/nekodb/nekod/nekorocks"
	"github.com/bigeagle/nekodb/nekolib"
	"github.com/coreos/go-etcd/etcd"
	zmq "github.com/pebbe/zmq4"
)

type nekoBackendServer struct {
	m          sync.RWMutex
	cfg        *backendServerCfg
	ec         *etcd.Client
	state      uint32
	seriesColl map[string]*nekorocks.Series
}

func startNekoBackendServer(cfg *backendServerCfg) error {
	srv = new(nekoBackendServer)
	srv.cfg = cfg
	srv.ec = nil
	srv.seriesColl = make(map[string]*nekorocks.Series)
	if err := srv.init(); err != nil {
		return err
	}
	srv.serveForever()
	return nil
}

func getServer() *nekoBackendServer {
	return srv
}

func (s *nekoBackendServer) setState(state int) {
	atomic.StoreUint32(&s.state, uint32(state))
	if s.ec != nil {
		s.refreshPeer(nekolib.PEER_FLG_UPDATE)
	}
}

func (s *nekoBackendServer) init() error {
	s.setState(nekolib.STATE_INIT)
	nekorocks.InitNekoRocks(s.cfg.DataPath, logger)
	if err := s.handleEtcd(); err != nil {
		return err
	}
	if err := s.initSeries(); err != nil {
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

	for i := 0; i < s.cfg.MaxWorkers; i++ {
		go startWorker(i, s)
	}

	go func() {
		time.Sleep(500 * time.Millisecond)
		s.setState(nekolib.STATE_READY)
	}()

	err := zmq.Proxy(clients, workers, nil)
	logger.Fatalf("Proxy Exited: %s", err.Error())

}

func (s *nekoBackendServer) initSeries() error {
	r, err := s.ec.Get(nekolib.ETCD_SERIES_DIR, true, true)
	if err != nil {
		logger.Error(err.Error())
		return err
	} else {
		for _, sNode := range r.Node.Nodes {
			var sInfo nekolib.NekoSeriesInfo

			if err = json.Unmarshal([]byte(sNode.Value), &sInfo); err == nil {
				var series *nekorocks.Series
				dbpath := path.Join(s.cfg.DataPath, sInfo.Id)

				if stat, err := os.Stat(dbpath); os.IsNotExist(err) {
					// If DBPath not inited, re-initialize series
					series, err = nekorocks.NewSeries(sInfo.Name, sInfo.Id, sInfo.FragLevel)
					if err != nil {
						logger.Error(err.Error())
						return err
					}
				} else if stat.IsDir() {
					// If DBPath presented, init from series files
					series, err = nekorocks.GetSeries(sInfo.Id)
					if err != nil {
						logger.Error(err.Error())
						return err
					}
				} else {
					return fmt.Errorf("Invalid DB Directory: %s", dbpath)
				}

				s.m.Lock()
				s.seriesColl[sInfo.Name] = series
				s.m.Unlock()

			} else {
				logger.Error(err.Error())
				return err
			}
		}
	}
	return nil
}

func (s *nekoBackendServer) NewSeries(name, id string, frag_level int) error {
	series, err := nekorocks.NewSeries(name, id, frag_level)
	if err != nil {
		logger.Error(err.Error())
		return err
	}

	s.m.Lock()
	defer s.m.Unlock()
	s.seriesColl[name] = series
	return nil
}

func (s *nekoBackendServer) GetSeries(name string) (*nekorocks.Series, bool) {
	s.m.RLock()
	defer s.m.RUnlock()
	series, found := s.seriesColl[name]
	return series, found
}
