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
    "time"
    "fmt"
    "encoding/json"
    "github.com/coreos/go-etcd/etcd"
    "github.com/bigeagle/nekodb/nekolib"
)


func virtualName(name string, number int) string {
    return fmt.Sprintf("%s-%d", name, number)
}

func refreshPeer(s *nekoBackendServer) error {
    var vnode nekodPeerInfo
    logger.Debug("Refresh Peer Info")
    for i := 0; i < s.cfg.Virtuals; i++ {
        vname := virtualName(s.cfg.Name, i)
        vnode.Name = vname
        vnode.RealName = s.cfg.Name
        vnode.Hostname = s.cfg.Hostname
        vnode.Port = s.cfg.Port

        vn, _ := json.Marshal(vnode)
        key := fmt.Sprintf("%s/%s", nekolib.ETCD_PEER_DIR, vname)
        s.ec.Set(key, string(vn), nekolib.ETCD_REFRESH_INTERVAL)
    }
    return nil
}

func handleEtcd(s *nekoBackendServer) error {
    s.ec = etcd.NewClient(s.cfg.EtcdPeers)
    _, err := s.ec.Get(nekolib.ETCD_DIR, true, true)

    if err != nil {
        logger.Error(err.Error())
        return err
    }

    refreshPeer(s)
    go func() {
        t :=  time.Tick((nekolib.ETCD_REFRESH_INTERVAL-5)*time.Second)
        for {
            <-t
            refreshPeer(s)
        }
    }()

    return nil
}


