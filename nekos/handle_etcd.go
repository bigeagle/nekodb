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
    // "time"
    // "fmt"
    "encoding/json"
    "github.com/coreos/go-etcd/etcd"
    "github.com/bigeagle/nekodb/nekolib"
)


func handleEtcd(s *nekoServer) error {
    s.ec = etcd.NewClient(s.cfg.EtcdPeers)
    r, err := s.ec.Get(nekolib.ETCD_PEER_DIR, true, true)

    if err != nil {
        logger.Error(err.Error())
        return err
    } else {
        s.m.Lock()
        defer s.m.Unlock()
        for _, vn := range r.Node.Nodes {
            var vnode nekolib.NekodPeerInfo
            json.Unmarshal([]byte(vn.Value), &vnode)
            // logger.Info("%v", vnode)
            peer := new(nekodPeer)
            peer.RealName = vnode.RealName
            peer.Hostname = vnode.Hostname
            peer.Port = vnode.Port
            s.backends[vnode.Name] = peer
        }
    }

    logger.Info("Watching for peer udpates")
    go s.ec.Watch(nekolib.ETCD_PEER_DIR, 0, true, s.peerChan, nil)
    go handlePeerUpdate(s)

    return nil
}


func handlePeerUpdate(s *nekoServer) {
    updatePeer := func(action string, vname string, value string) {
        s.m.Lock()
        defer s.m.Unlock()
        switch action {
        case "expire", "delete":
            delete(s.backends, vname)
        default:
            var vnode nekolib.NekodPeerInfo
            json.Unmarshal([]byte(value), &vnode)
            peer := new(nekodPeer)
            peer.RealName = vnode.RealName
            peer.Hostname = vnode.Hostname
            peer.Port = vnode.Port
            s.backends[vnode.Name] = peer
        }
    }

    for {
        update := <-s.peerChan
        logger.Debug("%s: %v", update.Action, update.Node)
        vname := update.Node.Key[len(nekolib.ETCD_PEER_DIR)+1:]
        updatePeer(update.Action, vname, update.Node.Value)
        logger.Debug("%v", s.backends)
    }
}
