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
    // "strings"
    //zmq "github.com/pebbe/zmq4"
    "github.com/coreos/go-etcd/etcd"
    "github.com/bigeagle/nekodb/nekolib"
)


func initEtcd() error {
    s := getServer()
    s.ec = etcd.NewClient(s.cfg.EtcdPeers)
    _, err := s.ec.Get(nekolib.ETCD_DIR, false, false)

    if err != nil {
        logger.Error(err.Error())
        return err
    }
    if err = initPeers(); err != nil {
        logger.Error(err.Error())
        return err
    }
    if err = initCollections(); err != nil {
        logger.Error(err.Error())
        return err
    }

    logger.Info("Watching for peer udpates")
    go s.ec.Watch(nekolib.ETCD_PEER_DIR, 0, true, s.peerChan, nil)
    logger.Info("Watching for collection and series udpates")
    go s.ec.Watch(nekolib.ETCD_SERIES_DIR, 0, true, s.seriesChan, nil)
    go handlePeerUpdate()
    go handleCollectionUpdate()
    logger.Debug("%v", s.collection)

    return nil
}

func initPeers() error {
    s := getServer()
    r, err := s.ec.Get(nekolib.ETCD_PEER_DIR, true, true)
    if err != nil {
        logger.Error(err.Error())
        return err
    } else {
        for _, vn := range r.Node.Nodes {
            var vnode nekolib.NekodPeerInfo
            if err = json.Unmarshal([]byte(vn.Value), &vnode); err == nil {
                s.backends.Insert(&vnode)
            }
            // logger.Info("%v", vnode)
        }
        return nil
    }
}

func initCollections() error {
    s := getServer()
    r, err := s.ec.Get(nekolib.ETCD_SERIES_DIR, true, true)
    if err != nil {
        logger.Error(err.Error())
        return err
    } else {
        for _, sNode := range r.Node.Nodes {
            series := new(nekoSeries)
            if err = json.Unmarshal([]byte(sNode.Value), series); err == nil {
                s.collection.insertSeries(series)
            }
        }
    }

    return nil
}

func handlePeerUpdate() {
    s := getServer()
    for {
        update := <-s.peerChan
        // logger.Debug("%s: %v", update.Action, update.Node)
        vname := update.Node.Key[len(nekolib.ETCD_PEER_DIR)+1:]

        switch update.Action {
        case "expire", "delete":
            s.backends.Remove(vname)
        default:
            var vnode nekolib.NekodPeerInfo
            if err := json.Unmarshal([]byte(update.Node.Value), &vnode); err == nil {
                switch vnode.Flag {
                case nekolib.PEER_FLG_NEW:
                    logger.Debug("insert")
                    s.backends.Insert(&vnode)
                case nekolib.PEER_FLG_UPDATE:
                    s.backends.UpdateInfo(vname, &vnode)
                case nekolib.PEER_FLG_RESET:
                    logger.Debug("reset")
                    s.backends.ResetPeer(vname, &vnode)
                default:
                    continue
                }
            }
        }

        logger.Debug("%v", s.backends)
    }
}

func handleCollectionUpdate() {

    s := getServer()
    for {
        update := <-s.seriesChan

        logger.Debug("%s: %v", update.Action, update.Node)
        sname := update.Node.Key[len(nekolib.ETCD_SERIES_DIR)+1:]

        switch update.Action {
        case "expire", "delete":
            s.collection.removeSeries(sname)
        default:
            series := new(nekoSeries)
            if err := json.Unmarshal([]byte(update.Node.Value), series); err == nil {
                s.collection.insertSeries(series)
            }
        }

    }
}

