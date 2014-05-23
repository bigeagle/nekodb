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
    "strings"
    // zmq "github.com/pebbe/zmq4"
    // "github.com/coreos/go-etcd/etcd"
    "github.com/bigeagle/nekodb/nekolib"
)

type nekoSeries nekolib.NekoSeriesInfo

type nekoCollection struct {
    m sync.RWMutex
    coll map[string](map[string]*nekoSeries)
}

func newNekoCollection() *nekoCollection {
    c := new(nekoCollection)
    c.coll = make(map[string](map[string]*nekoSeries))
    return c
}

func (c *nekoCollection) ensureCollection_unsafe(name string) {
    if _, ok := c.coll[name]; !ok {
        sc := make(map[string]*nekoSeries)
        c.coll[name] = sc
    }
}

func (c *nekoCollection) ensureCollection(name string) {
    c.m.Lock()
    defer c.m.Unlock()
    c.ensureCollection_unsafe(name)
}

func (c *nekoCollection) insertSeries_unsafe(collName string, series *nekoSeries) {
    c.ensureCollection_unsafe(collName)
    if series != nil {
        c.coll[collName][series.Name] = series
    }
}

func (c *nekoCollection) insertSeries(collName string, series *nekoSeries) {
    c.m.Lock()
    defer c.m.Unlock()
    c.insertSeries_unsafe(collName, series)
}

func (c *nekoCollection) String() string {
    c.m.RLock()
    defer c.m.RUnlock()
    colls := make([]string, 0)
    for cname, coll := range c.coll {
        series := make([]string, 0)
        for sname, _ := range coll {
            series = append(series, sname)
        }
        colls = append(colls,
            fmt.Sprintf("{%s: %s}", cname, strings.Join(series, ",")))
    }
    return strings.Join(colls, ", ")
}
