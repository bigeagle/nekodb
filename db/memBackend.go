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

package db

import (
    "time"
)


type memoryBackend struct {
    seriesFrags map[string]*memorySeriesFrag
}


type memorySeriesFrag struct {
    id string
    options NekoBackendOptions
    start time.Time
    end time.Time
    records []*NekoRecord
}


func newMemoryBackend() *memoryBackend {
    mb := new(memoryBackend)
    mb.seriesFrags = make(map[string]*memorySeriesFrag)
    return mb
}


func (mb *memoryBackend) GetSeriesFrag(id string) NekoSeriesFrag {
    if msf, found := mb.seriesFrags[id]; found {
        return msf
    }
    return nil
}


func (mb *memoryBackend) NewSeriesFrag(id string, options NekoBackendOptions) error {
    mb.seriesFrags[id] = newMemorySeriesFrag(id, options)
    return nil
}

func newMemorySeriesFrag(id string, options NekoBackendOptions) *memorySeriesFrag {
    msf := new(memorySeriesFrag)
    msf.id = id
    msf.options = options
    msf.start = time.Unix(1<<63-1, 1<<63-1)
    msf.end= time.Unix(0, 0)
    msf.records = make([]*NekoRecord, 0, 128)
    return msf
}

func (sf *memorySeriesFrag) Id() string {
    return sf.id
}

func (sf *memorySeriesFrag) GetByRange(start, end time.Time, to chan<- NekoRecord) {
    defer close(to)
    if sf.start.After(end) {
        return
    }
    for _, r := range(sf.records) {
        if (r.ts.After(start) || r.ts.Equal(start) )&& r.ts.Before(end) {
            to <- *r
        }
    }
}

func (sf *memorySeriesFrag) Insert(records []NekoRecord) error {
    for _, r := range records {
        if sf.start.After(r.ts) {
            sf.start = r.ts
        }
        if sf.end.Before(r.ts) {
            sf.end = r.ts
        }
        sf.records = append(sf.records, &r)
    }
    return nil
}

func (sf *memorySeriesFrag) DeleteByRange(start, end time.Time) error {
    return nil
}

func (sf *memorySeriesFrag) Count() uint64 {
    return uint64(len(sf.records))
}
