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

package nekorocks

import (
    "sync"
    "encoding/binary"
    . "github.com/tecbot/gorocksdb"
)


type Series struct {
    m sync.RWMutex
    opts *Options
    wo_async *WriteOptions
    wo_sync *WriteOptions
    ro *ReadOptions
    db *DB

    Name string
    Id string
}

func NewSeries(name, id string) (*Series, error) {
    s := new(Series)
    opts := NewDefaultOptions()
    // opts.SetBlockCache(NewLRUCache(4<<30)) // 4MB cache
    opts.SetFilterPolicy(NewBloomFilter(10))
    opts.SetPrefixExtractor(NewFixedPrefixTransform(5)) // {data_, meta_}
    opts.SetCreateIfMissing(true)
    db, err := OpenDb(opts, getDBPath(id))
    if err != nil {
        return nil, err
    }
    s.opts = opts
    s.db = db
    s.wo_async = NewDefaultWriteOptions()
    s.wo_async.SetSync(false)
    s.wo_sync = NewDefaultWriteOptions()
    s.wo_async.SetSync(true)
    s.ro = NewDefaultReadOptions()

    s.Name = name
    s.Id = id

    s.db.Put(s.wo_sync, []byte("meta_id"), []byte(id))
    s.db.Put(s.wo_sync, []byte("meta_name"), []byte(name))

    return s, nil
}

func (s *Series) Count() (int, error) {
    s.m.RLock()
    defer s.m.RUnlock()

    bcount, err := s.db.Get(s.ro, []byte("meta_count"))
    if err != nil {
        return -1, err
    }
    defer bcount.Free()
    return int(binary.BigEndian.Uint64(bcount.Data())), nil
}
