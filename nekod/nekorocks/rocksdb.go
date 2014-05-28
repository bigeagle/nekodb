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
	//    "os"
	"sync"
	// "encoding/binary"
	"github.com/tecbot/gorocksdb"
)

type RocksDB struct {
	dbpath string
	opt    *gorocksdb.Options
	db     *gorocksdb.DB
	m      sync.RWMutex
}

func NewRocksDB(path string, opt *gorocksdb.Options) (*RocksDB, error) {
	if opt == nil {
		opt = gorocksdb.NewDefaultOptions()
		opt.SetCreateIfMissing(true)
	}

	db, err := gorocksdb.OpenDb(opt, path)
	if err != nil {
		return nil, err
	}

	rdb := new(RocksDB)
	rdb.dbpath = path
	rdb.db = db
	rdb.opt = opt

	return rdb, nil
}

func (r *RocksDB) Get(key []byte) (*gorocksdb.Slice, error) {
	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	return r.db.Get(ro, key)
}

func (r *RocksDB) Delete(key []byte) error {
	wo := gorocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	return r.db.Delete(wo, key)
}

func (r *RocksDB) Put(key, value []byte) error {
	wo := gorocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	err := r.db.Put(wo, key, value)
	return err
}

func (r *RocksDB) PutSync(key, value []byte) error {
	wo := gorocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	wo.SetSync(true)
	return r.db.Put(wo, key, value)
}

func (r *RocksDB) Write(batch *gorocksdb.WriteBatch) error {
	wo := gorocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	return r.db.Write(wo, batch)
}

func (r *RocksDB) Merge(key, value []byte) error {
	wo := gorocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	return r.db.Merge(wo, key, value)
}

func (r *RocksDB) NewIterator() *gorocksdb.Iterator {
	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	ro.SetFillCache(false)
	return r.db.NewIterator(ro)
}

func (r *RocksDB) Destroy() error {
	r.db.Close()
	return gorocksdb.DestroyDb(r.dbpath, r.opt)
}

func (r *RocksDB) Close() {
	r.db.Close()
}
