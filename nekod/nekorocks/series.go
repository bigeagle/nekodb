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
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path"
	"sync"

	"github.com/bigeagle/nekodb/nekolib"
	. "github.com/tecbot/gorocksdb"
	"github.com/vmihailenco/msgpack"
)

const (
	TS_KEY_LEN             = 15
	SERIES_META_PREFIX_LEN = 4
	SERIES_KEY_PREFIX_LEN  = 1
	KEY_SERIES_NAME        = "srs_name"
	KEY_SERIES_ID          = "srs_id"
	KEY_SERIES_FRAG_LEVEL  = "srs_fragLevel"
	KEY_SERIES_ELEM_COUNT  = "elm_count"
	PREFIX_SERIES_KEY_MAP  = "key_"
)

var (
	InvalidTimestamp = errors.New("Invalid Binary Timestamp")
)

type Record struct {
	Key   []byte
	Value []byte
}

type Series struct {
	m sync.RWMutex

	data *RocksDB
	meta *RocksDB

	Name      string
	Id        string
	FragLevel int

	dbpath string
}

func NewSeries(name, id string, fragLevel int) (*Series, error) {
	s, err := GetSeries(id)
	if err != nil {
		return nil, err
	}
	s.Name = name
	s.Id = id
	s.FragLevel = fragLevel

	s.meta.PutSync([]byte(KEY_SERIES_NAME), []byte(name))
	s.meta.PutSync([]byte(KEY_SERIES_ID), []byte(id))
	s.meta.PutSync([]byte(KEY_SERIES_FRAG_LEVEL), []byte{byte(fragLevel)})
	s.meta.PutSync([]byte(KEY_SERIES_ELEM_COUNT), []byte{0, 0, 0, 0, 0, 0, 0, 0})

	return s, nil
}

func GetSeries(id string) (*Series, error) {
	if !inited {
		return nil, NotInited
	}
	logger.Debug("Open series: %s", id)
	var err error
	s := new(Series)

	s.dbpath = path.Join(DB_PATH, id)
	if _, err = os.Stat(s.dbpath); os.IsNotExist(err) {
		err = os.MkdirAll(s.dbpath, os.ModeDir|os.FileMode(0700))
		if err != nil {
			return nil, err
		}
	}

	opts := NewDefaultOptions()
	// opts.SetBlockCache(NewLRUCache(4<<30)) // 4MB cache
	// opts.SetPrefixExtractor(NewFixedPrefixTransform(5)) // {data_, meta_}
	opts.SetPrefixExtractor(NewFixedPrefixTransform(SERIES_KEY_PREFIX_LEN)) // {0, 1, 2, 3, ..., 255}
	opts.SetFilterPolicy(NewBloomFilter(10))
	opts.SetCreateIfMissing(true)

	s.data, err = NewRocksDB(path.Join(s.dbpath, "data"), opts)
	if err != nil {
		return nil, err
	}

	mopts := NewDefaultOptions()
	mopts.SetMergeOperator(new(int64AddOperator))
	mopts.SetCreateIfMissing(true)
	mopts.SetMaxSuccessiveMerges(10)
	mopts.SetPrefixExtractor(NewFixedPrefixTransform(SERIES_META_PREFIX_LEN)) // {srs_, elm_, key_}
	s.meta, err = NewRocksDB(path.Join(s.dbpath, "meta"), mopts)
	if err != nil {
		return nil, err
	}

	if slice, err := s.meta.Get([]byte(KEY_SERIES_NAME)); err == nil {
		b := slice.Data()
		if len(b) > 0 {
			s.Name = string(b)
		}
		slice.Free()
	} else {
		return nil, err
	}

	if slice, err := s.meta.Get([]byte(KEY_SERIES_ID)); err == nil {
		b := slice.Data()
		if len(b) > 0 {
			s.Id = string(b)
		}
		slice.Free()
	} else {
		return nil, err
	}

	if slice, err := s.meta.Get([]byte(KEY_SERIES_FRAG_LEVEL)); err == nil {
		b := slice.Data()
		if len(b) > 0 {
			s.FragLevel = int(b[0])
		}
		slice.Free()
	} else {
		return nil, err
	}

	return s, nil
}

func (s *Series) Count() (int, error) {
	// s.m.RLock()
	// defer s.m.RUnlock()

	bcount, err := s.meta.Get([]byte(KEY_SERIES_ELEM_COUNT))
	if err != nil {
		return -1, err
	}
	defer bcount.Free()
	return int(binary.BigEndian.Uint64(bcount.Data())), nil
}

func (s *Series) marshalKey(key []byte, priority uint8) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, SERIES_KEY_PREFIX_LEN+TS_KEY_LEN))
	buf.WriteByte(byte(priority))
	buf.Write(key)
	return buf.Bytes()
}

func (s *Series) unmarshalKey(_key []byte) []byte {
	return _key[SERIES_KEY_PREFIX_LEN:]
}

func (s *Series) Insert(key, value []byte, priority uint8) error {
	if len(key) != TS_KEY_LEN {
		return InvalidTimestamp
	}

	key = s.marshalKey(key, priority)
	if err := s.data.Put(key, value); err == nil {
		s.addCount(int64(1))
		return nil
	} else {
		return err
	}
}

func (s *Series) InsertBatch(records []Record, priority uint8) error {
	batch := NewWriteBatch()
	defer batch.Destroy()
	for _, r := range records {
		if len(r.Key) != TS_KEY_LEN {
			return InvalidTimestamp
		}
		key := s.marshalKey(r.Key, priority)
		batch.Put(key, r.Value)
	}

	if err := s.data.Write(batch); err == nil {
		s.addCount(int64(len(records)))
		return nil
	} else {
		return err
	}
}

func (s *Series) RangeOp(start, end []byte, priority uint8, op func(key, value []byte)) {
	endTs, _ := nekolib.Bytes2Time(end)

	start = s.marshalKey(start, priority)
	end = s.marshalKey(end, priority)

	iter := s.data.NewIterator()
	defer iter.Close()
	for iter.Seek(start); iter.Valid(); iter.Next() {
		key := iter.Key().Data()
		// Continue if invalid key
		if len(key) != TS_KEY_LEN+SERIES_KEY_PREFIX_LEN {
			continue
		}
		// Stop if passed prefix
		if key[0] != byte(priority) {
			break
		}

		cur := s.unmarshalKey(key)

		// stop if went after end
		curTs, _ := nekolib.Bytes2Time(cur)
		if curTs.After(endTs) {
			break
		}
		v := iter.Value().Data()
		op(cur, v)
	}
}

func (s *Series) ReverseHash(h []byte, ts_start, ts_end []byte) error {
	m := map[string]interface{}{
		"ts_start": ts_start,
		"ts_end":   ts_end,
	}
	value, _ := msgpack.Marshal(m)
	key := bytes.NewBuffer(make([]byte, 0, SERIES_META_PREFIX_LEN+4))
	key.Write([]byte(PREFIX_SERIES_KEY_MAP))
	key.Write(h)
	return s.meta.PutSync(h, value)
}

func (s *Series) addCount(n int64) error {
	key := []byte(KEY_SERIES_ELEM_COUNT)
	buf := bytes.NewBuffer(make([]byte, 0, 8))
	binary.Write(buf, binary.BigEndian, n)
	return s.meta.Merge(key, buf.Bytes())
}

func (s *Series) Destroy() error {
	err := s.data.Destroy()
	if err != nil {
		return err
	}
	err = s.meta.Destroy()
	if err != nil {
		return err
	}
	err = os.RemoveAll(s.dbpath)
	if err != nil {
		return err
	}
	return nil
}
