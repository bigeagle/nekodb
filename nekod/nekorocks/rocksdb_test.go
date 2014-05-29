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
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/tecbot/gorocksdb"
	"github.com/vmihailenco/msgpack"
)

func TestRocksdbBasic(t *testing.T) {
	dbpath := path.Join(os.TempDir(), "nekorocks_basic")

	Convey("Subject: Basic RocksDB Test", t, func() {
		db, err := NewRocksDB(dbpath, nil)

		Convey("Error should be nil", func() {
			So(err, ShouldBeNil)
		})

		Convey("DB Should Empty", func() {
			key := []byte("data-5")
			slice, err := db.Get(key)
			So(err, ShouldBeNil)
			So(slice.Size(), ShouldEqual, 0)
		})

		Convey("Data should be accessed", func() {
			for i := 0; i < 10; i++ {
				key := []byte(fmt.Sprintf("data-%d", i))
				value, err := msgpack.Marshal(
					map[string]interface{}{"data": i})
				So(err, ShouldBeNil)
				err = db.Put(key, value)
				So(err, ShouldBeNil)
			}

			Convey("Getter should Run", func() {
				key := []byte("data-5")
				slice, err := db.Get(key)
				So(err, ShouldBeNil)
				value := map[string]interface{}{}
				err = msgpack.Unmarshal(slice.Data(), &value)
				So(err, ShouldBeNil)
				v, ok := value["data"].(int64)
				So(ok, ShouldBeTrue)
				So(v, ShouldEqual, 5)
			})

			Convey("Iterator Should Run", func() {
				iter := db.NewIterator()
				defer iter.Close()
				iter.Seek([]byte("data-1"))
				for i := 1; iter.Valid(); iter.Next() {
					slice := iter.Value()
					value := map[string]interface{}{}
					err = msgpack.Unmarshal(slice.Data(), &value)
					So(err, ShouldBeNil)
					v := value["data"].(int64)
					So(v, ShouldEqual, i)
					i++
				}
			})

			Convey("Delete Should Run", func() {
				key := []byte("data-5")
				err := db.Delete(key)
				So(err, ShouldBeNil)

				slice, err := db.Get(key)
				So(err, ShouldBeNil)
				So(slice.Size(), ShouldEqual, 0)
			})

		})

		Convey("DB should be destroyd", func() {
			err = db.Destroy()
			So(err, ShouldBeNil)
		})

	})
}

func TestCounterDB(t *testing.T) {
	dbpath := path.Join(os.TempDir(), "nekorocks_counter")
	opts := gorocksdb.NewDefaultOptions()
	opts.SetMergeOperator(new(int64AddOperator))
	opts.SetCreateIfMissing(true)
	opts.SetMaxSuccessiveMerges(10)

	Convey("Subject: Counter DB Test", t, func() {
		db, err := NewRocksDB(dbpath, opts)
		Convey("Error should be nil", func() {
			So(err, ShouldBeNil)
		})

		Convey("Counter should be atomic", func() {
			key := []byte("count")
			step := make([]byte, 8)
			binary.BigEndian.PutUint64(step, 5)

			for i := 0; i < 3; i++ {
				go func() { db.Merge(key, step) }()
			}

			time.Sleep(40 * time.Millisecond)

			slice, err := db.Get(key)
			So(err, ShouldBeNil)
			value := binary.BigEndian.Uint64(slice.Data())
			So(value, ShouldEqual, 15)
		})

		Convey("DB should be destroyd", func() {
			err = db.Destroy()
			So(err, ShouldBeNil)
		})
	})

}
