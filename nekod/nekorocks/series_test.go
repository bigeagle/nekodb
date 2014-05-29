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
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/bigeagle/nekodb/nekolib"
	. "github.com/smartystreets/goconvey/convey"
)

// "github.com/vmihailenco/msgpack"

func TestSeriesOperations(t *testing.T) {
	dbpath := path.Join(os.TempDir(), "nekodb")
	if _, err := os.Stat(dbpath); os.IsNotExist(err) {
		os.MkdirAll(dbpath, os.ModeDir|os.FileMode(0755))
	}
	series_name := "test"
	series_id := "alsir12"
	frag_level := 12
	InitNekoRocks(dbpath, nil)

	Convey("Subject: Test Series Operations", t, func() {
		series, err := NewSeries(series_name, series_id, frag_level)
		Convey("Error Should Be Nil", func() {
			So(err, ShouldBeNil)
		})

		Convey("Data Should Be Inserted", func() {
			key := nekolib.Time2Bytes(time.Now())
			value := []byte("Hello")
			err := series.Insert(key, value, 0)
			So(err, ShouldBeNil)

			time.Sleep(100 * time.Millisecond)
			key = nekolib.Time2Bytes(time.Now())
			value = []byte("World")

			err = series.Insert(key, value, 0)
			So(err, ShouldBeNil)
		})

		Convey("Series Elements Count Should be 2", func() {
			count, err := series.Count()
			So(err, ShouldBeNil)
			So(count, ShouldEqual, 2)
		})

		Convey("Iteration Should Get Hello World", func() {
			iter := series.data.NewIterator()
			defer iter.Close()
			words := make([]string, 0)
			for iter.SeekToFirst(); iter.Valid(); iter.Next() {
				slice := iter.Value()
				v := slice.Data()
				So(len(v), ShouldNotEqual, 0)
				words = append(words, string(v))
			}
			So(strings.Join(words, " "), ShouldEqual, "Hello World")

		})

		Convey("Batch Job Should Run", func() {
			records := make([]Record, 0)

			key := nekolib.Time2Bytes(time.Now())
			value := []byte("Hello")
			r := Record{key, value}
			records = append(records, r)

			time.Sleep(100 * time.Millisecond)
			key = nekolib.Time2Bytes(time.Now())
			value = []byte("World")
			r = Record{key, value}
			records = append(records, r)

			err := series.InsertBatch(records, 0)
			So(err, ShouldBeNil)

			count, err := series.Count()
			So(err, ShouldBeNil)
			So(count, ShouldEqual, 4)
		})

		Convey("Series should be destroyed", func() {
			err = series.Destroy()
			So(err, ShouldBeNil)
		})
	})
}
