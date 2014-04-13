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
//    "fmt"
    "testing"
    "time"
)

func TestMemoryBackend(t *testing.T) {

    options := NekoBackendOptions{BlkSize: 0}
    id := "testSeries"
    var backend NekoBackend = newMemoryBackend()
    backend.NewSeriesFrag(id, options)

    series := backend.GetSeriesFrag(id)
    if series == nil {
        t.Error("None Series found")
    }

    const shortForm = "2006-Jan-02"
    start, _ := time.Parse(shortForm, "2013-Jan-01")
    ts, value := start, 0
    for i := 0; i < 10; i++ {
        r := NekoRecord{ts, value}
        ts = ts.Add(time.Hour)
        value += 1
        series.Insert([]NekoRecord{r})
    }
    if series.Count() != 10 {
        t.Error("Series Record Count Error");
    }

    out := make(chan NekoRecord)
    go func() {
        s, e := start, start.Add(4*time.Hour)
        series.GetByRange(s, e, (chan<- NekoRecord)(out))
    }()

    records := make([]NekoRecord, 0)
    for r := range out {
        records = append(records, r)
    }

    if len(records) != 4 {
        t.Error("Error Getting Records")
    }

}
