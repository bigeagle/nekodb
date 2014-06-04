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
	"net/http"
	"time"

	"github.com/bigeagle/nekodb/nekolib"
	"github.com/codegangsta/martini-contrib/render"
	"github.com/go-martini/martini"
)

func serveHTTP(addr string, port int) {
	m := martini.Classic()

	m.Get("/", func() string {
		return "Hello World"
	})

	m.Get("/series/", func(r render.Render) {
		s := getServer()
		coll := make([]*nekolib.NekoSeriesInfo, 0)
		for _, series := range s.collection.coll {
			coll = append(coll, series)
		}
		r.JSON(200, map[string]interface{}{
			"series": coll,
		})
	})

	m.Get("/peers/", func(r render.Render) {
		s := getServer()
		peers := make([]map[string]interface{}, 0)
		s.backends.ForEachSafe(func(n *nekoRingNode) {
			peers = append(peers, map[string]interface{}{
				"name":       n.Name,
				"real_name":  n.RealName,
				"hostname":   n.Hostname,
				"port":       n.Port,
				"state":      n.State,
				"hash_value": n.Key,
			})
		})
		r.JSON(200, map[string]interface{}{
			"peers": peers,
		})
	})

	m.Get("/series/:name/meta", func(params martini.Params, r render.Render) {
		s := getServer()
		_, found := s.collection.getSeries(params["name"])
		if !found {
			r.JSON(404, map[string]interface{}{"msg": "Series Not Found"})
			return
		}

		sm, err := getSeriesMeta(params["name"])
		if err != nil {
			r.JSON(500, map[string]interface{}{"msg": err.Error()})
			return
		}
		r.JSON(200, *sm)
	})

	m.Get("/series/:name", func(req *http.Request, params martini.Params, r render.Render) {
		s := getServer()
		series, found := s.collection.getSeries(params["name"])
		if !found {
			r.JSON(404, map[string]interface{}{"msg": "Series Not Found"})
			return
		}

		layout := "2006-01-02"
		start, err := time.Parse(layout, req.FormValue("start"))
		if err != nil {
			r.JSON(400, map[string]interface{}{"msg": err.Error()})
			return
		}
		end, err := time.Parse(layout, req.FormValue("end"))
		if err != nil {
			r.JSON(400, map[string]interface{}{"msg": err.Error()})
			return
		}

		reqHdr := &nekolib.ReqFindByRangeHdr{
			SeriesName: params["name"],
			StartTs:    nekolib.Time2Bytes(start),
			EndTs:      nekolib.Time2Bytes(end),
			Priority:   0,
		}

		bench_start := time.Now()
		bench_peers := map[string](map[string]int){}
		bench := map[string]interface{}{
			"total_time":  0,
			"bench_peers": bench_peers,
		}

		recordChan := make(chan nekolib.SCNode, 1024)
		msgChan := make(chan map[string]interface{}, 256)
		done := make(chan struct{})
		records := make([]interface{}, 0, 1024)
		go func() {
			for record := range recordChan {
				r := record.(*nekolib.NekodRecord)
				t, _ := nekolib.Bytes2Time(r.Ts)
				records = append(records,
					[]interface{}{
						t.UnixNano() / 1000000, string(r.Value)})
			}
			bench["total_time"] = time.Since(bench_start).Nanoseconds()
			close(msgChan)
		}()
		getRangeToChan(reqHdr, recordChan, msgChan)

		go func() {
			for r := range msgChan {
				peer := r["peer"].(string)
				if _, found := bench_peers[peer]; found {
					bench_peers[peer]["count"] += int(r["count"].(float64))
					bench_peers[peer]["duration"] += int(r["duration"].(float64))
					bench_peers[peer]["full_duration"] += int(r["full_duration"].(float64))
				} else {
					bench_peers[peer] = map[string]int{
						"count":         int(r["count"].(float64)),
						"duration":      int(r["duration"].(float64)),
						"full_duration": int(r["full_duration"].(int64)),
					}
				}
			}
			close(done)
		}()

		<-done
		logger.Debug("%v", bench)

		r.JSON(200, map[string]interface{}{
			"data":      records,
			"label":     series.Name,
			"benchmark": bench,
		})
	})

	m.Handlers(
		render.Renderer(),
	)
	m.Use(func(res http.ResponseWriter, req *http.Request) {
		res.Header().Set("Access-Control-Allow-Origin", "*")
	})

	logger.Info("Serving REST API at %s:%d", addr, port)
	http.ListenAndServe(fmt.Sprintf("%s:%d", addr, port), m)
}
