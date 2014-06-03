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

package nekolib

import (
	"bytes"
	"time"
)

func Time2Bytes(t time.Time) []byte {
	b, _ := t.MarshalBinary()
	return b
}

func Bytes2Time(b []byte) (time.Time, error) {
	t := new(time.Time)
	err := t.UnmarshalBinary(b)
	return *t, err
}

func Bytes2TimeSec(b []byte) int64 {
	tb := b[1:9]
	ts := int64(tb[7]) | int64(tb[6])<<8 | int64(tb[5])<<16 |
		int64(tb[4])<<24 | int64(tb[3])<<32 | int64(tb[2])<<40 |
		int64(tb[1])<<48 | int64(tb[0])<<56
	return ts
}

func TimeSec2Bytes(ts int64) []byte {
	return []byte{
		byte(ts >> 56),
		byte(ts >> 48),
		byte(ts >> 40),
		byte(ts >> 32),
		byte(ts >> 24),
		byte(ts >> 16),
		byte(ts >> 8),
		byte(ts),
	}
}

func TsBoundary(ts int64, frag_level int) (lower, upper int64) {
	if ts > 0 {
		step := uint64(1 << uint8(frag_level))
		mask := uint64(0xFFFFFFFFFFFFFFFF) - (step - 1)
		lower = int64(uint64(ts) & mask)
		upper = lower + int64(step)
	} else {
		step := int64(1 << uint8(frag_level))
		mask := step - 1
		upper = ts | mask + 1
		lower = upper - step
	}

	return lower, upper
}

func TimeBoundary(tb []byte, frag_level int) (lower, upper []byte) {
	lower = make([]byte, 15)
	upper = make([]byte, 15)
	copy(lower, tb)
	copy(upper, tb)

	ts := Bytes2TimeSec(tb)
	l, u := TsBoundary(ts, frag_level)

	lbs := TimeSec2Bytes(l)
	ubs := TimeSec2Bytes(u)

	copy(lower[1:9], lbs)
	copy(lower[9:13], []byte{0, 0, 0, 0})
	copy(upper[1:9], ubs)
	copy(upper[9:13], []byte{0, 0, 0, 0})

	return lower, upper
}

func MakeResponse(code uint8, msg interface{}) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, 16))
	buf.WriteByte(byte(code))
	switch v := msg.(type) {
	case string:
		buf.Write([]byte(v))
	case []byte:
		buf.Write(v)
	case byte:
		buf.WriteByte(v)
	}
	return buf.Bytes()
}
