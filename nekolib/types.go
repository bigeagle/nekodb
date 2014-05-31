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
	// "fmt"
	//  "github.com/bigeagle/nekodb/nekod/backend"
	"bytes"
	"encoding/binary"
	"errors"
)

type BytePacket interface {
	ToBytes() []byte
}

var InvalidPacket = errors.New("Invalid Packet")
var EndOfStream = errors.New("End Of Stream")

type NekodMsgHeader struct {
	Opcode uint8
}

type NekoReplyHeader struct {
	RepCode uint8
}

type NekoStrPack struct {
	Len   uint16
	Bytes []byte
}

func NekoString(s string) *NekoStrPack {
	return &NekoStrPack{uint16(len(s)), []byte(s)}
}

func (ns *NekoStrPack) ToBytes() []byte {
	buf := bytes.NewBuffer(make([]byte, 0, 16))
	binary.Write(buf, binary.BigEndian, ns.Len)
	buf.Write(ns.Bytes)
	return buf.Bytes()
}

func (ns *NekoStrPack) FromBytes(buf *bytes.Buffer) error {
	binary.Read(buf, binary.BigEndian, &ns.Len)
	ns.Bytes = make([]byte, ns.Len)
	i, _ := buf.Read(ns.Bytes)
	if i != int(ns.Len) {
		return InvalidPacket
	}
	// fmt.Println(ns.Bytes)
	return nil
}

func (ns *NekoStrPack) String() string {
	return string(ns.Bytes)
}

type NekodRecord struct {
	Ts    []byte // 15bytes, Go built-in time binary serialization
	Value []byte
}

func (r *NekodRecord) ToBytes() []byte {
	buf := bytes.NewBuffer(make([]byte, 0, 32))

	binary.Write(buf, binary.BigEndian, uint16(15+len(r.Value)))
	buf.Write(r.Ts)
	buf.Write(r.Value)

	return buf.Bytes()
}

func (r *NekodRecord) FromBytes(buf *bytes.Buffer) (err error) {
	var l uint16
	binary.Read(buf, binary.BigEndian, &l)
	if l == 0 {
		return EndOfStream
	}
	if l <= 15 {
		return InvalidPacket
	}
	lv := l - 15
	r.Ts = make([]byte, 15)
	r.Value = make([]byte, lv)
	buf.Read(r.Ts)
	buf.Read(r.Value)

	return err
}

type NekodSeriesInfo struct {
	Name  NekoStrPack
	Count uint64
}

type NekodPeerInfo struct {
	Name     string `json:"name"`
	RealName string `json:"real_name"`
	Hostname string `json:"hostname"`
	Port     int    `json:"port"`
	State    int    `json:"state"`
	Flag     int    `json:"flag"`
}

type NekoSeriesInfo struct {
	// series name, used for query
	Name string `json:"name"`
	// unmutable unique id, used for storage
	Id string `json:"id"`
	// fragmentation level
	FragLevel int `json:"frag_level"`
}

func (ns *NekoSeriesInfo) ToBytes() []byte {
	buf := bytes.NewBuffer(make([]byte, 0, 16))
	// fmt.Println(NekoString(ns.Name).ToBytes())
	buf.Write(NekoString(ns.Name).ToBytes())
	buf.Write(NekoString(ns.Id).ToBytes())
	binary.Write(buf, binary.BigEndian, uint8(ns.FragLevel))
	return buf.Bytes()
}

func (ns *NekoSeriesInfo) FromBytes(buf *bytes.Buffer) error {
	name := new(NekoStrPack)
	if err := name.FromBytes(buf); err == nil {
		ns.Name = name.String()
	} else {
		return err
	}

	id := new(NekoStrPack)
	if err := id.FromBytes(buf); err == nil {
		ns.Id = id.String()
	} else {
		return err
	}

	fragLevel := uint8(0)
	if err := binary.Read(buf, binary.BigEndian, &fragLevel); err == nil {
		ns.FragLevel = int(fragLevel)
	} else {
		return err
	}
	return nil
}

type ReqImportSeriesHdr struct {
	SeriesName string
}

func (r *ReqImportSeriesHdr) ToBytes() []byte {
	buf := bytes.NewBuffer(make([]byte, 0, 16))
	sn := NekoString(r.SeriesName)
	buf.Write(sn.ToBytes())
	return buf.Bytes()
}

func (r *ReqImportSeriesHdr) FromBytes(buf *bytes.Buffer) error {
	sn := new(NekoStrPack)
	if err := sn.FromBytes(buf); err == nil {
		r.SeriesName = sn.String()
	} else {
		return err
	}
	return nil
}

type ReqInsertBlockHdr struct {
	SeriesName string
	HashValue  uint32
	StartTs    []byte
	EndTs      []byte
	Priority   uint8
	Count      uint16
}

func (r *ReqInsertBlockHdr) ToBytes() []byte {
	buf := bytes.NewBuffer(make([]byte, 0, 32))
	sn := NekoString(r.SeriesName)
	buf.Write(sn.ToBytes())
	binary.Write(buf, binary.BigEndian, r.HashValue)
	// if len(r.StartTs) != 15 {
	// 	return []byte{}, InvalidPacket
	// }
	// if len(r.EndTs) != 15 {
	// 	return []byte{}, InvalidPacket
	// }
	buf.Write(r.StartTs)
	buf.Write(r.EndTs)
	binary.Write(buf, binary.BigEndian, r.Priority)
	binary.Write(buf, binary.BigEndian, r.Count)
	return buf.Bytes()
}

func (r *ReqInsertBlockHdr) FromBytes(buf *bytes.Buffer) error {
	sn := new(NekoStrPack)
	if err := sn.FromBytes(buf); err == nil {
		r.SeriesName = sn.String()
	} else {
		return err
	}

	binary.Read(buf, binary.BigEndian, &r.HashValue)

	r.StartTs = make([]byte, 15)
	l, err := buf.Read(r.StartTs)
	if l != 15 {
		return InvalidPacket
	}
	if err != nil {
		return err
	}

	r.EndTs = make([]byte, 15)
	l, err = buf.Read(r.EndTs)
	if l != 15 {
		return InvalidPacket
	}
	if err != nil {
		return err
	}

	binary.Read(buf, binary.BigEndian, &r.Priority)
	binary.Read(buf, binary.BigEndian, &r.Count)

	return nil
}

type ReqFindByRangeHdr struct {
	SeriesName string
	StartTs    []byte
	EndTs      []byte
	Priority   uint8
}

func (r *ReqFindByRangeHdr) ToBytes() []byte {
	buf := bytes.NewBuffer(make([]byte, 0, 32))
	sn := NekoString(r.SeriesName)
	buf.Write(sn.ToBytes())
	buf.Write(r.StartTs)
	buf.Write(r.EndTs)
	binary.Write(buf, binary.BigEndian, r.Priority)
	return buf.Bytes()
}

func (r *ReqFindByRangeHdr) FromBytes(buf *bytes.Buffer) error {
	sn := new(NekoStrPack)
	if err := sn.FromBytes(buf); err == nil {
		r.SeriesName = sn.String()
	} else {
		return err
	}

	r.StartTs = make([]byte, 15)
	l, err := buf.Read(r.StartTs)
	if l != 15 {
		return InvalidPacket
	}
	if err != nil {
		return err
	}

	r.EndTs = make([]byte, 15)
	l, err = buf.Read(r.EndTs)
	if l != 15 {
		return InvalidPacket
	}
	if err != nil {
		return err
	}

	binary.Read(buf, binary.BigEndian, &r.Priority)
	return nil
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
