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
    "errors"
    "time"
    "encoding/binary"
)

const (
    OP_FIND uint8 = iota
    OP_INSERT
    OP_DELETE

    OP_NEW_SERIES
    OP_DELETE_SERIES
    OP_SERIES_INFO

    OP_PING
    OP_PONG
)

const (
    STATE_INIT int = iota
    STATE_READY
    STATE_RECOVERING
    STATE_SYNCING
)

const (
    PEER_FLG_KEEP int = iota
    PEER_FLG_UPDATE
    PEER_FLG_NEW
    PEER_FLG_RESET
)

type BytePacket interface {
    ToBytes() []byte
}

var InvalidPacket = errors.New("Invalid Packet")

type NekodMsgHeader struct {
    Opcode uint8
}

type NekoStrPack struct {
    Len uint16
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
    Ts  time.Time
    Value []byte
}

func (r *NekodRecord) ToBytes() []byte {
    buf := bytes.NewBuffer(make([]byte, 0, 32))
    buf.Write(Time2Bytes(r.Ts))
    buf.Write(r.Value)
    return buf.Bytes()
}

func (r *NekodRecord) FromBytes(buf *bytes.Buffer) (err error) {
    if buf.Len() <= 15 {
        return InvalidPacket
    }
    lv := buf.Len() - 15
    tsB := make([]byte, 15)
    r.Value = make([]byte, lv)
    buf.Read(tsB)
    buf.Read(r.Value)
    r.Ts, err = Bytes2Time(tsB)

    return err
}

type NekodSeriesInfo struct {
    Name NekoStrPack
    Count uint64
}


type NekodPeerInfo struct {
    Name string `json:"name"`
    RealName string `json:"real_name"`
    Hostname string `json:"hostname"`
    Port int `json:"port"`
    State int `json:"state"`
    Flag  int `json:"flag"`
}


type NekoSeriesInfo struct {
    // series name, used for query
    Name string   `json:"name"`
    // unmutable unique id, used for storage
    Id string     `json:"id"`
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
