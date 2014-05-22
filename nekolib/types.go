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

// import (
//     "github.com/bigeagle/nekodb/nekod/backend"
// )

const (
    OP_FIND uint8 = iota
    OP_INSERT
    OP_DELETE

    OP_NEW_SERIES
    OP_DELETE_SERIES
    OP_SERIES_INFO
)

const (
    STATE_INIT int = iota
    STATE_READY
    STATE_RECOVERING
    STATE_SYNCING
)

type NekodMsgHeader struct {
    Opcode uint8
}

type NekoStrPack struct {
    Len uint16
    Bytes []byte
}

type NekodRecord struct {
    Tsec uint32
    Tmilli uint16
    DLen uint16
    Value []byte
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
}

