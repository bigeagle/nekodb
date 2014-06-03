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
	"encoding/binary"
)

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

type ReqSeriesMetaHdr struct {
	SeriesName string
}

func (r *ReqSeriesMetaHdr) ToBytes() []byte {
	buf := bytes.NewBuffer(make([]byte, 0, 16))
	sn := NekoString(r.SeriesName)
	buf.Write(sn.ToBytes())
	return buf.Bytes()
}

func (r *ReqSeriesMetaHdr) FromBytes(buf *bytes.Buffer) error {
	sn := new(NekoStrPack)
	if err := sn.FromBytes(buf); err == nil {
		r.SeriesName = sn.String()
	} else {
		return err
	}
	return nil
}
