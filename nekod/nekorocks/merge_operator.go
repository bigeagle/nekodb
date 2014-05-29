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

// Merge Operator Provides Atomic Updates
package nekorocks

import (
	"bytes"
	"encoding/binary"

	//     gorocksdb "github.com/tecbot/gorocksdb"
)

type int64AddOperator struct{}

func (u *int64AddOperator) FullMerge(key, existingValue []byte,
	operands [][]byte) ([]byte, bool) {
	count := int64(0)
	if len(existingValue) != 0 {
		if len(existingValue) != 8 {
			return []byte{}, false
		}
		binary.Read(bytes.NewReader(existingValue), binary.BigEndian, &count)
	}
	for _, o := range operands {
		var v int64
		binary.Read(bytes.NewReader(o), binary.BigEndian, &v)
		count += v
	}
	buf := bytes.NewBuffer(make([]byte, 0, 8))
	binary.Write(buf, binary.BigEndian, count)
	return buf.Bytes(), true
}

func (u *int64AddOperator) PartialMerge(
	key, leftOperand, rightOperand []byte) ([]byte, bool) {

	if len(leftOperand) == 8 && len(rightOperand) == 8 {
		var l, r int64
		binary.Read(bytes.NewReader(leftOperand), binary.BigEndian, &l)
		binary.Read(bytes.NewReader(rightOperand), binary.BigEndian, &r)
		count := l + r
		value := []byte{0, 0, 0, 0, 0, 0, 0, 0}
		buf := bytes.NewBuffer(value)
		binary.Write(buf, binary.BigEndian, count)
		return value, true
	}
	return []byte{}, false
}

func (u *int64AddOperator) Name() string {
	return "Uint64AddOperator"
}
