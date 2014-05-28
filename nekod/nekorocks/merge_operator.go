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
    "encoding/binary"
//     gorocksdb "github.com/tecbot/gorocksdb"
)

type uint64AddOperator struct {}

func (u *uint64AddOperator) FullMerge(key, existingValue []byte,
                                      operands [][]byte) ([]byte, bool) {
    count := uint64(0)
    value := []byte{0, 0, 0, 0, 0, 0, 0, 0}
    if len(existingValue) != 0 {
        if len(existingValue) != 8 {
            return []byte{}, false
        }
        count = binary.BigEndian.Uint64(existingValue)
    }
    for _, o := range operands {
        count += binary.BigEndian.Uint64(o)
    }
    binary.BigEndian.PutUint64(value, count)
    return value, true
}

func (u *uint64AddOperator) PartialMerge(
    key, leftOperand, rightOperand []byte) ([]byte, bool) {

    if len(leftOperand) == 8 && len(rightOperand) == 8 {
        count := binary.BigEndian.Uint64(leftOperand) +
                    binary.BigEndian.Uint64(rightOperand)
        value := []byte{0, 0, 0, 0, 0, 0, 0, 0}
        binary.BigEndian.PutUint64(value, count)
        return value, true
    }
    return []byte{}, false
}

func (u *uint64AddOperator) Name() string {
    return "Uint64AddOperator"
}
