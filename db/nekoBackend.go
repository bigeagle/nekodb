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

import "time"

type NekoBackendOptions struct {
    BlkSize int
}

type NekoBackend interface {
    GetSeriesFrag(id string) NekoSeriesFrag
    NewSeriesFrag(id string, options NekoBackendOptions) error
}


type NekoSeriesFrag interface {
    Id() string
    // GetMatch(RecordFilter) []NekoRecord;
    GetByRange(start, end time.Time, to chan<- NekoRecord)
    Insert(records []NekoRecord) error
    DeleteByRange(start, end time.Time) error
    // DeleteMatch(RecordFilter) error
    Count() uint64
}

