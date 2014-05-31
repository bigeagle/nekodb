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

const (
	ETCD_DIR              = "/nekodb"
	ETCD_PEER_DIR         = ETCD_DIR + "/peers"
	ETCD_COLLECTION_DIR   = ETCD_DIR + "/collections"
	ETCD_SERIES_DIR       = ETCD_DIR + "/series"
	ETCD_REFRESH_INTERVAL = 64

	SLICE_FRAG_LEVEL_DEFAULT = 14
	ISO8601                  = "2006-01-02T15:04:05.999Z0700"
)

const (
	OP_FIND_RANGE uint8 = iota
	OP_INSERT
	OP_INSERT_BATCH
	OP_DELETE

	OP_NEW_SERIES
	OP_IMPORT_SERIES
	OP_DELETE_SERIES
	OP_SERIES_INFO

	OP_PING
	OP_PONG

	REP_OK
	REP_ERR
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
