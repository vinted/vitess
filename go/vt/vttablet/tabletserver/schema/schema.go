/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package schema

import (
	"fmt"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/sqlparser"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// Table types
const (
	NoType = iota
	Sequence
	Snowflake
	Message
)

// TypeNames allows to fetch a the type name for a table.
// Count must match the number of table types.
var TypeNames = []string{
	"none",
	"sequence",
	"snowflake",
	"message",
}

// Table contains info about a table.
type Table struct {
	Name      sqlparser.TableIdent
	Fields    []*querypb.Field
	PKColumns []int
	Type      int

	// SequenceInfo contains info for sequence tables.
	SequenceInfo *SequenceInfo

	// SnowflakeInfo contains info for snowflake tables.
	SnowflakeInfo *SnowflakeInfo

	// MessageInfo contains info for message tables.
	MessageInfo *MessageInfo

	CreateTime    int64
	FileSize      uint64
	AllocatedSize uint64
}

// SequenceInfo contains info specific to sequence tabels.
// It must be locked before accessing the values inside.
// If CurVal==LastVal, we have to cache new values.
// When the schema is first loaded, the values are all 0,
// which will trigger caching on first use.
type SequenceInfo struct {
	sync.Mutex
	NextVal int64
	LastVal int64
}

// These constants are the bit lengths of snowflake ID parts.
const (
	TimestampLength uint8 = 41
	MachineIDLength uint8 = 10
	SequenceLength  uint8 = 12
	MaxSequence     int64 = 1<<SequenceLength - 1
	MaxTimestamp    int64 = 1<<TimestampLength - 1
	MaxMachineID    int64 = 1<<MachineIDLength - 1

	machineIDMoveLength = SequenceLength
	timestampMoveLength = MachineIDLength + SequenceLength
)

var (
	// default starttime
	SnowflakeStartTime = time.Date(2008, 11, 10, 23, 0, 0, 0, time.UTC)
)

// SnowflakeInfo contains info specific to sequence tabels.
// It must be locked before accessing the values inside.
// If CurVal==LastVal, we have to cache new values.
// When the schema is first loaded, the values are all 0,
// which will trigger caching on first use.
type SnowflakeInfo struct {
	sync.Mutex
	// Snowflake     *sonyflake.Sonyflake
	MachineID     int64
	Sequence      int64
	LastTimestamp int64
	// NextVal   int64
	LastVal int64
}

func elapsedTime(noms int64, s time.Time) int64 {
	return noms - s.UTC().UnixNano()/1e6
}

func (s *SnowflakeInfo) NextNID(inc int64, currentTimestamp int64) (int64, error) {
	fmt.Println("----------")
	// need to pass timestamo in order to make it more testable
	// currentTimestamp := currentMillis()
	var firstSequence, firstTimestamp int64
	if s.LastTimestamp < currentTimestamp {
		firstTimestamp = currentTimestamp
		firstSequence = 0
		s.LastTimestamp = currentTimestamp
		s.Sequence = 0
	} else {
		if s.LastTimestamp > currentTimestamp {
			fmt.Println("current timestamp is less than last timestamp, so we are overflowing again")
			currentTimestamp = s.LastTimestamp
		} else {
			fmt.Println("Same timestamp", currentTimestamp)
		}
		// calculate first id values
		firstInc := s.Sequence + 1
		firstTimestamp = currentTimestamp + firstInc/MaxSequence // add overflow to timestamp as ms
		firstSequence = firstInc % MaxSequence                   // set first sequence
		// calculate last id values
		lastInc := s.Sequence + inc
		s.LastTimestamp = currentTimestamp + lastInc/MaxSequence // add overflow to timestamp as ms
		s.Sequence = lastInc % MaxSequence                       // set last sequence
	}
	fmt.Println("firstSequence", firstSequence, "firstTimestamp", firstTimestamp)
	fmt.Println("lastSequence", s.Sequence, "lastTimestamp", s.LastTimestamp)

	firstDF := elapsedTime(firstTimestamp, SnowflakeStartTime)
	firstId := (uint64(firstDF) << uint64(timestampMoveLength)) | (uint64(s.MachineID) << uint64(machineIDMoveLength)) | uint64(firstSequence)
	return int64(firstId), nil
}

// SetMachineID specify the machine ID. It will panic when machined > max limit for 2^10-1.
// This function is thread-unsafe, recommended you call him in the main function.
func (s *SnowflakeInfo) SetMachineID(m int64) error {
	if m > MaxMachineID {
		return fmt.Errorf("the machineID cannot be greater than 1023: %d", m)
	}
	s.MachineID = m
	return nil
}

// // ParseID parse snowflake it to SID struct.
// func ParseSnowflakeID(id uint64) SnowflakeID {
// 	t := id >> uint64(SequenceLength+MachineIDLength)
// 	sequence := id & uint64(MaxSequence)
// 	mID := (id & (uint64(MaxMachineID) << SequenceLength)) >> SequenceLength

// 	return SnowflakeID{
// 		ID:        id,
// 		Sequence:  sequence,
// 		MachineID: mID,
// 		Timestamp: t,
// 	}
// }

// MessageInfo contains info specific to message tables.
type MessageInfo struct {
	// Fields stores the field info to be
	// returned for subscribers.
	Fields []*querypb.Field

	// AckWaitDuration specifies how long to wait after
	// the message was first sent. The back-off doubles
	// every attempt.
	AckWaitDuration time.Duration

	// PurgeAfterDuration specifies the time after which
	// a successfully acked message can be deleted.
	PurgeAfterDuration time.Duration

	// BatchSize specifies the max number of events to
	// send per response.
	BatchSize int

	// CacheSize specifies the number of messages to keep
	// in cache. Anything that cannot fit in the cache
	// is sent as best effort.
	CacheSize int

	// PollInterval specifies the polling frequency to
	// look for messages to be sent.
	PollInterval time.Duration

	// MinBackoff specifies the shortest duration message manager
	// should wait before rescheduling a message
	MinBackoff time.Duration

	// MaxBackoff specifies the longest duration message manager
	// should wait before rescheduling a message
	MaxBackoff time.Duration
}

// NewTable creates a new Table.
func NewTable(name string) *Table {
	return &Table{
		Name: sqlparser.NewTableIdent(name),
	}
}

// FindColumn finds a column in the table. It returns the index if found.
// Otherwise, it returns -1.
func (ta *Table) FindColumn(name sqlparser.ColIdent) int {
	for i, col := range ta.Fields {
		if name.EqualString(col.Name) {
			return i
		}
	}
	return -1
}

// GetPKColumn returns the pk column specified by the index.
func (ta *Table) GetPKColumn(index int) *querypb.Field {
	return ta.Fields[ta.PKColumns[index]]
}

// HasPrimary returns true if the table has a primary key.
func (ta *Table) HasPrimary() bool {
	return len(ta.PKColumns) != 0
}
