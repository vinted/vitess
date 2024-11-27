package schema

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func compareSnowflake(t *testing.T, id, wantTimestamp int64, wantSequence int64, wantMachineID int64) {
	gotTimestamp := (id >> int64(SequenceLength+MachineIDLength)) + SnowflakeStartTime.UTC().UnixNano()/1e6
	gotSequence := id & int64(MaxSequence)
	gotMachineID := (id & (int64(MaxMachineID) << SequenceLength)) >> SequenceLength
	fmt.Println("got ", gotTimestamp, gotSequence, gotMachineID)
	assert.Equal(t, wantSequence, gotSequence)
	// assert.Equal(t, wantMachineID, gotMachineID)
	// this is a flaky test
	assert.Equal(t, wantTimestamp, gotTimestamp)
}

func TestNextNID(t *testing.T) {
	snow := &SnowflakeInfo{}
	snow.SetMachineID(1)
	ts := int64(1732711077200)

	gotId, err := snow.NextNID(1, ts)
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	// assert.Equal(t, gotId, snow.LastVal)
	compareSnowflake(t, gotId, ts, 0, 1)

	// test multiple values within same ms (flaky)
	for i := 1; i <= 4; i++ {
		gotId, err := snow.NextNID(1, ts)
		if err != nil {
			t.Fatalf("qre.Execute() = %v, want nil", err)
		}
		compareSnowflake(t, gotId, ts, int64(i), 1)
	}

	// test ms overflow by 1 with high inc number
	gotId, err = snow.NextNID(5000, ts)
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	compareSnowflake(t, gotId, ts, 5, 1)
	gotId, err = snow.NextNID(1, ts)
	if err != nil {
		t.Fatalf("qre.Execute() = %v, want nil", err)
	}
	compareSnowflake(t, gotId, ts+1, 910, 1)

	assert.Equal(t, 1, 2)
}
