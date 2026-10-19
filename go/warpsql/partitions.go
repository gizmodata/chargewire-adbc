// Licensed under the Apache License, Version 2.0.

package warpsql

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/gizmodata/warp-sql-adbc/go/internal/protocol"
)

// partitionDescriptor is what ExecutePartitions hands out for one partition
// and ReadPartition reads back: enough to fetch it from its server.
type partitionDescriptor struct {
	Version     int     `json:"v"`
	SessionID   string  `json:"session_id"`
	StatementID string  `json:"statement_id"`
	Partition   int     `json:"partition"`
	Location    *string `json:"location,omitempty"`
}

const partitionDescriptorVersion = 1

func encodePartition(ref protocol.StatementRef, partition protocol.Partition) ([]byte, error) {
	return json.Marshal(partitionDescriptor{
		Version: partitionDescriptorVersion, SessionID: ref.SessionID, StatementID: ref.StatementID,
		Partition: partition.Index, Location: partition.Location,
	})
}

// resultParts lists a result's partitions. A partition held by another server
// is fetched from its location (§6.2).
func resultParts(result *protocol.Result) ([]resultPart, error) {
	parts := make([]resultPart, 0, len(result.Partitions))
	for _, partition := range result.Partitions {
		part, err := newResultPart(partition.Index, partition.Location)
		if err != nil {
			return nil, err
		}
		parts = append(parts, part)
	}
	return parts, nil
}

func newResultPart(index int, location *string) (resultPart, error) {
	part := resultPart{index: index}
	if location != nil {
		u, err := url.Parse(*location)
		if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
			return resultPart{}, fmt.Errorf("partition %d has an invalid location %q", index, *location)
		}
		part.location = u
	}
	return part, nil
}

// ReadPartition reads one partition that ExecutePartitions described. It
// works from any connection of the same principal while the result is
// retained, and resumes a broken download like any other result.
func (c *connectionImpl) ReadPartition(ctx context.Context, serialized []byte) (array.RecordReader, error) {
	var descriptor partitionDescriptor
	if err := json.Unmarshal(serialized, &descriptor); err != nil || descriptor.Version != partitionDescriptorVersion {
		return nil, c.ErrorHelper.InvalidArgument("not a Warp SQL partition descriptor")
	}
	part, err := newResultPart(descriptor.Partition, descriptor.Location)
	if err != nil {
		return nil, c.ErrorHelper.InvalidArgument("%s", err)
	}
	return newResultReader(resultReaderConfig{
		ctx: ctx, alloc: c.Alloc, session: c.session,
		ref:   protocol.StatementRef{SessionID: descriptor.SessionID, StatementID: descriptor.StatementID},
		parts: []resultPart{part}, codec: ipcCodec(c.defaults.compression, c.discovery), cancelled: func() bool { return false },
	}, nil)
}
