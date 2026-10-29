// Licensed under the Apache License, Version 2.0.

package chargewire

import (
	"context"
	"encoding/json"
	"net/url"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/gizmodata/chargewire-adbc/go/v2/internal/protocol"
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
// is fetched from its location (§6.2), which only a client that asked for
// remote locations may be given. base is the server's own base URL: a
// location must not downgrade its TLS, since the request carries the access
// token.
func resultParts(result *protocol.Result, remoteAllowed bool, base *url.URL) ([]resultPart, error) {
	parts := make([]resultPart, 0, len(result.Partitions))
	for _, partition := range result.Partitions {
		if partition.Location != nil && !remoteAllowed {
			return nil, protocol.ProtocolError("partition %d is on another server (%q), but the driver did not send "+
				"Chargewire-Result-Locations: remote", partition.Index, *partition.Location)
		}
		part, err := newResultPart(partition.Index, partition.Location, base)
		if err != nil {
			return nil, err
		}
		parts = append(parts, part)
	}
	return parts, nil
}

func newResultPart(index int, location *string, base *url.URL) (resultPart, error) {
	part := resultPart{index: index}
	if location != nil {
		u, err := url.Parse(*location)
		if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
			return resultPart{}, protocol.ProtocolError("partition %d has an invalid location %q", index, *location)
		}
		if base != nil && base.Scheme == "https" && u.Scheme != "https" {
			return resultPart{}, protocol.ProtocolError("partition %d is at %q, which is not TLS, though the server is: "+
				"the driver does not send the access token there", index, *location)
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
		return nil, c.ErrorHelper.InvalidArgument("not a Chargewire partition descriptor")
	}
	part, err := newResultPart(descriptor.Partition, descriptor.Location, c.client.BaseURL())
	if err != nil {
		return nil, c.ErrorHelper.InvalidArgument("%s", err)
	}
	return newResultReader(resultReaderConfig{
		ctx: ctx, alloc: c.Alloc, session: c.session,
		ref:   protocol.StatementRef{SessionID: descriptor.SessionID, StatementID: descriptor.StatementID},
		parts: []resultPart{part}, codec: ipcCodec(c.defaults.compression, c.discovery), cancelled: func() bool { return false },
	}, nil)
}
