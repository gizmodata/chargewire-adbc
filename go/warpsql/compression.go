// Licensed under the Apache License, Version 2.0.

package warpsql

import (
	"time"

	"github.com/gizmodata/warp-sql-adbc/go/internal/protocol"
)

// Values of OptionIPCCompression; the codecs are named as in §8.2.
const (
	codecAuto     = "auto"
	codecNone     = "none"
	codecLZ4Frame = "lz4_frame"
	codecZstd     = "zstd"
)

// farLink is the round trip from which "auto" compresses with zstd: a link
// that slow is a WAN, where bandwidth, not CPU, limits a transfer.
const farLink = 10 * time.Millisecond

// ipcCodec is the codec a statement compresses with and asks results in, or
// "" for none: the option's codec when the server offers it, and for "auto"
// the one that suits the link.
func ipcCodec(setting string, discovery *protocol.Discovery) string {
	if setting == codecAuto {
		setting = autoCodec(discovery.RTT)
	}
	switch {
	case setting == codecLZ4Frame && discovery.Has(protocol.CapabilityIPCCompressionLZ4Frame):
		return codecLZ4Frame
	case setting == codecZstd && discovery.Has(protocol.CapabilityIPCCompressionZstd):
		return codecZstd
	}
	return ""
}

// ingestParts is the most parts an ingest is sent in: the option's number, or
// for "auto" more on a far link, where each connection carries less.
func ingestParts(setting int, discovery *protocol.Discovery) int {
	if setting != autoIngestParts {
		return setting
	}
	if discovery.RTT >= farLink {
		return farIngestParts
	}
	return nearIngestParts
}

// autoCodec is the codec "auto" uses on a link with this round trip.
func autoCodec(rtt time.Duration) string {
	if rtt >= farLink {
		return codecZstd
	}
	return codecLZ4Frame
}
