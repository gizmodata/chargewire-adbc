// Licensed under the Apache License, Version 2.0.

// Package warpsql is an ADBC driver for the Warp SQL protocol: SQL over plain
// HTTP, with results, parameters and uploads as Arrow IPC streams.
//
// It is generic: it talks to any server that implements the Warp SQL spec.
//
//	drv := warpsql.NewDriver(memory.DefaultAllocator)
//	db, err := drv.NewDatabaseWithContext(ctx, map[string]string{
//		"uri":      "warp://db.example.com:31337",
//		"username": "joe",
//		"password": "…",
//	})
//	cnxn, err := db.Open(ctx)
package warpsql

import (
	"context"
	"runtime/debug"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// driverName prefixes error messages ("[warpsql] …", the same prefix the C
// library uses) and names the driver in telemetry.
const driverName = "warpsql"

// driverDisplayName is what GetInfo reports as the driver name.
const driverDisplayName = "ADBC Warp SQL Driver - Go"

type driverImpl struct {
	driverbase.DriverImplBase
}

// NewDriver returns the Warp SQL ADBC driver.
func NewDriver(alloc memory.Allocator) driverbase.Driver {
	return NewDriverWithName(alloc, driverDisplayName)
}

// NewDriverWithName returns the Warp SQL ADBC driver reporting displayName as
// its driver name in GetInfo, for a driver built on this one (for example
// "ADBC Hardcharger Driver - Go"). Nothing else changes.
func NewDriverWithName(alloc memory.Allocator, displayName string) driverbase.Driver {
	info := driverbase.DefaultDriverInfo(driverName)
	info.MustRegister(map[adbc.InfoCode]any{
		adbc.InfoDriverName:      displayName,
		adbc.InfoVendorSql:       true,
		adbc.InfoVendorSubstrait: false,
	})
	return driverbase.NewDriver(&driverImpl{DriverImplBase: driverbase.NewDriverImplBase(info, alloc)})
}

// NewDatabaseWithContext validates opts; nothing is sent to the server until
// the first connection opens.
func (d *driverImpl) NewDatabaseWithContext(ctx context.Context, opts map[string]string) (adbc.DatabaseWithContext, error) {
	db, err := newDatabase(ctx, &d.DriverImplBase, opts)
	if err != nil {
		return nil, err
	}
	return driverbase.NewDatabase(db), nil
}

// cloneDriverInfo copies the driver's info for one database, whose vendor
// codes are then filled in from its server.
func cloneDriverInfo(src *driverbase.DriverInfo) *driverbase.DriverInfo {
	info := driverbase.DefaultDriverInfo(src.GetName())
	for _, code := range src.InfoSupportedCodes() {
		if value, ok := src.GetInfoForInfoCode(code); ok {
			info.MustRegister(map[adbc.InfoCode]any{code: value})
		}
	}
	return info
}

// userAgent identifies the driver to servers, with the module version when
// the binary records it.
func userAgent() string {
	version := driverbase.DriverVersion()
	if version == "" {
		if build, ok := debug.ReadBuildInfo(); ok {
			for _, dep := range build.Deps {
				if dep.Path == "github.com/gizmodata/warp-sql-adbc/go" {
					version = dep.Version
				}
			}
		}
	}
	if version == "" {
		version = "dev"
	}
	return "warp-sql-adbc/" + version + " (Go)"
}
