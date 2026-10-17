// Licensed under the Apache License, Version 2.0.

//go:build integration

package warpsql

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// metaConn opens a connection with a small catalog of its own:
// md.s1.{orders, order_lines, orders_v}, md.s2.orderxlines.
func metaConn(t *testing.T) adbc.ConnectionWithContext {
	t.Helper()
	cnxn := openConn(t)
	execUpdate(t, cnxn, "ATTACH IF NOT EXISTS ':memory:' AS md; CREATE SCHEMA IF NOT EXISTS md.s1; CREATE SCHEMA IF NOT EXISTS md.s2; "+
		"CREATE TABLE IF NOT EXISTS md.s1.orders (id INTEGER PRIMARY KEY, total DECIMAL(12, 2) NOT NULL, note VARCHAR); "+
		"CREATE TABLE IF NOT EXISTS md.s1.order_lines (order_id INTEGER REFERENCES md.s1.orders (id), line INTEGER); "+
		"CREATE OR REPLACE VIEW md.s1.orders_v AS SELECT id FROM md.s1.orders; "+
		"CREATE TABLE IF NOT EXISTS md.s2.orderxlines (x INTEGER)")
	return cnxn
}

// getObjects runs GetObjects and returns its rows as driverbase structs.
func getObjects(t *testing.T, cnxn adbc.ConnectionWithContext, depth adbc.ObjectDepth, catalog, schema, table, column *string, types []string) []driverbase.GetObjectsInfo {
	t.Helper()
	reader, err := cnxn.GetObjects(context.Background(), depth, catalog, schema, table, column, types)
	if err != nil {
		t.Fatalf("GetObjects: %v", err)
	}
	defer reader.Release()
	if !reader.Schema().Equal(adbc.GetObjectsSchema) {
		t.Fatalf("GetObjects schema %s", reader.Schema())
	}
	var infos []driverbase.GetObjectsInfo
	for reader.Next() {
		rows := array.RecordToStructArray(reader.RecordBatch())
		defer rows.Release()
		for i := range rows.Len() {
			data, err := json.Marshal(rows.GetOneForMarshal(i))
			if err != nil {
				t.Fatal(err)
			}
			var info driverbase.GetObjectsInfo
			if err := json.Unmarshal(data, &info); err != nil {
				t.Fatal(err)
			}
			infos = append(infos, info)
		}
	}
	if err := reader.Err(); err != nil {
		t.Fatal(err)
	}
	return infos
}

func findCatalog(infos []driverbase.GetObjectsInfo, name string) *driverbase.GetObjectsInfo {
	for i := range infos {
		if infos[i].CatalogName != nil && *infos[i].CatalogName == name {
			return &infos[i]
		}
	}
	return nil
}

func findSchema(info *driverbase.GetObjectsInfo, name string) *driverbase.DBSchemaInfo {
	for i := range info.CatalogDbSchemas {
		if *info.CatalogDbSchemas[i].DbSchemaName == name {
			return &info.CatalogDbSchemas[i]
		}
	}
	return nil
}

func tableNames(schema *driverbase.DBSchemaInfo) []string {
	var names []string
	for _, table := range schema.DbSchemaTables {
		names = append(names, table.TableName)
	}
	slices.Sort(names)
	return names
}

func TestIntegrationGetObjectsDepths(t *testing.T) {
	cnxn := metaConn(t)
	md := new("md")

	catalogs := getObjects(t, cnxn, adbc.ObjectDepthCatalogs, nil, nil, nil, nil, nil)
	for _, name := range []string{"md", "memory"} {
		if findCatalog(catalogs, name) == nil {
			t.Errorf("catalog %s missing", name)
		}
	}
	if catalogs[0].CatalogDbSchemas != nil {
		t.Error("catalog depth returned schemas")
	}

	schemas := getObjects(t, cnxn, adbc.ObjectDepthDBSchemas, md, nil, nil, nil, nil)
	if len(schemas) != 1 {
		t.Fatalf("catalog filter md matched %d catalogs", len(schemas))
	}
	for _, name := range []string{"main", "s1", "s2"} {
		if findSchema(&schemas[0], name) == nil {
			t.Errorf("schema md.%s missing", name)
		}
	}

	tables := getObjects(t, cnxn, adbc.ObjectDepthTables, md, new("s1"), nil, nil, nil)
	s1 := findSchema(&tables[0], "s1")
	if got := tableNames(s1); !slices.Equal(got, []string{"order_lines", "orders", "orders_v"}) {
		t.Errorf("md.s1 tables = %v", got)
	}
	if s1.DbSchemaTables[0].TableColumns != nil {
		t.Error("table depth returned columns")
	}

	views := getObjects(t, cnxn, adbc.ObjectDepthTables, md, nil, nil, nil, []string{"VIEW"})
	if got := tableNames(findSchema(&views[0], "s1")); !slices.Equal(got, []string{"orders_v"}) {
		t.Errorf("views = %v", got)
	}
}

func TestIntegrationGetObjectsColumns(t *testing.T) {
	cnxn := metaConn(t)
	infos := getObjects(t, cnxn, adbc.ObjectDepthAll, new("md"), new("s1"), new("orders"), nil, nil)
	tables := findSchema(&infos[0], "s1").DbSchemaTables
	if len(tables) != 1 {
		t.Fatalf("table filter matched %v", tables)
	}
	columns := tables[0].TableColumns
	if len(columns) != 3 {
		t.Fatalf("columns = %+v", columns)
	}
	total := columns[1]
	if total.ColumnName != "total" || *total.OrdinalPosition != 2 {
		t.Errorf("second column %+v", total)
	}
	if total.XdbcTypeName == nil || *total.XdbcTypeName != "DECIMAL(12,2)" {
		t.Errorf("type name %v", total.XdbcTypeName)
	}
	if *total.XdbcColumnSize != 12 || *total.XdbcDecimalDigits != 2 {
		t.Errorf("precision/scale %d/%d", *total.XdbcColumnSize, *total.XdbcDecimalDigits)
	}
	if *total.XdbcDataType != int16(driverbase.XdbcDataType_XDBC_DECIMAL) {
		t.Errorf("xdbc type %d", *total.XdbcDataType)
	}
	if *total.XdbcNullable != driverbase.XdbcColumnNoNulls || *total.XdbcIsNullable != "NO" {
		t.Errorf("NOT NULL column reported nullable: %d %s", *total.XdbcNullable, *total.XdbcIsNullable)
	}
	if *columns[2].XdbcNullable != driverbase.XdbcColumnNullable {
		t.Errorf("nullable column reported NOT NULL")
	}
	constraints := tables[0].TableConstraints
	if len(constraints) != 1 || constraints[0].ConstraintType != driverbase.PrimaryKey ||
		fmt.Sprint(constraints[0].ConstraintColumnNames) != "[id]" {
		t.Errorf("orders constraints %+v", constraints)
	}
	lines := getObjects(t, cnxn, adbc.ObjectDepthAll, new("md"), new("s1"), new("order_lines"), nil, nil)
	foreign := findSchema(&lines[0], "s1").DbSchemaTables[0].TableConstraints
	if len(foreign) != 1 || foreign[0].ConstraintType != driverbase.ForeignKey ||
		fmt.Sprint(foreign[0].ConstraintColumnNames) != "[order_id]" || len(foreign[0].ConstraintColumnUsage) != 1 ||
		foreign[0].ConstraintColumnUsage[0].ForeignKeyTable != "orders" || foreign[0].ConstraintColumnUsage[0].ForeignKeyColumn != "id" {
		t.Errorf("order_lines constraints %+v", foreign)
	}

	filtered := getObjects(t, cnxn, adbc.ObjectDepthAll, new("md"), new("s1"), new("orders"), new("n%"), nil)
	if got := findSchema(&filtered[0], "s1").DbSchemaTables[0].TableColumns; len(got) != 1 || got[0].ColumnName != "note" {
		t.Errorf("column filter n%% = %+v", got)
	}
}

// LIKE patterns: _ matches one character unless escaped.
func TestIntegrationGetObjectsPatterns(t *testing.T) {
	cnxn := metaConn(t)
	loose := getObjects(t, cnxn, adbc.ObjectDepthTables, new("md"), nil, new("order_lines"), nil, nil)
	var all []string
	for _, schema := range []string{"s1", "s2"} {
		all = append(all, tableNames(findSchema(&loose[0], schema))...)
	}
	slices.Sort(all)
	if !slices.Equal(all, []string{"order_lines", "orderxlines"}) {
		t.Errorf("order_lines (with _ as a wildcard) matched %v", all)
	}
	exact := getObjects(t, cnxn, adbc.ObjectDepthTables, new("md"), nil, new(`order\_lines`), nil, nil)
	if got := tableNames(findSchema(&exact[0], "s2")); len(got) != 0 {
		t.Errorf(`order\_lines matched %v in s2`, got)
	}
	if got := getObjects(t, cnxn, adbc.ObjectDepthCatalogs, new("m_"), nil, nil, nil, nil); len(got) != 1 || *got[0].CatalogName != "md" {
		t.Errorf("catalog pattern m_ matched %+v", got)
	}
	// A filter is a value, not SQL.
	if got := getObjects(t, cnxn, adbc.ObjectDepthTables, new("md"), nil, new("x' OR '1'='1"), nil, nil); len(tableNames(findSchema(&got[0], "s1"))) != 0 {
		t.Error("a quote in a pattern matched tables")
	}
}

func TestIntegrationGetTableSchema(t *testing.T) {
	cnxn := metaConn(t)
	schema, err := cnxn.GetTableSchema(context.Background(), new("md"), new("s1"), "order_lines")
	if err != nil {
		t.Fatal(err)
	}
	if schema.NumFields() != 2 || schema.Field(0).Name != "order_id" {
		t.Fatalf("schema %s", schema)
	}
	// The name is exact: order_lines is not orderxlines.
	if _, err := cnxn.GetTableSchema(context.Background(), new("md"), new("s2"), "order_lines"); err == nil {
		t.Fatal("md.s2.order_lines should not exist")
	} else {
		assertStatus(t, err, adbc.StatusNotFound, "")
	}
	// Without catalog and schema, the name resolves in the current ones.
	execUpdate(t, cnxn, "CREATE OR REPLACE TABLE orders (only_in_main INTEGER)")
	schema, err = cnxn.GetTableSchema(context.Background(), nil, nil, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if schema.Field(0).Name != "only_in_main" {
		t.Fatalf("resolved to %s", schema)
	}
}

func TestIntegrationGetTableTypes(t *testing.T) {
	cnxn := openConn(t)
	reader, err := cnxn.GetTableTypes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	types := stringsOf(t, readAll(t, reader), 0)
	for _, want := range []string{"BASE TABLE", "VIEW"} {
		if !slices.Contains(types, want) {
			t.Errorf("table types %v lack %s", types, want)
		}
	}
}

func TestIntegrationGetInfo(t *testing.T) {
	cnxn := openConn(t)
	reader, err := cnxn.GetInfo(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	values := map[adbc.InfoCode]any{}
	for reader.Next() {
		rec := reader.RecordBatch()
		codes := rec.Column(0).(*array.Uint32)
		union := rec.Column(1).(*array.DenseUnion)
		for i := range int(rec.NumRows()) {
			child := union.Field(union.ChildID(i))
			offset := int(union.ValueOffset(i))
			switch c := child.(type) {
			case *array.String:
				values[adbc.InfoCode(codes.Value(i))] = c.Value(offset)
			case *array.Boolean:
				values[adbc.InfoCode(codes.Value(i))] = c.Value(offset)
			case *array.Int64:
				values[adbc.InfoCode(codes.Value(i))] = c.Value(offset)
			}
		}
	}
	if want := envOr("WARP_SQL_ADBC_TEST_SERVER_NAME", "warp-duckdb"); values[adbc.InfoVendorName] != want {
		t.Errorf("vendor name %v", values[adbc.InfoVendorName])
	}
	if version, _ := values[adbc.InfoVendorVersion].(string); version == "" || version == driverbase.UnknownVersion {
		t.Errorf("vendor version %v", values[adbc.InfoVendorVersion])
	}
	if values[adbc.InfoDriverName] != driverDisplayName {
		t.Errorf("driver name %v", values[adbc.InfoDriverName])
	}
	if values[adbc.InfoVendorSql] != true || values[adbc.InfoVendorSubstrait] != false {
		t.Errorf("sql %v substrait %v", values[adbc.InfoVendorSql], values[adbc.InfoVendorSubstrait])
	}
}

type optionConn interface {
	GetOption(context.Context, string) (string, error)
	SetOption(context.Context, string, string) error
}

func TestIntegrationCurrentCatalogAndSchema(t *testing.T) {
	cnxn := metaConn(t)
	options := cnxn.(optionConn)
	if got, _ := options.GetOption(context.Background(), adbc.OptionKeyCurrentCatalog); got != "memory" {
		t.Errorf("initial catalog %q", got)
	}
	if err := options.SetOption(context.Background(), adbc.OptionKeyCurrentCatalog, "md"); err != nil {
		t.Fatal(err)
	}
	if err := options.SetOption(context.Background(), adbc.OptionKeyCurrentDbSchema, "s1"); err != nil {
		t.Fatal(err)
	}
	if got, _ := options.GetOption(context.Background(), adbc.OptionKeyCurrentDbSchema); got != "s1" {
		t.Errorf("schema %q", got)
	}
	if got := scalarInt(t, cnxn, "SELECT count(*) FROM order_lines"); got != 0 {
		t.Errorf("unqualified order_lines in md.s1: %d rows", got)
	}
	err := options.SetOption(context.Background(), adbc.OptionKeyCurrentCatalog, "no_such_catalog")
	assertStatus(t, err, adbc.StatusInvalidArgument, "invalid_option_value")
}

func TestIntegrationUnsupportedConnectionOptions(t *testing.T) {
	cnxn := openConn(t).(optionConn)
	assertStatus(t, cnxn.SetOption(context.Background(), adbc.OptionKeyReadOnly, "true"), adbc.StatusNotImplemented, "")
	assertStatus(t, cnxn.SetOption(context.Background(), OptionSessionID, "s_x"), adbc.StatusInvalidArgument, "")
}

func TestIntegrationTransactions(t *testing.T) {
	cnxn := openConn(t)
	options := cnxn.(optionConn)
	execUpdate(t, cnxn, "CREATE OR REPLACE TABLE tx (v INTEGER)")
	// Commit and rollback need autocommit off.
	assertStatus(t, cnxn.Commit(context.Background()), adbc.StatusInvalidState, "")

	if err := options.SetOption(context.Background(), adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled); err != nil {
		t.Fatal(err)
	}
	execUpdate(t, cnxn, "INSERT INTO tx VALUES (1)")
	if err := cnxn.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := scalarInt(t, cnxn, "SELECT count(*) FROM tx"); got != 0 {
		t.Fatalf("after rollback: %d rows", got)
	}
	execUpdate(t, cnxn, "INSERT INTO tx VALUES (2)")
	other := openConn(t)
	if got := scalarInt(t, other, "SELECT count(*) FROM tx"); got != 0 {
		t.Fatalf("an uncommitted row is visible elsewhere: %d", got)
	}
	if err := cnxn.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := scalarInt(t, other, "SELECT count(*) FROM tx"); got != 1 {
		t.Fatalf("after commit, another session sees %d rows", got)
	}
	if got, _ := options.GetOption(context.Background(), adbc.OptionKeyAutoCommit); got != adbc.OptionValueDisabled {
		t.Errorf("autocommit option %q", got)
	}
	if err := options.SetOption(context.Background(), adbc.OptionKeyAutoCommit, adbc.OptionValueEnabled); err != nil {
		t.Fatal(err)
	}
}
