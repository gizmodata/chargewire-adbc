// Licensed under the Apache License, Version 2.0.

package chargewire

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/gizmodata/chargewire-adbc/go/v2/internal/protocol"
)

// Field metadata keys of Flight SQL column metadata, which the tables
// endpoint's table_schema carries (§10).
const (
	metaTypeName      = "ARROW:FLIGHT:SQL:TYPE_NAME"
	metaPrecision     = "ARROW:FLIGHT:SQL:PRECISION"
	metaScale         = "ARROW:FLIGHT:SQL:SCALE"
	metaAutoIncrement = "ARROW:FLIGHT:SQL:IS_AUTO_INCREMENT"
	metaRemarks       = "ARROW:FLIGHT:SQL:REMARKS"
)

// decodeSchema decodes a base64 Arrow schema from a JSON document (§2.2).
func decodeSchema(encoded string, alloc memory.Allocator) (*arrow.Schema, error) {
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decoding a base64 Arrow schema: %w", err)
	}
	return readSchema(data, alloc)
}

// readMetadata reads a metadata endpoint (§10) and calls fn for each batch.
func (c *connectionImpl) readMetadata(ctx context.Context, kind string, query url.Values, fn func(arrow.RecordBatch) error) error {
	resp, err := c.session.Metadata(ctx, kind, query)
	if err != nil {
		return wrapError(err, "reading metadata/"+kind)
	}
	defer resp.Body.Close()
	reader, err := newIPCReader(resp.Body, c.Alloc)
	if err != nil {
		return wrapError(err, "reading metadata/"+kind)
	}
	defer reader.Release()
	for reader.Next() {
		if err := fn(reader.RecordBatch()); err != nil {
			return err
		}
	}
	return wrapError(reader.Err(), "reading metadata/"+kind)
}

// stringColumn returns a utf8 column of a metadata result by name.
func stringColumn(rec arrow.RecordBatch, name string) (*array.String, error) {
	indices := rec.Schema().FieldIndices(name)
	if len(indices) == 0 {
		return nil, adbc.Error{Code: adbc.StatusInternal, Msg: fmt.Sprintf("[%s] metadata result has no column %q", driverName, name)}
	}
	column, ok := rec.Column(indices[0]).(*array.String)
	if !ok {
		return nil, adbc.Error{Code: adbc.StatusInternal, Msg: fmt.Sprintf("[%s] metadata column %q is %s, not utf8", driverName, name, rec.Column(indices[0]).DataType())}
	}
	return column, nil
}

// stringAt copies a value out of a metadata column. Values from Arrow alias
// the record's buffers, which are freed (and, with the C library's malloc
// allocator, reused) once the stream is released.
func stringAt(column *array.String, i int) string {
	return strings.Clone(column.Value(i))
}

func stringValue(column *array.String, i int) *string {
	if column.IsNull(i) {
		return nil
	}
	return new(stringAt(column, i))
}

// ListTableTypes implements driverbase.TableTypeLister.
func (c *connectionImpl) ListTableTypes(ctx context.Context) ([]string, error) {
	var types []string
	err := c.readMetadata(ctx, "table-types", nil, func(rec arrow.RecordBatch) error {
		column, err := stringColumn(rec, "table_type")
		if err != nil {
			return err
		}
		for i := range column.Len() {
			types = append(types, stringAt(column, i))
		}
		return nil
	})
	return types, err
}

// likePattern matches names against a SQL LIKE pattern: % any run, _ one
// character, \ escapes (§10). A nil pattern matches everything.
type likePattern struct{ re *regexp.Regexp }

func compileLike(pattern *string) likePattern {
	if pattern == nil {
		return likePattern{}
	}
	var b strings.Builder
	b.WriteString("^(?s:")
	escaped := false
	for _, r := range *pattern {
		switch {
		case escaped:
			b.WriteString(regexp.QuoteMeta(string(r)))
			escaped = false
		case r == '\\':
			escaped = true
		case r == '%':
			b.WriteString(".*")
		case r == '_':
			b.WriteString(".")
		default:
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	if escaped {
		b.WriteString(`\\`)
	}
	b.WriteString(")$")
	return likePattern{re: regexp.MustCompile(b.String())}
}

func (p likePattern) match(name string) bool {
	return p.re == nil || p.re.MatchString(name)
}

// escapeLike quotes a name so the server's LIKE matches it exactly.
func escapeLike(name string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(name)
}

// tableRow is one row of the tables endpoint.
type tableRow struct {
	catalog, schema *string
	name, kind      string
	arrowSchema     *arrow.Schema // when asked for
}

// listTables reads the tables endpoint (§10).
func (c *connectionImpl) listTables(ctx context.Context, query url.Values, withSchema bool) ([]tableRow, error) {
	if withSchema {
		query.Set("include_schema", "true")
	}
	var rows []tableRow
	err := c.readMetadata(ctx, "tables", query, func(rec arrow.RecordBatch) error {
		catalogs, err := stringColumn(rec, "catalog_name")
		if err != nil {
			return err
		}
		schemas, err := stringColumn(rec, "db_schema_name")
		if err != nil {
			return err
		}
		names, err := stringColumn(rec, "table_name")
		if err != nil {
			return err
		}
		kinds, err := stringColumn(rec, "table_type")
		if err != nil {
			return err
		}
		var tableSchemas *array.Binary
		if withSchema {
			indices := rec.Schema().FieldIndices("table_schema")
			if len(indices) == 0 {
				return adbc.Error{Code: adbc.StatusInternal, Msg: fmt.Sprintf("[%s] the tables endpoint returned no table_schema column", driverName)}
			}
			var ok bool
			if tableSchemas, ok = rec.Column(indices[0]).(*array.Binary); !ok {
				return adbc.Error{Code: adbc.StatusInternal, Msg: fmt.Sprintf("[%s] table_schema is %s, not binary", driverName, rec.Column(indices[0]).DataType())}
			}
		}
		for i := range int(rec.NumRows()) {
			row := tableRow{catalog: stringValue(catalogs, i), schema: stringValue(schemas, i), name: stringAt(names, i), kind: stringAt(kinds, i)}
			if tableSchemas != nil {
				// table_schema is a serialized schema message (§10).
				schema, err := readSchema(bytes.Clone(tableSchemas.Value(i)), c.Alloc)
				if err != nil {
					return adbc.Error{Code: adbc.StatusInternal, Msg: fmt.Sprintf("[%s] decoding the schema of %s: %v", driverName, row.name, err)}
				}
				row.arrowSchema = schema
			}
			rows = append(rows, row)
		}
		return nil
	})
	return rows, err
}

// GetObjects builds the catalog/schema/table/column hierarchy from the
// metadata endpoints, with one request per level. Patterns on catalogs and
// columns are applied here; the server filters schemas, tables and table
// types. At column depth, primary and foreign keys come from one request each
// to the key endpoints.
func (c *connectionImpl) GetObjects(ctx context.Context, depth adbc.ObjectDepth, catalog, dbSchema, tableName, columnName *string, tableTypes []string) (rdr array.RecordReader, err error) {
	ctx, span := c.StartSpan(ctx, "Connection.GetObjects")
	defer driverbase.EndSpanWithError(span, &err)

	catalogPattern := compileLike(catalog)
	var infos []driverbase.GetObjectsInfo
	index := map[string]int{}
	err = c.readMetadata(ctx, "catalogs", nil, func(rec arrow.RecordBatch) error {
		names, err := stringColumn(rec, "catalog_name")
		if err != nil {
			return err
		}
		for i := range names.Len() {
			if name := stringAt(names, i); catalogPattern.match(name) {
				index[name] = len(infos)
				infos = append(infos, driverbase.GetObjectsInfo{CatalogName: new(name)})
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	if depth != adbc.ObjectDepthCatalogs {
		for i := range infos {
			infos[i].CatalogDbSchemas = []driverbase.DBSchemaInfo{}
		}
		if err := c.addSchemas(ctx, infos, index, dbSchema); err != nil {
			return nil, err
		}
	}
	if depth == adbc.ObjectDepthTables || depth == adbc.ObjectDepthColumns {
		if err := c.addTables(ctx, depth, infos, index, dbSchema, tableName, columnName, tableTypes); err != nil {
			return nil, err
		}
	}

	in := make(chan driverbase.GetObjectsInfo, len(infos))
	for _, info := range infos {
		in <- info
	}
	close(in)
	return driverbase.BuildGetObjectsRecordReader(c.Alloc, in, make(chan error))
}

func (c *connectionImpl) addSchemas(ctx context.Context, infos []driverbase.GetObjectsInfo, index map[string]int, dbSchema *string) error {
	query := url.Values{}
	if dbSchema != nil {
		query.Set("db_schema_filter_pattern", *dbSchema)
	}
	return c.readMetadata(ctx, "db-schemas", query, func(rec arrow.RecordBatch) error {
		catalogs, err := stringColumn(rec, "catalog_name")
		if err != nil {
			return err
		}
		schemas, err := stringColumn(rec, "db_schema_name")
		if err != nil {
			return err
		}
		for i := range schemas.Len() {
			if catalogs.IsNull(i) {
				continue
			}
			if at, ok := index[catalogs.Value(i)]; ok {
				infos[at].CatalogDbSchemas = append(infos[at].CatalogDbSchemas, driverbase.DBSchemaInfo{DbSchemaName: new(stringAt(schemas, i))})
			}
		}
		return nil
	})
}

func (c *connectionImpl) addTables(ctx context.Context, depth adbc.ObjectDepth, infos []driverbase.GetObjectsInfo, index map[string]int,
	dbSchema, tableName, columnName *string, tableTypes []string) error {
	query := url.Values{}
	if dbSchema != nil {
		query.Set("db_schema_filter_pattern", *dbSchema)
	}
	if tableName != nil {
		query.Set("table_name_filter_pattern", *tableName)
	}
	if len(tableTypes) > 0 {
		query.Set("table_types", strings.Join(tableTypes, ","))
	}
	withColumns := depth == adbc.ObjectDepthColumns
	rows, err := c.listTables(ctx, query, withColumns)
	if err != nil {
		return err
	}
	var constraints map[tableKey][]driverbase.ConstraintInfo
	if withColumns {
		if constraints, err = c.listConstraints(ctx); err != nil {
			return err
		}
	}
	columnPattern := compileLike(columnName)
	for i := range infos {
		for j := range infos[i].CatalogDbSchemas {
			infos[i].CatalogDbSchemas[j].DbSchemaTables = []driverbase.TableInfo{}
		}
	}
	for _, row := range rows {
		if row.catalog == nil || row.schema == nil {
			continue
		}
		at, ok := index[*row.catalog]
		if !ok {
			continue
		}
		schemas := infos[at].CatalogDbSchemas
		s := slices.IndexFunc(schemas, func(info driverbase.DBSchemaInfo) bool { return *info.DbSchemaName == *row.schema })
		if s < 0 {
			continue
		}
		table := driverbase.TableInfo{TableName: row.name, TableType: row.kind}
		if withColumns {
			table.TableColumns = []driverbase.ColumnInfo{}
			table.TableConstraints = constraints[tableKey{*row.catalog, *row.schema, row.name}]
			if table.TableConstraints == nil {
				table.TableConstraints = []driverbase.ConstraintInfo{}
			}
			for position, field := range row.arrowSchema.Fields() {
				if columnPattern.match(field.Name) {
					table.TableColumns = append(table.TableColumns, columnInfo(field, position+1))
				}
			}
		}
		schemas[s].DbSchemaTables = append(schemas[s].DbSchemaTables, table)
	}
	return nil
}

// columnInfo describes a column from its Arrow field and Flight SQL column
// metadata.
func columnInfo(field arrow.Field, position int) driverbase.ColumnInfo {
	info := driverbase.ColumnInfo{ColumnName: field.Name, OrdinalPosition: new(int32(position))}
	xdbcType := int16(driverbase.ToXdbcDataType(field.Type))
	info.XdbcDataType, info.XdbcSqlDataType = new(xdbcType), new(xdbcType)
	if field.Nullable {
		info.XdbcNullable, info.XdbcIsNullable = new(driverbase.XdbcColumnNullable), new("YES")
	} else {
		info.XdbcNullable, info.XdbcIsNullable = new(driverbase.XdbcColumnNoNulls), new("NO")
	}
	meta := func(key string) (string, bool) {
		if i := field.Metadata.FindKey(key); i >= 0 {
			return field.Metadata.Values()[i], true
		}
		return "", false
	}
	if typeName, ok := meta(metaTypeName); ok {
		info.XdbcTypeName = new(typeName)
	}
	if remarks, ok := meta(metaRemarks); ok {
		info.Remarks = new(remarks)
	}
	if auto, ok := meta(metaAutoIncrement); ok {
		info.XdbcIsAutoincrement = new(auto == "1" || strings.EqualFold(auto, "true"))
	}
	if precision, ok := meta(metaPrecision); ok {
		if n, err := strconv.ParseInt(precision, 10, 32); err == nil {
			info.XdbcColumnSize = new(int32(n))
		}
	}
	if scale, ok := meta(metaScale); ok {
		if n, err := strconv.ParseInt(scale, 10, 16); err == nil {
			info.XdbcDecimalDigits = new(int16(n))
		}
	}
	if _, ok := field.Type.(arrow.DecimalType); ok {
		info.XdbcNumPrecRadix = new(int16(10))
	}
	return info
}

// tableKey names a table in GetObjects.
type tableKey struct{ catalog, schema, table string }

// keyColumn is one column of a primary or foreign key.
type keyColumn struct {
	sequence int32
	column   string
	// For a foreign key, the column it references.
	references *driverbase.ConstraintColumnUsage
}

// listConstraints reads every visible table's primary and foreign keys, with
// one request to each key endpoint (§10). A server whose key endpoints need a
// table name reports none.
func (c *connectionImpl) listConstraints(ctx context.Context) (map[tableKey][]driverbase.ConstraintInfo, error) {
	type constraintKey struct {
		table      tableKey
		kind, name string
	}
	columns := map[constraintKey][]keyColumn{}
	var order []constraintKey
	add := func(key constraintKey, column keyColumn) {
		if _, seen := columns[key]; !seen {
			order = append(order, key)
		}
		columns[key] = append(columns[key], column)
	}

	err := c.readMetadata(ctx, "primary-keys", nil, func(rec arrow.RecordBatch) error {
		names, err := stringColumns(rec, "catalog_name", "db_schema_name", "table_name", "column_name", "key_name")
		if err != nil {
			return err
		}
		sequences, err := int32Column(rec, "key_sequence")
		if err != nil {
			return err
		}
		for i := range int(rec.NumRows()) {
			key := constraintKey{table: tableKey{stringAt(names[0], i), stringAt(names[1], i), stringAt(names[2], i)},
				kind: driverbase.PrimaryKey, name: stringAt(names[4], i)}
			add(key, keyColumn{sequence: sequences.Value(i), column: stringAt(names[3], i)})
		}
		return nil
	})
	if protocol.HasCode(err, protocol.CodeBadRequest) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	err = c.readMetadata(ctx, "imported-keys", nil, func(rec arrow.RecordBatch) error {
		names, err := stringColumns(rec, "pk_catalog_name", "pk_db_schema_name", "pk_table_name", "pk_column_name",
			"fk_catalog_name", "fk_db_schema_name", "fk_table_name", "fk_column_name", "fk_key_name")
		if err != nil {
			return err
		}
		sequences, err := int32Column(rec, "key_sequence")
		if err != nil {
			return err
		}
		for i := range int(rec.NumRows()) {
			name := stringAt(names[8], i)
			if names[8].IsNull(i) {
				name = stringAt(names[2], i) // an unnamed key, told apart by the table it references
			}
			key := constraintKey{table: tableKey{stringAt(names[4], i), stringAt(names[5], i), stringAt(names[6], i)},
				kind: driverbase.ForeignKey, name: name}
			add(key, keyColumn{sequence: sequences.Value(i), column: stringAt(names[7], i),
				references: &driverbase.ConstraintColumnUsage{
					ForeignKeyCatalog: stringValue(names[0], i), ForeignKeyDbSchema: stringValue(names[1], i),
					ForeignKeyTable: stringAt(names[2], i), ForeignKeyColumn: stringAt(names[3], i),
				}})
		}
		return nil
	})
	if err != nil && !protocol.HasCode(err, protocol.CodeBadRequest) {
		return nil, err
	}

	constraints := map[tableKey][]driverbase.ConstraintInfo{}
	for _, key := range order {
		keyColumns := columns[key]
		slices.SortFunc(keyColumns, func(a, b keyColumn) int { return int(a.sequence - b.sequence) })
		info := driverbase.ConstraintInfo{ConstraintType: key.kind}
		if key.name != "" {
			info.ConstraintName = new(key.name)
		}
		var names []string
		for _, column := range keyColumns {
			names = append(names, column.column)
			if column.references != nil {
				info.ConstraintColumnUsage = append(info.ConstraintColumnUsage, *column.references)
			}
		}
		info.ConstraintColumnNames = driverbase.RequiredList(names)
		constraints[key.table] = append(constraints[key.table], info)
	}
	return constraints, nil
}

// stringColumns returns several utf8 columns of a metadata result.
func stringColumns(rec arrow.RecordBatch, names ...string) ([]*array.String, error) {
	columns := make([]*array.String, len(names))
	for i, name := range names {
		column, err := stringColumn(rec, name)
		if err != nil {
			return nil, err
		}
		columns[i] = column
	}
	return columns, nil
}

// int32Column returns an int32 column of a metadata result by name.
func int32Column(rec arrow.RecordBatch, name string) (*array.Int32, error) {
	indices := rec.Schema().FieldIndices(name)
	if len(indices) == 0 {
		return nil, adbc.Error{Code: adbc.StatusInternal, Msg: fmt.Sprintf("[%s] metadata result has no column %q", driverName, name)}
	}
	column, ok := rec.Column(indices[0]).(*array.Int32)
	if !ok {
		return nil, adbc.Error{Code: adbc.StatusInternal, Msg: fmt.Sprintf("[%s] metadata column %q is %s, not int32", driverName, name, rec.Column(indices[0]).DataType())}
	}
	return column, nil
}

// GetTableSchema returns a table's Arrow schema, with the server's column
// metadata. Without a catalog or schema, a name that several schemas share
// resolves to the session's current catalog and schema.
func (c *connectionImpl) GetTableSchema(ctx context.Context, catalog, dbSchema *string, tableName string) (schema *arrow.Schema, err error) {
	ctx, span := c.StartSpan(ctx, "Connection.GetTableSchema")
	defer driverbase.EndSpanWithError(span, &err)

	query := url.Values{"table_name_filter_pattern": {escapeLike(tableName)}}
	if catalog != nil {
		query.Set("catalog", *catalog)
	}
	if dbSchema != nil {
		query.Set("db_schema_filter_pattern", escapeLike(*dbSchema))
	}
	rows, err := c.listTables(ctx, query, true)
	if err != nil {
		return nil, err
	}
	rows = slices.DeleteFunc(rows, func(row tableRow) bool { return row.name != tableName })
	if len(rows) > 1 && (catalog == nil || dbSchema == nil) {
		doc, err := c.session.Get(ctx)
		if err != nil {
			return nil, wrapError(err, "reading the session")
		}
		currentCatalog, _ := doc.Options["catalog"].(string)
		currentSchema, _ := doc.Options["schema"].(string)
		rows = slices.DeleteFunc(rows, func(row tableRow) bool {
			return (catalog == nil && (row.catalog == nil || *row.catalog != currentCatalog)) ||
				(dbSchema == nil && (row.schema == nil || *row.schema != currentSchema))
		})
	}
	switch len(rows) {
	case 0:
		return nil, c.ErrorHelper.NotFound("table %s not found", qualified(catalog, dbSchema, tableName))
	case 1:
		return rows[0].arrowSchema, nil
	}
	return nil, c.ErrorHelper.InvalidArgument("table name %s is ambiguous; give its catalog and schema", tableName)
}

func qualified(catalog, dbSchema *string, table string) string {
	parts := []string{}
	if catalog != nil {
		parts = append(parts, *catalog)
	}
	if dbSchema != nil {
		parts = append(parts, *dbSchema)
	}
	return strings.Join(append(parts, table), ".")
}
