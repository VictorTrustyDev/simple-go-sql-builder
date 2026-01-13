package sqlb

import (
	"fmt"
	"math/rand/v2"
)

type GenericTableToUse interface {
	uniqueIdentity() int64
	tableName() string
	tableAlias() string
	genericTableMeta() genericTableMetadata
	allColumns() []GenericColumnToUse
	mustSealed()
}

var _ GenericTableToUse = (*TableToUse[any])(nil)

type TableToUse[T any] struct {
	uid      int64
	sealed   bool
	metadata TableMetadata[T]
	name     string
	alias    string // alias is the alias for the table
}

// UseTable returns table to use.
func UseTable[T any]() *TableToUse[T] {
	metadata := GetTableMetadata[T]()

	return &TableToUse[T]{
		uid:      rand.Int64(),
		sealed:   false,
		metadata: metadata,
		name:     metadata.name,
		alias:    metadata.name,
	}
}

// As provides a way to alter the table name working on, when using partitioned tables.
func (t *TableToUse[T]) As(name string) *TableToUse[T] {
	t.mustNotSealed()
	if name == "" {
		panic("name cannot be empty")
	} else if t.name != t.metadata.name {
		panic("name already set")
	}

	t.name = name
	return t
}

// Alias sets the table alias.
func (t *TableToUse[T]) Alias(alias string) *TableToUse[T] {
	t.mustNotSealed()
	if alias == "" {
		panic("alias cannot be empty")
	} else if t.alias != t.metadata.name {
		panic("alias already set")
	}

	t.alias = alias
	return t
}

// ValuesToAny converts values to any.
func (t *TableToUse[T]) ValuesToAny(values []T) []any {
	result := make([]any, len(values))
	for i, v := range values {
		result[i] = v
	}
	return result
}

// ValueToAny converts value to any.
func (t *TableToUse[T]) ValueToAny(value T) any {
	return value
}

// Seal ensures that the table properties already set up correctly and prevent future changes to them.
func (t *TableToUse[T]) Seal() *TableToUse[T] {
	t.sealed = true
	if len(t.name) == 0 {
		panic("name must be set")
	}
	if len(t.alias) == 0 {
		panic("alias must be set")
	}
	return t
}

func (t *TableToUse[T]) Metadata() TableMetadata[T] {
	return t.metadata
}

// Columns returns columns by names.
func (t *TableToUse[T]) Columns(columns ...string) []GenericColumnToUse {
	t.mustSealed()
	if len(columns) == 0 {
		columns = t.metadata.ColumnsName()
	}

	uc := make([]GenericColumnToUse, len(columns))
	for i, column := range columns {
		col := t.metadata.MustGetColumnByName(column) // check if column exists
		uc[i] = newGenericColumnToUse(col, t)
	}

	return uc
}

// ColumnsExcept returns columns by names, except the given columns.
func (t *TableToUse[T]) ColumnsExcept(exceptColumns ...string) []GenericColumnToUse {
	t.mustSealed()
	if len(exceptColumns) == 0 {
		panic("no columns to exclude")
	}

	trackerExcept := make(map[string]struct{}, len(exceptColumns))
	for _, columnName := range exceptColumns {
		trackerExcept[wrapWithDoubleQuoteIfSqlKeyword(columnName)] = struct{}{}
	}

	var columns []string
	for _, columnName := range t.metadata.ColumnsName() {
		if _, found := trackerExcept[columnName]; !found {
			columns = append(columns, columnName)
		}
	}

	return t.Columns(columns...)
}

func (t *TableToUse[T]) PrimaryKeyColumns() []GenericColumnToUse {
	columns := t.metadata.PrimaryKeyColumns()
	if len(columns) == 0 {
		panic("no primary key found")
	}

	uc := make([]GenericColumnToUse, len(columns))
	for i, column := range columns {
		uc[i] = t.Col(column.name)
	}

	return uc
}

// Column returns column by name.
func (t *TableToUse[T]) Column(column string) GenericColumnToUse {
	return t.Columns(column)[0]
}

// Col returns column by name.
//
// An alias of Column.
func (t *TableToUse[T]) Col(column string) GenericColumnToUse {
	return t.Column(column)
}

// ReadFromRow reads the table from the scanned rows.
func (t *TableToUse[T]) ReadFromRow(scanner *ScannedRows) T {
	return scanner.GetTable(t.alias).(T)
}

// ReadAllFromRows reads all the table from the scanned rows.
func (t *TableToUse[T]) ReadAllFromRows(scanner *ScannedRows) []T {
	result := make([]T, 0, len(scanner.rowsOfAliasToRow))
	for scanner.Next() {
		result = append(result, t.ReadFromRow(scanner))
	}
	return result
}

func (t *TableToUse[T]) mustNotSealed() {
	if t.sealed {
		panic(fmt.Sprintf("table %s is sealed", t.metadata.name))
	}
}

func (t *TableToUse[T]) mustSealed() {
	if !t.sealed {
		panic(fmt.Sprintf("table %s is must be sealed", t.metadata.name))
	}
}

func (t *TableToUse[T]) uniqueIdentity() int64 {
	return t.uid
}

func (t *TableToUse[T]) tableName() string {
	return t.name
}

func (t *TableToUse[T]) tableAlias() string {
	return t.alias
}

func (t TableToUse[T]) genericTableMeta() genericTableMetadata {
	return t.metadata.asGeneric()
}

func (t *TableToUse[T]) allColumns() []GenericColumnToUse {
	columns := make([]GenericColumnToUse, len(t.metadata.columns))
	for i, col := range t.metadata.columns {
		columns[i] = newGenericColumnToUse(col, t)
	}
	return columns
}
