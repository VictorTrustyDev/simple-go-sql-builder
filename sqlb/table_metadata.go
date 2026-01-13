package sqlb

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"

	"golang.org/x/exp/maps"
)

var (
	mutexRegisterTable        sync.Mutex
	registeredTableTypeToName = make(map[string]string)
	registeredTables          = make(map[string]any)
)

type TableMetadata[T any] struct {
	name          string
	columns       []ColumnMetadata[T]
	columnsByName map[string]ColumnMetadata[T]
}

func GetTableMetadata[T any]() TableMetadata[T] {
	typeName := getStructTypeName(new(T))
	if name, found := registeredTableTypeToName[typeName]; found {
		return registeredTables[name].(TableMetadata[T])
	}
	panic(fmt.Sprintf("table for type %s is not registered", typeName))
}

func GetRegisteredTablesName() []string {
	return maps.Keys(registeredTables)
}

func (t TableMetadata[T]) Name() string {
	return t.name
}

func (t TableMetadata[T]) Columns() []ColumnMetadata[T] {
	clone := make([]ColumnMetadata[T], len(t.columns))
	copy(clone, t.columns)
	return clone
}

func (t TableMetadata[T]) PrimaryKeyColumns() []ColumnMetadata[T] {
	var columns []ColumnMetadata[T]
	for _, col := range t.columns {
		if col.isPk {
			columns = append(columns, col)
		}
	}
	return columns
}

func (t TableMetadata[T]) ColumnsName() []string {
	names := make([]string, len(t.columns))
	for i, col := range t.columns {
		names[i] = col.name
	}
	return names
}

func (t TableMetadata[T]) MustGetColumnByName(name string) ColumnMetadata[T] {
	if col, found := t.columnsByName[wrapWithDoubleQuoteIfSqlKeyword(name)]; found {
		return col
	}
	panic(fmt.Sprintf("column with name %s not found", name))
}

// NewRow returns new struct of type T
func (t TableMetadata[T]) NewRow() T {
	return *new(T)
}

type TableMetadataBuilder[T any] struct {
	name    string
	columns []*ColumnMetadataBuilder[T]
}

func NewTableMetadata[T any](name string) *TableMetadataBuilder[T] {
	return &TableMetadataBuilder[T]{
		name: name,
	}
}

func (b *TableMetadataBuilder[T]) AddColumns(columns ...*ColumnMetadataBuilder[T]) *TableMetadataBuilder[T] {
	for _, cb := range columns {
		column := cb.column
		column.name = wrapWithDoubleQuoteIfSqlKeyword(strings.TrimSpace(column.name))
		cb.column = column

		b.columns = append(b.columns, cb)
	}
	return b
}

type TableMetadataBuildOption struct {
	ExpectedPkColumns []string // used to double-check the primary key columns
}

func (b *TableMetadataBuilder[T]) Build(opt TableMetadataBuildOption) TableMetadata[T] {
	mutexRegisterTable.Lock()
	defer mutexRegisterTable.Unlock()

	columns := make([]ColumnMetadata[T], len(b.columns))
	columnsByName := make(map[string]ColumnMetadata[T])
	pkColumnsName := make([]string, 0)
	for i, col := range b.columns {
		columns[i] = col.column
		if _, found := columnsByName[col.column.name]; found {
			panic(fmt.Sprintf("column with name %s is already added", col.column.name))
		}
		columnsByName[col.column.name] = col.column
		if col.column.isPk {
			pkColumnsName = append(pkColumnsName, col.column.name)
		}
	}

	opt.ExpectedPkColumns = wrapManyWithDoubleQuoteIfSqlKeyword(opt.ExpectedPkColumns...)
	sort.Strings(pkColumnsName)
	sort.Strings(opt.ExpectedPkColumns)
	if !reflect.DeepEqual(pkColumnsName, opt.ExpectedPkColumns) {
		panic(fmt.Sprintf("expected primary keys [%s] for table %s, but got [%s]", strings.Join(opt.ExpectedPkColumns, ", "), b.name, strings.Join(pkColumnsName, ", ")))
	}

	tableMetadata := TableMetadata[T]{
		name:          b.name,
		columns:       columns,
		columnsByName: columnsByName,
	}

	{ // register table
		typeName := getStructTypeName(new(T))

		if _, found := registeredTableTypeToName[typeName]; found { // prevent duplicate registration
			panic(fmt.Sprintf("table for type %s is already registered", typeName))
		}

		//
		registeredTableTypeToName[typeName] = b.name
		registeredTables[b.name] = tableMetadata
	}

	return tableMetadata
}

func getStructTypeName(v any) string {
	if t := reflect.TypeOf(v); t.Kind() == reflect.Ptr {
		return t.Elem().Name()
	} else {
		return t.Name()
	}
}

type genericTableMetadata interface {
	Name() string
	typeName() string
	selectSpecOfColumns(columnsName ...string) (valueFunc func() any, specs []ResultColumnSelectSpec)
	insertSpecOfColumns(columnsName ...string) []func(any) any
}

func (t TableMetadata[T]) asGeneric() genericTableMetadata {
	return t
}

var _ genericTableMetadata = TableMetadata[any]{}

func (t TableMetadata[T]) newRow() any {
	return t.NewRow()
}

func (t TableMetadata[T]) typeName() string {
	return getStructTypeName(new(T))
}

func (t TableMetadata[T]) selectSpecOfColumns(columnsName ...string) (func() any, []ResultColumnSelectSpec) {
	if len(columnsName) == 0 {
		columnsName = t.ColumnsName()
	}

	row := t.NewRow()

	columns := make([]ResultColumnSelectSpec, len(columnsName))
	for i, name := range columnsName {
		name := wrapWithDoubleQuoteIfSqlKeyword(name)
		_, selectSpec := t.MustGetColumnByName(name).SelectSpec()
		columns[i] = selectSpec(&row)
	}

	return func() any {
		return row
	}, columns
}

func (t TableMetadata[T]) insertSpecOfColumns(columnsName ...string) []func(any) any {
	if len(columnsName) == 0 {
		columnsName = t.ColumnsName()
	}

	result := make([]func(any) any, len(columnsName))
	for i, name := range columnsName {
		name := wrapWithDoubleQuoteIfSqlKeyword(name)
		_, insertSpec := t.MustGetColumnByName(name).InsertSpec()
		result[i] = func(a any) any {
			return insertSpec(a.(T))
		}
	}

	return result
}

// Contains SQL keywords that need to be double-quoted.
// Can be added via AddSqlKeyword
var sqlKeywords map[string]struct{}

// AddSqlKeyword adds a SQL keyword to be double-quoted when used as table or column name.
func AddSqlKeyword(keyword string) {
	sqlKeywords[strings.ToLower(keyword)] = struct{}{}
}

func wrapWithDoubleQuoteIfSqlKeyword(name string) string {
	if _, found := sqlKeywords[name]; found {
		return fmt.Sprintf(`"%s"`, name)
	}
	return name
}

func wrapManyWithDoubleQuoteIfSqlKeyword(name ...string) []string {
	result := make([]string, len(name))
	for i, n := range name {
		result[i] = wrapWithDoubleQuoteIfSqlKeyword(n)
	}
	return result
}

func init() {
	sqlKeywords = map[string]struct{}{}

	predefinedList := []string{
		"count", "index", "name", "type", "types", "from", "to", "order", "value", "state", "time", "left", "right", "day", "local",
	}

	for _, kw := range predefinedList {
		sqlKeywords[kw] = struct{}{}
	}
}
