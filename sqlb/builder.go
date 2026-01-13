package sqlb

import (
	"fmt"
	"strings"
)

type SqlBuilder struct {
	//
	_type                sqlBuilderType
	previousAction       previousAddedBuilderAction
	aliasToTableUniqueId map[string]int64 // alias to unique id of the using table, used to validate input
	tableUniqueIdToAlias map[int64]string // unique id to alias of the using table
	// special fields for type select
	selectType      selectType
	selectColumns   []GenericColumnToUse
	selectFromTable []GenericTableToUse
	joinsOn         []joinOn
	whereTokens     []any
	whereArgs       []any // whereArgs is the arguments for the whereCondition clause
	orders          []orderBy
	offset          uint // offset is the number of rows to skip
	limit           uint // limit is the number of rows to return
	// special fields for type insert
	insertIntoTable                     GenericTableToUse
	insertColumns                       []GenericColumnToUse
	insertValues                        []any
	insertOnConflictKeys                []GenericColumnToUse
	insertOnConflictDoUpdateTokens      []any
	insertOnConflictDoUpdateWhereTokens []any
	insertOnConflictDoNothing           bool
}

func newSqlBuilder() *SqlBuilder {
	return &SqlBuilder{
		//
		_type:                sqlBuilderTypeSelect,
		selectType:           notSelectTypeBasic,
		previousAction:       nonePrevious,
		aliasToTableUniqueId: make(map[string]int64),
		tableUniqueIdToAlias: make(map[int64]string),
	}
}

func SelectExists() *SqlBuilder {
	b := Select()
	b.selectType = selectTypeExists
	return b
}

func SelectCount() *SqlBuilder {
	b := Select()
	b.selectType = selectTypeCount
	return b
}

func Select(selectColumns ...GenericColumnToUse) *SqlBuilder {
	b := newSqlBuilder()
	b._type = sqlBuilderTypeSelect
	b.selectType = selectTypeBasic
	b.previousAction = previousIsSelect
	return b.Select(selectColumns...)
}

func InsertInto[T any](use *TableToUse[T], columns ...GenericColumnToUse) *SqlBuilder {
	b := newSqlBuilder()
	b._type = sqlBuilderTypeInsert
	defer b.setPreviousAction(previousIsInsertInto)

	if len(columns) == 0 {
		for _, c := range GetTableMetadata[T]().Columns() {
			columns = append(columns, use.Col(c.name))
		}
	}
	b.insertColumns = columns

	b.registerUsingTable(use)
	b.insertIntoTable = use
	return b
}

// registerUsingTable performs validation and registration of the using table.
func (b *SqlBuilder) registerUsingTable(use GenericTableToUse) {
	use.mustSealed()
	alias := use.tableAlias()
	uid := use.uniqueIdentity()

	// one alias cannot be used by multiple using tables
	if byTableUid, found := b.aliasToTableUniqueId[alias]; found && byTableUid != uid {
		panic(fmt.Sprintf("alias %s already used by table (alias): %s", alias, b.tableUniqueIdToAlias[byTableUid]))
	}

	// set
	b.aliasToTableUniqueId[alias] = uid
	b.tableUniqueIdToAlias[uid] = alias
}

// mustPreviousAction checks if the previous action is one of the expected actions.
func (b *SqlBuilder) mustPreviousAction(expected ...previousAddedBuilderAction) {
	var matchAny bool
	for _, e := range expected {
		if b.previousAction == e {
			matchAny = true
			break
		}
	}
	if !matchAny {
		if len(expected) == 1 {
			panic(fmt.Sprintf("unexpected previous action %s, expected %s", b.previousAction, expected[0]))
		} else {
			var expectedStr []string
			for _, e := range expected {
				expectedStr = append(expectedStr, string(e))
			}
			panic(fmt.Sprintf("unexpected previous action %s, expected any of [%s]", b.previousAction, strings.Join(expectedStr, ",")))
		}
	}
}

func (b *SqlBuilder) setPreviousAction(a previousAddedBuilderAction) {
	b.previousAction = a
}

func (b *SqlBuilder) mustSelectType(_type selectType) {
	b.mustTypeSelect()
	if b.selectType != _type {
		panic(fmt.Sprintf("only %s is supported by this operation, got %s", _type, b.selectType))
	}
}

func (b *SqlBuilder) mustBasicSelect() {
	b.mustSelectType(selectTypeBasic)
}

func (b *SqlBuilder) mustSelectExists() {
	b.mustSelectType(selectTypeExists)
}

func (b *SqlBuilder) mustSelectCount() {
	b.mustSelectType(selectTypeCount)
}

// SELECT

func (b *SqlBuilder) mustTypeSelect() {
	if b._type != sqlBuilderTypeSelect {
		panic(fmt.Sprintf("only %s is supported, got %s", sqlBuilderTypeSelect, b._type))
	}
}

// Select adds more columns to the SELECT statement.
func (b *SqlBuilder) Select(columns ...GenericColumnToUse) *SqlBuilder {
	b.mustTypeSelect()
	b.mustBasicSelect()
	b.mustPreviousAction(previousIsSelect)
	defer b.setPreviousAction(previousIsSelect)
	for _, column := range columns {
		b.registerUsingTable(column.table)
	}
	b.selectColumns = append(b.selectColumns, columns...)
	return b
}

// From specifies the tables to SELECT FROM
func (b *SqlBuilder) From(tables ...GenericTableToUse) *SqlBuilder {
	b.mustTypeSelect()
	b.mustPreviousAction(previousIsSelect, previousIsSelectFrom)
	defer b.setPreviousAction(previousIsSelectFrom)

	for _, table := range tables {
		b.registerUsingTable(table)
	}
	b.selectFromTable = tables
	return b
}

// Join add JOIN...ON clause.
func (b *SqlBuilder) Join(joinType JoinType, joinOnTable GenericTableToUse, onKeyPairs ...GenericColumnToUse) *SqlBuilder {
	b.mustTypeSelect()
	b.mustPreviousAction(previousIsSelectFrom, previousIsSelectJoin)
	if len(onKeyPairs)%2 != 0 {
		panic("onKeyPairs must be even")
	}
	defer b.setPreviousAction(previousIsSelectJoin)

	joinOnTableName := joinOnTable.tableName()
	// loop through each pair
	for i := 0; i < len(onKeyPairs); i += 2 {
		leftTable := onKeyPairs[i].table
		rightTable := onKeyPairs[i+1].table

		if leftTable.tableName() == rightTable.tableName() {
			panic(fmt.Sprintf("join on the same table at pair no.%d", i/2+1))
		} else if leftTable.tableName() != joinOnTableName && rightTable.tableName() != joinOnTableName {
			panic(fmt.Sprintf("either of the join must be table %s, got %s and %s", joinOnTableName, leftTable.tableName(), rightTable.tableName()))
		}

		b.registerUsingTable(leftTable)
		b.registerUsingTable(rightTable)
	}

	b.joinsOn = append(b.joinsOn, joinOn{
		joinType:      joinType,
		joinOnTable:   joinOnTable,
		joinOnColumns: onKeyPairs,
	})
	return b
}

// Where adds the WHERE clause. If having argument on SELECT, need to call Args
func (b *SqlBuilder) Where(whereTokens ...any) *SqlBuilder {
	if b._type == sqlBuilderTypeSelect {
		b.mustPreviousAction(previousIsSelectFrom, previousIsSelectJoin, previousIsSelectWhere)
		defer b.setPreviousAction(previousIsSelectWhere)

		b.whereTokens = append(b.whereTokens, whereTokens...)
	} else if b._type == sqlBuilderTypeInsert {
		b.mustPreviousAction(previousIsInsertIntoOnConflictDoUpdate)
		defer b.setPreviousAction(previousIsInsertIntoOnConflictDoUpdateWhere)

		b.insertOnConflictDoUpdateWhereTokens = whereTokens
	} else {
		panic(fmt.Sprintf("WHERE is not supported for this type %s", b._type))
	}
	return b
}

// And continues the WHERE clause with AND.
func (b *SqlBuilder) And(whereTokens ...any) *SqlBuilder {
	if b._type == sqlBuilderTypeSelect {
		b.mustPreviousAction(previousIsSelectWhere)

		if len(b.whereTokens) == 0 {
			panic("AND must be after WHERE")
		} else if len(whereTokens) == 0 {
			panic("AND must have at least one token")
		}

		b.whereTokens = append(b.whereTokens, "AND")
		b.whereTokens = append(b.whereTokens, whereTokens...)
	} else if b._type == sqlBuilderTypeInsert {
		b.mustPreviousAction(previousIsInsertIntoOnConflictDoUpdateWhere)

		if len(b.insertOnConflictDoUpdateWhereTokens) == 0 {
			panic("AND must be after WHERE")
		} else if len(whereTokens) == 0 {
			panic("AND must have at least one token")
		}

		b.insertOnConflictDoUpdateWhereTokens = append(b.insertOnConflictDoUpdateWhereTokens, "AND")
		b.insertOnConflictDoUpdateWhereTokens = append(b.insertOnConflictDoUpdateWhereTokens, whereTokens...)
	} else {
		panic(fmt.Sprintf("WHERE is not supported for this type %s", b._type))
	}
	return b
}

// Or continues the WHERE clause with OR.
func (b *SqlBuilder) Or(whereTokens ...any) *SqlBuilder {
	if b._type == sqlBuilderTypeSelect {
		b.mustPreviousAction(previousIsSelectWhere)

		if len(b.whereTokens) == 0 {
			panic("OR must be after WHERE")
		} else if len(whereTokens) == 0 {
			panic("OR must have at least one token")
		}

		b.whereTokens = append(b.whereTokens, "OR")
		b.whereTokens = append(b.whereTokens, whereTokens...)
	} else if b._type == sqlBuilderTypeInsert {
		b.mustPreviousAction(previousIsInsertIntoOnConflictDoUpdateWhere)

		if len(b.insertOnConflictDoUpdateWhereTokens) == 0 {
			panic("OR must be after WHERE")
		} else if len(whereTokens) == 0 {
			panic("OR must have at least one token")
		}

		b.insertOnConflictDoUpdateWhereTokens = append(b.insertOnConflictDoUpdateWhereTokens, "OR")
		b.insertOnConflictDoUpdateWhereTokens = append(b.insertOnConflictDoUpdateWhereTokens, whereTokens...)
	} else {
		panic(fmt.Sprintf("WHERE is not supported for this type %s", b._type))
	}
	return b
}

// Args provides args for the WHERE clause.
func (b *SqlBuilder) Args(whereArgs ...any) *SqlBuilder {
	b.mustTypeSelect()
	b.mustPreviousAction(previousIsSelectWhere)
	b.whereArgs = append(b.whereArgs, whereArgs...)
	return b
}

func (b *SqlBuilder) AnyWhereTokens() bool {
	if b._type == sqlBuilderTypeSelect {
		return len(b.whereTokens) > 0
	} else {
		panic(fmt.Sprintf("the operation does not support type %s", b._type))
	}
}

// OrderBy adds the ORDER BY clause.
func (b *SqlBuilder) OrderBy(column GenericColumnToUse, asc OrderType) *SqlBuilder {
	b.mustTypeSelect()
	b.mustBasicSelect()
	b.mustPreviousAction(previousIsSelectFrom, previousIsSelectJoin, previousIsSelectWhere, previousIsSelectOrderBy)
	defer b.setPreviousAction(previousIsSelectOrderBy)

	b.orders = append(b.orders, orderBy{
		column: column,
		asc:    bool(asc),
	})
	return b
}

// ThenBy continues the ORDER BY clause with another column.
func (b *SqlBuilder) ThenBy(column GenericColumnToUse, asc OrderType) *SqlBuilder {
	b.mustTypeSelect()
	b.mustBasicSelect()
	b.mustPreviousAction(previousIsSelectOrderBy)

	b.orders = append(b.orders, orderBy{
		column: column,
		asc:    bool(asc),
	})
	return b
}

// Pagination adds the OFFSET and LIMIT clauses if the pagination is not nil and the values are greater than 0.
func (b *SqlBuilder) Pagination(pagination *Pagination) *SqlBuilder {
	if pagination == nil {
		return b
	}

	if pagination.offset > 0 {
		b.Offset(pagination.offset)
	}
	if pagination.limit > 0 {
		b.Limit(pagination.limit)
	}
	return b
}

func (b *SqlBuilder) Offset(offset uint) *SqlBuilder {
	b.mustTypeSelect()
	b.mustBasicSelect()
	b.mustPreviousAction(previousIsSelectFrom, previousIsSelectJoin, previousIsSelectWhere, previousIsSelectOrderBy, previousIsSelectLimit)
	defer b.setPreviousAction(previousIsSelectOffset)

	b.offset = offset
	return b
}

func (b *SqlBuilder) Limit(limit uint) *SqlBuilder {
	b.mustTypeSelect()
	b.mustBasicSelect()
	b.mustPreviousAction(previousIsSelectFrom, previousIsSelectJoin, previousIsSelectWhere, previousIsSelectOrderBy, previousIsSelectOffset)
	defer b.setPreviousAction(previousIsSelectLimit)

	b.limit = limit
	return b
}

// INSERT INTO

func (b *SqlBuilder) mustTypeInsert() {
	if b._type != sqlBuilderTypeInsert {
		panic(fmt.Sprintf("only %s is supported, got %s", sqlBuilderTypeInsert, b._type))
	}
}

// Values put the values to be inserted.
func (b *SqlBuilder) Values(values ...any) *SqlBuilder {
	b.mustTypeInsert()
	b.mustPreviousAction(previousIsInsertInto)
	defer b.setPreviousAction(previousIsInsertIntoValues)

	// validation
	for _, value := range values {
		if getStructTypeName(value) != b.insertIntoTable.genericTableMeta().typeName() {
			panic(fmt.Sprintf("value %T is not of type %s", value, b.insertIntoTable.genericTableMeta().typeName()))
		}
	}

	// set
	b.insertValues = values
	return b
}

// OnConflict adds the ON CONFLICT clause with the columns to be checked.
func (b *SqlBuilder) OnConflict(columns ...GenericColumnToUse) *SqlBuilder {
	b.mustTypeInsert()
	b.mustPreviousAction(previousIsInsertIntoValues)
	defer b.setPreviousAction(previousIsInsertIntoOnConflict)

	// validation
	if len(b.insertOnConflictKeys) > 0 {
		panic("ON CONFLICT keys already added")
	}
	for _, column := range columns {
		if column.table.tableName() != b.insertIntoTable.tableName() {
			panic(fmt.Sprintf("column %s is not from table %s", column.name, b.insertIntoTable.tableName()))
		}
	}

	// set
	b.insertOnConflictKeys = columns
	return b
}

// DoUpdate adds the ON CONFLICT UPDATE clause.
func (b *SqlBuilder) DoUpdate(tokens ...any) *SqlBuilder {
	b.mustTypeInsert()
	b.mustPreviousAction(previousIsInsertIntoOnConflict, previousIsInsertIntoOnConflictDoUpdate)
	defer b.setPreviousAction(previousIsInsertIntoOnConflictDoUpdate)

	if len(b.insertOnConflictKeys) < 1 {
		panic("ON CONFLICT keys not added")
	}
	if len(b.insertOnConflictDoUpdateTokens) > 0 {
		b.insertOnConflictDoUpdateTokens = append(b.insertOnConflictDoUpdateTokens, ",\n")
	}
	b.insertOnConflictDoUpdateTokens = append(b.insertOnConflictDoUpdateTokens, tokens...)
	return b
}

// DoUpdateExceptPrimaryKeys adds the ON CONFLICT UPDATE clause to excluded, except the primary keys.
func (b *SqlBuilder) DoUpdateExceptPrimaryKeys() *SqlBuilder {
	b.mustTypeInsert()

	var tokens []any
	for _, column := range b.insertIntoTable.allColumns() {
		if column.isPk {
			continue
		}
		if len(tokens) > 0 {
			tokens = append(tokens, ",\n")
		}
		tokens = append(tokens, column.FromExcluded())
	}

	return b.DoUpdate(tokens...)
}

// DoNothing adds the ON CONFLICT DO NOTHING clause.
func (b *SqlBuilder) DoNothing() *SqlBuilder {
	b.mustTypeInsert()
	b.mustPreviousAction(previousIsInsertIntoOnConflict)
	defer b.setPreviousAction(previousIsInsertIntoOnConflictDoNoThing)

	b.insertOnConflictDoNothing = true
	return b
}

// Build

func (b *SqlBuilder) Build() (sql string, args []any) {
	switch b._type {
	case sqlBuilderTypeSelect:
		return b.buildSelect()
	case sqlBuilderTypeInsert:
		return b.buildInsert()
	default:
		panic(fmt.Sprintf("unknown builder type: %s", b._type))
	}
}

func (b *SqlBuilder) buildSelect() (sql string, args []any) {
	if len(b.selectColumns) == 0 {
		switch b.selectType {
		case selectTypeBasic:
			panic("no columns selected")
		case selectTypeExists, selectTypeCount:
			// valid
		default:
			panic(fmt.Sprintf("unexpected select type %s", b.selectType))
		}
	}
	if len(b.selectFromTable) == 0 {
		panic("no tables selected")
	}

	sb := strings.Builder{}

	// SELECT
	sb.WriteString("SELECT ")
	if b.selectType == selectTypeExists {
		sb.WriteString("1 ")
	} else if b.selectType == selectTypeCount {
		sb.WriteString("COUNT(1) ")
	} else {
		for i, column := range b.selectColumns {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(column.nameWithAlias())
		}
		sb.WriteString("\n")
	}

	// FROM
	sb.WriteString("FROM ")
	for i, table := range b.selectFromTable {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(table.tableName())
		sb.WriteString(" AS ")
		sb.WriteString(table.tableAlias())
	}
	sb.WriteString("\n")

	// JOIN
	for _, joinOn := range b.joinsOn {
		switch joinOn.joinType {
		case LeftJoin:
			sb.WriteString("LEFT JOIN ")
		case RightJoin:
			sb.WriteString("RIGHT JOIN ")
		default:
			sb.WriteString("INNER JOIN ")
		}
		sb.WriteString(joinOn.joinOnTable.tableName())
		sb.WriteString(" AS ")
		sb.WriteString(joinOn.joinOnTable.tableAlias())
		sb.WriteString(" ON ")
		for i := 0; i < len(joinOn.joinOnColumns); i += 2 {
			if i > 0 {
				sb.WriteString(" AND ")
			}
			left := joinOn.joinOnColumns[i]
			right := joinOn.joinOnColumns[i+1]
			sb.WriteString(left.nameWithAlias())
			sb.WriteString(" = ")
			sb.WriteString(right.nameWithAlias())
		}
		sb.WriteString("\n")
	}

	// WHERE
	if len(b.whereTokens) > 0 {
		sb.WriteString("WHERE")
		for _, token := range b.whereTokens {
			sb.WriteString(" ")
			switch t := token.(type) {
			case string:
				sb.WriteString(strings.TrimSpace(t))
			case GenericColumnToUse:
				sb.WriteString(t.nameWithAlias())
			case int8, uint8, int16, uint16, int32, uint32, int64, uint64, int, uint:
				sb.WriteString(fmt.Sprintf("%d", t))
			case bool:
				if t {
					sb.WriteString("TRUE")
				} else {
					sb.WriteString("FALSE")
				}
			default:
				panic(fmt.Sprintf("unexpected WHERE token type %T", t))
			}
		}
		sb.WriteString("\n")
	}

	// ORDER BY
	if len(b.orders) > 0 {
		sb.WriteString("ORDER BY ")
		for i, order := range b.orders {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(order.column.nameWithAlias())
			if order.asc {
				sb.WriteString(" ASC")
			} else {
				sb.WriteString(" DESC")
			}
		}
		sb.WriteString("\n")
	}

	// OFFSET & LIMIT
	if b.offset > 0 && b.limit > 0 {
		sb.WriteString(fmt.Sprintf("OFFSET %d LIMIT %d\n", b.offset, b.limit))
	} else if b.offset > 0 {
		sb.WriteString("OFFSET ")
		sb.WriteString(fmt.Sprintf("%d", b.offset))
		sb.WriteString("\n")
	} else if b.limit > 0 {
		sb.WriteString("LIMIT ")
		sb.WriteString(fmt.Sprintf("%d", b.limit))
		sb.WriteString("\n")
	}

	stmt := sb.String()
	if b.selectType == selectTypeExists {
		stmt = fmt.Sprintf("SELECT EXISTS(%s)", stmt)
	}

	return stmt, b.whereArgs
}

func (b *SqlBuilder) buildInsert() (sql string, args []any) {
	if len(b.insertColumns) == 0 {
		panic("no columns selected for inserting")
	}
	if b.insertIntoTable == nil {
		panic("no tables selected for inserting")
	}
	if len(b.insertValues) == 0 {
		panic("no values for inserting")
	}

	sb := strings.Builder{}

	// INSERT INTO
	sb.WriteString("INSERT INTO ")
	sb.WriteString(b.insertIntoTable.tableName())
	sb.WriteString(" (")
	columnsName := make([]string, len(b.insertColumns))
	for i, column := range b.insertColumns {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(column.name)
		columnsName[i] = column.name
	}
	// VALUES
	sb.WriteString(")\nVALUES ")
	columnsCount := len(b.insertColumns)
	values := make([]any, 0, columnsCount*len(b.insertValues))
	for i, record := range b.insertValues {
		vi := i * columnsCount

		if i > 0 {
			sb.WriteString(",")
		}

		sb.WriteString("(")
		for paramIdx := 1; paramIdx <= columnsCount; paramIdx++ {
			if paramIdx > 1 {
				sb.WriteString(",")
			}

			sb.WriteString(fmt.Sprintf("$%d", vi+paramIdx))
		}
		sb.WriteString(")")

		for _, isf := range b.insertIntoTable.genericTableMeta().insertSpecOfColumns(columnsName...) {
			values = append(values, isf(record))
		}
	}

	// ON CONFLICT
	if b.insertOnConflictDoNothing {
		sb.WriteString("\nON CONFLICT DO NOTHING")
	} else if len(b.insertOnConflictKeys) > 0 {
		sb.WriteString("\nON CONFLICT (")
		for i, column := range b.insertOnConflictKeys {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(column.name)
		}
		sb.WriteString(") ")

		sb.WriteString("DO UPDATE SET\n")
		for _, token := range b.insertOnConflictDoUpdateTokens {
			sb.WriteString(" ")
			switch t := token.(type) {
			case string:
				sb.WriteString(strings.TrimSpace(t))
			case GenericColumnToUse:
				sb.WriteString(t.name)
			case int8, uint8, int16, uint16, int32, uint32, int64, uint64, int, uint:
				sb.WriteString(fmt.Sprintf("%d", t))
			case bool:
				if t {
					sb.WriteString("TRUE")
				} else {
					sb.WriteString("FALSE")
				}
			default:
				panic(fmt.Sprintf("unexpected ON CONFLICT UPDATE token type %T", t))
			}
		}
		if len(b.insertOnConflictDoUpdateWhereTokens) > 0 {
			sb.WriteString("\nWHERE")
			for _, token := range b.insertOnConflictDoUpdateWhereTokens {
				sb.WriteString(" ")
				switch t := token.(type) {
				case string:
					sb.WriteString(strings.TrimSpace(t))
				case GenericColumnToUse:
					sb.WriteString(t.table.tableName())
					sb.WriteString(".")
					sb.WriteString(t.name)
				case int8, uint8, int16, uint16, int32, uint32, int64, uint64, int, uint:
					sb.WriteString(fmt.Sprintf("%d", t))
				case bool:
					if t {
						sb.WriteString("TRUE")
					} else {
						sb.WriteString("FALSE")
					}
				default:
					panic(fmt.Sprintf("unexpected ON CONFLICT UPDATE WHERE token type %T", t))
				}
			}
		}
	}

	return sb.String(), values
}
