package sqlb

import (
	"fmt"
	"strings"
)

type GenericColumnToUse struct {
	name  string
	isPk  bool
	table GenericTableToUse
}

func newGenericColumnToUse[T any](column ColumnMetadata[T], table GenericTableToUse) GenericColumnToUse {
	return GenericColumnToUse{
		name:  column.Name(),
		isPk:  column.isPk,
		table: table,
	}
}

// nameWithAlias returns [alias].[column]
func (c GenericColumnToUse) nameWithAlias() string {
	return c.table.tableAlias() + "." + c.name
}

// NameOnly returns [column]
func (c GenericColumnToUse) NameOnly() string {
	return c.name
}

// NameWithTableName returns [table].[column]
func (c GenericColumnToUse) NameWithTableName() string {
	return c.table.genericTableMeta().Name() + "." + c.name
}

// Excluded returns excluded.[column]
func (c GenericColumnToUse) Excluded() string {
	return "excluded." + c.name
}

// FromExcluded generates statement '[column] = excluded.[column]', used in ON CONFLICT DO UPDATE
func (c GenericColumnToUse) FromExcluded() string {
	return c.name + " = " + c.Excluded()
}

// FromCoalesceWithExcluded generates statement '[column] = COALESCE([table].[column], excluded.[column])', used in ON CONFLICT DO UPDATE
func (c GenericColumnToUse) FromCoalesceWithExcluded() string {
	return c.name + " = COALESCE(" + c.NameWithTableName() + ", " + c.Excluded() + ")"
}

// EqualsToCurrent generates statement '[column] = [table].[column]', used in ON CONFLICT DO UPDATE
func (c GenericColumnToUse) EqualsToCurrent() string {
	return c.name + " = " + c.NameWithTableName()
}

// Greatest generates statement '[column] = GREATEST([table].[column], excluded.[column])', used in ON CONFLICT DO UPDATE
func (c GenericColumnToUse) Greatest() string {
	return c.name + " = GREATEST(" + c.NameWithTableName() + ", " + c.Excluded() + ")"
}

// Least generates statement '[column] = LEAST([table].[column], excluded.[column])', used in ON CONFLICT DO UPDATE
func (c GenericColumnToUse) Least() string {
	return c.name + " = LEAST(" + c.NameWithTableName() + ", " + c.Excluded() + ")"
}

// GinStringArrayContains generates statement '[column] @> ARRAY[$1]::TEXT[]'
func (c GenericColumnToUse) GinStringArrayContains(argumentNumber int) string {
	return fmt.Sprintf(`%s @> ARRAY[$%d]::TEXT[]`, c.name, argumentNumber)
}

// Gin2DimensionalByteArrayContains generates statement '[column] @> ARRAY[$1]::BYTEA[]'
func (c GenericColumnToUse) Gin2DimensionalByteArrayContains(argumentNumber int) string {
	return fmt.Sprintf(`%s @> ARRAY[$%d]::BYTEA[]`, c.name, argumentNumber)
}

// InNumbers generates statement '[column] IN (1,2,3)'
func (c GenericColumnToUse) InNumbers(numbers ...int) string {
	var sb strings.Builder
	sb.WriteString(c.name)
	sb.WriteString(" IN (")
	for i, number := range numbers {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(fmt.Sprintf("%d", number))
	}
	sb.WriteString(")")
	return sb.String()
}
