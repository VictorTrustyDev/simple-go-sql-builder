package sqlb

//goland:noinspection GoSnakeCaseUsage
type (
	ColumnInsertSpec[T any] func(T) (insertArg any)

	QueryArg_ColumnSelectSpec          func() any
	OptionalTransform_ColumnSelectSpec func() error

	ResultColumnSelectSpec struct {
		ToQueryArg        QueryArg_ColumnSelectSpec
		OptionalTransform OptionalTransform_ColumnSelectSpec
	}

	ColumnSelectSpec[T any] func(*T) ResultColumnSelectSpec
)

type ColumnMetadata[T any] struct {
	name       string
	isPk       bool // indicate this column is PK or a part of multi-columns-PK
	insertSpec ColumnInsertSpec[T]
	selectSpec ColumnSelectSpec[T]
}

func (c ColumnMetadata[T]) Name() string {
	return c.name
}

func (c ColumnMetadata[T]) InsertSpec() (columnName string, spec ColumnInsertSpec[T]) {
	return c.name, c.insertSpec
}

func (c ColumnMetadata[T]) SelectSpec() (columnName string, spec ColumnSelectSpec[T]) {
	return c.name, c.selectSpec
}

type ColumnMetadataBuilder[T any] struct {
	column ColumnMetadata[T]
}

func NewColumnMetadata[T any](
	name string,
) *ColumnMetadataBuilder[T] {
	return &ColumnMetadataBuilder[T]{
		column: ColumnMetadata[T]{
			name: name,
		},
	}
}

// SelectSpec sets the select spec for this column
func (b *ColumnMetadataBuilder[T]) SelectSpec(spec ColumnSelectSpec[T]) *ColumnMetadataBuilder[T] {
	b.column.selectSpec = spec
	return b
}

// InsertSpec sets the insert spec for this column
func (b *ColumnMetadataBuilder[T]) InsertSpec(spec ColumnInsertSpec[T]) *ColumnMetadataBuilder[T] {
	b.column.insertSpec = spec
	return b
}

// PrimaryKey marks this column is a part of multi-columns-PK
func (b *ColumnMetadataBuilder[T]) PrimaryKey() *ColumnMetadataBuilder[T] {
	b.column.isPk = true
	return b
}
