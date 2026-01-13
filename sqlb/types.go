package sqlb

type sqlBuilderType string

const (
	sqlBuilderTypeSelect sqlBuilderType = "SELECT"
	sqlBuilderTypeInsert                = "INSERT"
)

type selectType string

const (
	notSelectTypeBasic selectType = "-"
	selectTypeBasic               = "SELECT"
	selectTypeExists              = "SELECT EXISTS"
	selectTypeCount               = "SELECT COUNT"
)

type previousAddedBuilderAction string

const (
	nonePrevious previousAddedBuilderAction = "none"
	// SELECT
	previousIsSelect        previousAddedBuilderAction = "SELECT"
	previousIsSelectFrom    previousAddedBuilderAction = "SELECT FROM"
	previousIsSelectJoin    previousAddedBuilderAction = "SELECT JOIN"
	previousIsSelectWhere   previousAddedBuilderAction = "SELECT WHERE"
	previousIsSelectOrderBy previousAddedBuilderAction = "SELECT ORDER BY"
	previousIsSelectOffset  previousAddedBuilderAction = "SELECT OFFSET"
	previousIsSelectLimit   previousAddedBuilderAction = "SELECT LIMIT"
	// INSERT
	previousIsInsertInto                        previousAddedBuilderAction = "INSERT INTO"
	previousIsInsertIntoValues                  previousAddedBuilderAction = "INSERT VALUES"
	previousIsInsertIntoOnConflict              previousAddedBuilderAction = "INSERT ON CONFLICT"
	previousIsInsertIntoOnConflictDoUpdate      previousAddedBuilderAction = "INSERT ON CONFLICT DO UPDATE"
	previousIsInsertIntoOnConflictDoUpdateWhere previousAddedBuilderAction = "INSERT ON CONFLICT DO UPDATE WHERE"
	previousIsInsertIntoOnConflictDoNoThing     previousAddedBuilderAction = "INSERT ON CONFLICT DO NOTHING"
	//
)

//goland:noinspection GoSnakeCaseUsage
type JoinType uint8

//goland:noinspection GoUnusedConst
const (
	InnerJoin JoinType = iota
	LeftJoin
	RightJoin
)

type joinOn struct {
	joinType      JoinType
	joinOnTable   GenericTableToUse
	joinOnColumns []GenericColumnToUse
}

// OrderType is used to specify the order of the results
type OrderType bool

const (
	ASC  OrderType = true
	DESC OrderType = false
)

type orderBy struct {
	column GenericColumnToUse
	asc    bool
}

type SqlRows interface {
	Next() bool
	Scan(dest ...any) error
	Close() error
}

type Pagination struct {
	offset uint
	limit  uint
}

func NewPaginationFromPagingConfig(page, size int) *Pagination {
	if page < 1 {
		panic("page must be greater than 0")
	}
	if size < 1 {
		panic("size must be greater than 0")
	}
	return &Pagination{
		offset: uint((page - 1) * size),
		limit:  uint(size),
	}
}

func (p *Pagination) Set(offset, limit uint) {
	p.offset = offset
	p.limit = limit
}

func (p *Pagination) Offset() uint {
	if p == nil {
		return 0
	}
	return p.offset
}

func (p *Pagination) Limit() uint {
	if p == nil {
		return 0
	}
	return p.limit
}
