package sqlb

import (
	"context"
	"database/sql"

	"github.com/pkg/errors"
)

type ScannedRows struct {
	rowsOfAliasToRow []map[string]*row
	rowIdx           int
	anyNext          bool
}

type row struct {
	valueFunc func() any
	read      bool
}

func (sr *ScannedRows) Count() int {
	return len(sr.rowsOfAliasToRow)
}

func (sr *ScannedRows) Next() bool {
	if sr.anyNext {
		currentRow := sr.rowsOfAliasToRow[sr.rowIdx]
		for _, r := range currentRow {
			if !r.read {
				panic("not all columns are read before moving to the next row")
			}
		}
	}

	if sr.anyNext {
		sr.rowIdx++
	} else {
		sr.anyNext = true
		sr.rowIdx = 0
	}
	return sr.rowIdx < len(sr.rowsOfAliasToRow)
}

func (sr *ScannedRows) GetTable(byAlias string) any {
	if !sr.anyNext {
		panic("require calls Next() first")
	}
	r := sr.rowsOfAliasToRow[sr.rowIdx][byAlias]
	r.read = true
	return r.valueFunc()
}

var _ SqlRows = (*sql.Rows)(nil)

func (b *SqlBuilder) Query(sqlDB *sql.DB) (*ScannedRows, error) {
	b.mustTypeSelect()
	b.mustBasicSelect()
	stmt, args := b.Build()
	return b.scanRows(sqlDB.Query(stmt, args...))
}

func (b *SqlBuilder) QueryWithContext(ctx context.Context, sqlTx *sql.Tx) (*ScannedRows, error) {
	b.mustTypeSelect()
	b.mustBasicSelect()
	stmt, args := b.Build()
	return b.scanRows(sqlTx.QueryContext(ctx, stmt, args...))
}

func (b *SqlBuilder) QueryExists(sqlDB *sql.DB) (exists bool, err error) {
	b.mustSelectExists()
	stmt, args := b.Build()
	rows, err := sqlDB.Query(stmt, args...)
	if err != nil {
		return false, err
	}

	defer func() {
		_ = rows.Close()
	}()

	if !rows.Next() {
		return false, errors.New("no rows returned")
	}

	err = rows.Scan(&exists)
	if err != nil {
		return false, err
	}

	return exists, nil
}

func (b *SqlBuilder) QueryExistsWithContext(ctx context.Context, sqlTx *sql.Tx) (exists bool, err error) {
	b.mustSelectExists()
	stmt, args := b.Build()
	rows, err := sqlTx.QueryContext(ctx, stmt, args...)
	if err != nil {
		return false, err
	}

	defer func() {
		_ = rows.Close()
	}()

	if !rows.Next() {
		return false, errors.New("no rows returned")
	}

	err = rows.Scan(&exists)
	if err != nil {
		return false, err
	}

	return exists, nil
}

func (b *SqlBuilder) QueryCount(sqlDB *sql.DB) (count int, err error) {
	b.mustSelectCount()
	stmt, args := b.Build()
	rows, err := sqlDB.Query(stmt, args...)
	if err != nil {
		return 0, err
	}

	defer func() {
		_ = rows.Close()
	}()

	if !rows.Next() {
		return 0, errors.New("no rows returned")
	}

	err = rows.Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (b *SqlBuilder) QueryCountWithContext(ctx context.Context, sqlTx *sql.Tx) (count int, err error) {
	b.mustSelectCount()
	stmt, args := b.Build()
	rows, err := sqlTx.QueryContext(ctx, stmt, args...)
	if err != nil {
		return 0, err
	}

	defer func() {
		_ = rows.Close()
	}()

	if !rows.Next() {
		return 0, errors.New("no rows returned")
	}

	err = rows.Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (b *SqlBuilder) scanRows(rows SqlRows, err error) (*ScannedRows, error) {
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = rows.Close()
	}()

	b.mustTypeSelect()
	sr := &ScannedRows{
		rowsOfAliasToRow: make([]map[string]*row, 0),
	}

	tablesByAlias := make(map[string]GenericTableToUse)
	columnsByTableAlias := make(map[string][]string)
	tableAliasToColumnToIndex := make(map[string]map[string]int)
	for i, column := range b.selectColumns {
		alias := column.table.tableAlias()
		tablesByAlias[alias] = column.table
		columnsByTableAlias[alias] = append(columnsByTableAlias[alias], column.name)

		if _, found := tableAliasToColumnToIndex[alias]; !found {
			tableAliasToColumnToIndex[alias] = make(map[string]int)
		}
		tableAliasToColumnToIndex[alias][column.name] = i
	}

	for rows.Next() {
		rowScanErr := func() (err error) {
			aliasToRow := make(map[string]*row)
			defer func() {
				sr.rowsOfAliasToRow = append(sr.rowsOfAliasToRow, aliasToRow)
			}()
			//
			columnsForScanning := make([]any, len(b.selectColumns))
			optionalTransformFunctions := make([]func() error, 0, len(b.selectColumns))
			defer func() {
				if err == nil {
					for _, transformFunc := range optionalTransformFunctions {
						if transformFunc == nil {
							continue
						}
						if transErr := transformFunc(); transErr != nil {
							err = errors.Wrap(transErr, "failed to transform column")
							return
						}
					}
				}
			}()

			// construct columns for scanning and output
			for _, table := range tablesByAlias {
				tableAlias := table.tableAlias()
				columns := columnsByTableAlias[tableAlias]
				vf, specs := table.genericTableMeta().selectSpecOfColumns(columns...)
				aliasToRow[tableAlias] = &row{
					valueFunc: vf,
				}

				// register transform functions, order is not important
				for _, spec := range specs {
					optionalTransformFunctions = append(optionalTransformFunctions, spec.OptionalTransform)
				}

				// register columns for scanning, order is VERY important
				for i, column := range columns {
					spec := specs[i]
					columnsForScanning[tableAliasToColumnToIndex[tableAlias][column]] = spec.ToQueryArg()
				}
			}

			err = rows.Scan(columnsForScanning...)
			if err != nil {
				err = errors.Wrap(err, "failed to scan row")
				return
			}

			return nil
		}()
		if rowScanErr != nil {
			return nil, rowScanErr
		}
	}

	return sr, nil
}

func (b *SqlBuilder) Exec(sqlDB *sql.DB) (sql.Result, error) {
	b.mustTypeInsert()
	stmt, args := b.Build()
	return sqlDB.Exec(stmt, args...)
}

func (b *SqlBuilder) ExecContext(ctx context.Context, sqlTx *sql.Tx) (sql.Result, error) {
	b.mustTypeInsert()
	stmt, args := b.Build()
	return sqlTx.ExecContext(ctx, stmt, args...)
}
