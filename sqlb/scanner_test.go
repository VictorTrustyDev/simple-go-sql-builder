package sqlb

import (
	"testing"

	"github.com/pkg/errors"

	"github.com/stretchr/testify/require"
)

type mockRowScanner struct {
	columns []string
	rows    [][]any
	rowIdx  int
	anyNext bool
}

func (m *mockRowScanner) Next() bool {
	if m.anyNext {
		m.rowIdx++
	} else {
		m.anyNext = true
		m.rowIdx = 0
	}
	return m.rowIdx < len(m.rows)
}

func (m *mockRowScanner) Scan(dest ...any) error {
	row := m.rows[m.rowIdx]
	for i, d := range dest {
		v := row[i]

		switch d := d.(type) {
		case *string:
			*d = v.(string)
		case *int:
			*d = v.(int)
		case *int64:
			*d = v.(int64)
		default:
			return errors.Errorf("unsupported type %T", d)
		}
	}

	return nil
}

func (m *mockRowScanner) Close() error {
	return nil
}

var _ SqlRows = (*mockRowScanner)(nil)

func TestScannedRows(t *testing.T) {
	sr := &ScannedRows{
		rowsOfAliasToRow: []map[string]*row{
			{
				"alias1": {valueFunc: func() any { return 1 }},
				"alias2": {valueFunc: func() any { return 2 }},
			},
			{
				"alias1": {valueFunc: func() any { return 3 }},
				"alias2": {valueFunc: func() any { return 4 }},
			},
			{
				"alias1": {valueFunc: func() any { return 5 }},
				"alias2": {valueFunc: func() any { return 6 }},
			},
		},
	}

	require.True(t, sr.Next())
	require.Equal(t, 1, sr.GetTable("alias1"))
	require.Equal(t, 2, sr.GetTable("alias2"))

	require.True(t, sr.Next())
	require.Equal(t, 3, sr.GetTable("alias1"))
	require.Equal(t, 4, sr.GetTable("alias2"))

	require.True(t, sr.Next())
	require.Equal(t, 5, sr.GetTable("alias1"))
	require.Equal(t, 6, sr.GetTable("alias2"))
}

func TestSqlBuilder_scanRows(t *testing.T) {
	mockScanner := &mockRowScanner{
		rows: [][]any{
			{1, "1testa", "2testb", int64(4)},
			{5, "3testa", "4testb", int64(8)},
		},
	}

	table1 := UseTable[testStruct1]().Alias("t1").Seal()
	table2 := UseTable[testStruct2]().Alias("t2").Seal()

	builder := Select(
		table1.Col("amount"),
		table2.Col("amount"),
		table1.Col("cost"),
		table2.Col("pk3"),
	).From(table1, table2)

	rows, err := builder.scanRows(mockScanner, nil)
	require.NoError(t, err)

	require.True(t, rows.Next())
	t1 := table1.ReadFromRow(rows)
	t2 := table2.ReadFromRow(rows)
	require.Equal(t, testStruct1{
		Amount: 1,
		Cost: Money{
			Currency: "testb",
			Amount:   2,
		},
	}, t1)
	require.Equal(t, testStruct2{
		Pk3: 4,
		Amount: Money{
			Currency: "testa",
			Amount:   1,
		},
	}, t2)

	require.True(t, rows.Next())
	t1 = table1.ReadFromRow(rows)
	t2 = table2.ReadFromRow(rows)
	require.Equal(t, testStruct1{
		Amount: 5,
		Cost: Money{
			Currency: "testb",
			Amount:   4,
		},
	}, t1)
	require.Equal(t, testStruct2{
		Pk3: 8,
		Amount: Money{
			Currency: "testa",
			Amount:   3,
		},
	}, t2)
}
