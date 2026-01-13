package sqlb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

//goland:noinspection SqlNoDataSourceInspection
func TestSqlBuilder2_buildSelect(t *testing.T) {
	tests := []struct {
		name     string
		builder  func() *SqlBuilder
		wantSql  string
		wantArgs []any
	}{
		{
			name: "select * from one table",
			builder: func() *SqlBuilder {
				table1 := UseTable[testStruct1]().Seal()
				return Select(
					table1.Columns()...,
				).From(table1)
			},
			wantSql: `SELECT table1.pk1, table1.pk2, table1.amount, table1.cost
FROM table1 AS table1
`,
			wantArgs: nil,
		},
		{
			name: "select * from one table with alias",
			builder: func() *SqlBuilder {
				table1 := UseTable[testStruct1]().Alias("t").Seal()
				return Select(
					table1.Columns()...,
				).From(table1)
			},
			wantSql: `SELECT t.pk1, t.pk2, t.amount, t.cost
FROM table1 AS t
`,
			wantArgs: nil,
		},
		{
			name: "select * from one table with custom table name",
			builder: func() *SqlBuilder {
				table1 := UseTable[testStruct1]().As("table_1_1").Alias("t").Seal()
				return Select(
					table1.Columns()...,
				).From(table1)
			},
			wantSql: `SELECT t.pk1, t.pk2, t.amount, t.cost
FROM table_1_1 AS t
`,
			wantArgs: nil,
		},
		{
			name: "select some columns from one table",
			builder: func() *SqlBuilder {
				table1 := UseTable[testStruct1]().Seal()
				return Select(
					table1.Columns("pk1", "pk2", "cost")...,
				).From(table1)
			},
			wantSql: `SELECT table1.pk1, table1.pk2, table1.cost
FROM table1 AS table1
`,
			wantArgs: nil,
		},
		{
			name: "select some columns from multiple tables",
			builder: func() *SqlBuilder {
				table1 := UseTable[testStruct1]().Alias("t1").Seal()
				table2 := UseTable[testStruct2]().Alias("t2").Seal()
				return Select(
					table1.Columns("pk1", "pk2", "cost")...,
				).Select(
					table2.Columns("pk1", "pk2", "pk3", "amount")...,
				).
					From(table1, table2).
					Where(table1.Col("pk1"), "=", table2.Col("pk1")).
					And(table1.Col("pk2"), "=", table2.Col("pk2"))
			},
			wantSql: `SELECT t1.pk1, t1.pk2, t1.cost, t2.pk1, t2.pk2, t2.pk3, t2.amount
FROM table1 AS t1, table2 AS t2
WHERE t1.pk1 = t2.pk1 AND t1.pk2 = t2.pk2
`,
			wantArgs: nil,
		},
		{
			name: "select some columns from multiple tables of one table",
			builder: func() *SqlBuilder {
				table1 := UseTable[testStruct1]().Alias("ta").Seal()
				stillTable1 := UseTable[testStruct1]().Alias("tb").Seal()
				return Select(
					table1.Columns("pk1", "pk2", "cost")...,
				).Select(
					stillTable1.Columns("pk1", "pk2", "amount")...,
				).
					From(table1, stillTable1).
					Where(table1.Col("pk1"), "=", stillTable1.Col("pk1"))
			},
			wantSql: `SELECT ta.pk1, ta.pk2, ta.cost, tb.pk1, tb.pk2, tb.amount
FROM table1 AS ta, table1 AS tb
WHERE ta.pk1 = tb.pk1
`,
			wantArgs: nil,
		},
		{
			name: "select some columns from one tables with where clause",
			builder: func() *SqlBuilder {
				table1 := UseTable[testStruct1]().Alias("t1").Seal()
				return Select(
					table1.Columns("cost", "amount")...,
				).
					From(table1).
					Where(table1.Col("amount"), "= $1").Args(100)
			},
			wantSql: `SELECT t1.cost, t1.amount
FROM table1 AS t1
WHERE t1.amount = $1
`,
			wantArgs: []any{100},
		},
		{
			name: "select some columns from multiple tables with join",
			builder: func() *SqlBuilder {
				table1 := UseTable[testStruct1]().Alias("t1").Seal()
				table2 := UseTable[testStruct2]().Alias("t2").Seal()
				return Select(
					table1.Columns("pk1", "pk2", "cost")...,
				).Select(
					table2.Columns("pk3", "amount")...,
				).
					From(table1).
					Join(
						InnerJoin,
						table2,
						table1.Col("pk1"), table2.Col("pk1"),
						table1.Col("pk2"), table2.Col("pk2"),
					)
			},
			wantSql: `SELECT t1.pk1, t1.pk2, t1.cost, t2.pk3, t2.amount
FROM table1 AS t1
INNER JOIN table2 AS t2 ON t1.pk1 = t2.pk1 AND t1.pk2 = t2.pk2
`,
			wantArgs: nil,
		},
		{
			name: "select some columns from one tables with order by",
			builder: func() *SqlBuilder {
				table1 := UseTable[testStruct1]().Alias("t1").Seal()
				return Select(
					table1.Columns("cost", "amount")...,
				).
					From(table1).
					OrderBy(table1.Col("amount"), DESC).
					ThenBy(table1.Col("pk1"), ASC)
			},
			wantSql: `SELECT t1.cost, t1.amount
FROM table1 AS t1
ORDER BY t1.amount DESC, t1.pk1 ASC
`,
			wantArgs: nil,
		},
		{
			name: "select some columns from one tables with paging",
			builder: func() *SqlBuilder {
				table1 := UseTable[testStruct1]().Alias("t1").Seal()
				return Select(
					table1.Columns("cost", "amount")...,
				).
					From(table1).Offset(10).Limit(20)
			},
			wantSql: `SELECT t1.cost, t1.amount
FROM table1 AS t1
OFFSET 10 LIMIT 20
`,
			wantArgs: nil,
		},
		{
			name: "select exists from one table",
			builder: func() *SqlBuilder {
				table1 := UseTable[testStruct1]().Alias("t1").Seal()
				return SelectExists().
					From(table1).
					Where(table1.Col("pk1"), "=", 2)
			},
			wantSql: `SELECT EXISTS(SELECT 1 FROM table1 AS t1
WHERE t1.pk1 = 2
)`,
			wantArgs: nil,
		},
		{
			name: "select count from one table",
			builder: func() *SqlBuilder {
				table1 := UseTable[testStruct1]().Alias("t1").Seal()
				return SelectCount().
					From(table1).
					Where(table1.Col("pk1"), "=", 2)
			},
			wantSql: `SELECT COUNT(1) FROM table1 AS t1
WHERE t1.pk1 = 2
`,
			wantArgs: nil,
		},
		{
			name: "multi-operations",
			builder: func() *SqlBuilder {
				table1 := UseTable[testStruct1]().Alias("t1").Seal()
				table2 := UseTable[testStruct2]().Alias("t2").Seal()
				return Select(
					table1.Columns("pk1", "pk2", "cost")...,
				).Select(
					table2.Columns("pk1", "pk2", "pk3", "amount")...,
				).
					From(table1).
					Join(
						LeftJoin, table2,
						table1.Col("pk1"), table2.Col("pk1"),
						table1.Col("pk2"), table2.Col("pk2"),
					).
					Where(table1.Col("pk1"), "= $1").
					Or(table1.Col("pk2"), "= $2").
					And(table1.Col("pk2"), "= 3").
					Args(1, 2).
					OrderBy(table1.Col("cost"), DESC).
					ThenBy(table2.Col("pk3"), ASC).
					Offset(10).Limit(20)
			},
			wantSql: `SELECT t1.pk1, t1.pk2, t1.cost, t2.pk1, t2.pk2, t2.pk3, t2.amount
FROM table1 AS t1
LEFT JOIN table2 AS t2 ON t1.pk1 = t2.pk1 AND t1.pk2 = t2.pk2
WHERE t1.pk1 = $1 OR t1.pk2 = $2 AND t1.pk2 = 3
ORDER BY t1.cost DESC, t2.pk3 ASC
OFFSET 10 LIMIT 20
`,
			wantArgs: []any{1, 2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSql, gotArgs := tt.builder().buildSelect()
			require.Equal(t, tt.wantSql, gotSql)
			require.Equal(t, tt.wantArgs, gotArgs)
		})
	}
}

//goland:noinspection SqlNoDataSourceInspection
func TestSqlBuilder2_buildInsert(t *testing.T) {
	tests := []struct {
		name     string
		builder  func() *SqlBuilder
		wantSql  string
		wantArgs []any
	}{
		{
			name: "INSERT INTO TABLE basic",
			builder: func() *SqlBuilder {
				table1 := UseTable[testStruct1]().Seal()
				return InsertInto(table1).Values(testStruct1{
					Pk1:    "1",
					Pk2:    2,
					Amount: 3,
					Cost: Money{
						Currency: "testa",
						Amount:   4,
					},
				})
			},
			wantSql: `INSERT INTO table1 (pk1, pk2, amount, cost)
VALUES ($1,$2,$3,$4)`,
			wantArgs: []any{"1", 2, 3, "4testa"},
		},
		{
			name: "INSERT INTO TABLE basic with limited volumns",
			builder: func() *SqlBuilder {
				table1 := UseTable[testStruct1]().Seal()
				return InsertInto(table1, table1.Col("pk1"), table1.Col("pk2")).
					Values(testStruct1{
						Pk1:    "1",
						Pk2:    2,
						Amount: 3,
						Cost: Money{
							Currency: "testa",
							Amount:   4,
						},
					})
			},
			wantSql: `INSERT INTO table1 (pk1, pk2)
VALUES ($1,$2)`,
			wantArgs: []any{"1", 2},
		},
		{
			name: "INSERT INTO TABLE basic with multiple records",
			builder: func() *SqlBuilder {
				table1 := UseTable[testStruct1]().Seal()
				return InsertInto(table1).Values(testStruct1{
					Pk1:    "1",
					Pk2:    2,
					Amount: 3,
					Cost: Money{
						Currency: "testa",
						Amount:   4,
					},
				}, testStruct1{
					Pk1:    "5",
					Pk2:    6,
					Amount: 7,
					Cost: Money{
						Currency: "testa",
						Amount:   8,
					},
				})
			},
			wantSql: `INSERT INTO table1 (pk1, pk2, amount, cost)
VALUES ($1,$2,$3,$4),($5,$6,$7,$8)`,
			wantArgs: []any{"1", 2, 3, "4testa", "5", 6, 7, "8testa"},
		},
		{
			name: "INSERT INTO TABLE basic with multiple records",
			builder: func() *SqlBuilder {
				table1 := UseTable[testStruct1]().Seal()
				return InsertInto(table1).Values(testStruct1{
					Pk1:    "1",
					Pk2:    2,
					Amount: 3,
					Cost: Money{
						Currency: "testa",
						Amount:   4,
					},
				}, testStruct1{
					Pk1:    "5",
					Pk2:    6,
					Amount: 7,
					Cost: Money{
						Currency: "testa",
						Amount:   8,
					},
				})
			},
			wantSql: `INSERT INTO table1 (pk1, pk2, amount, cost)
VALUES ($1,$2,$3,$4),($5,$6,$7,$8)`,
			wantArgs: []any{"1", 2, 3, "4testa", "5", 6, 7, "8testa"},
		},
		{
			name: "INSERT INTO TABLE ON CONFLICT DO NOTHING",
			builder: func() *SqlBuilder {
				table1 := UseTable[testStruct1]().Seal()
				return InsertInto(table1).Values(testStruct1{
					Pk1:    "1",
					Pk2:    2,
					Amount: 3,
					Cost: Money{
						Currency: "testa",
						Amount:   4,
					},
				}, testStruct1{
					Pk1:    "5",
					Pk2:    6,
					Amount: 7,
					Cost: Money{
						Currency: "testa",
						Amount:   8,
					},
				}).
					OnConflict().DoNothing()
			},
			wantSql: `INSERT INTO table1 (pk1, pk2, amount, cost)
VALUES ($1,$2,$3,$4),($5,$6,$7,$8)
ON CONFLICT DO NOTHING`,
			wantArgs: []any{"1", 2, 3, "4testa", "5", 6, 7, "8testa"},
		},
		{
			name: "INSERT INTO TABLE ON CONFLICT DO UPDATE",
			builder: func() *SqlBuilder {
				table1 := UseTable[testStruct1]().Seal()
				return InsertInto(table1).Values(testStruct1{
					Pk1:    "1",
					Pk2:    2,
					Amount: 3,
					Cost: Money{
						Currency: "testa",
						Amount:   4,
					},
				}, testStruct1{
					Pk1:    "5",
					Pk2:    6,
					Amount: 7,
					Cost: Money{
						Currency: "testa",
						Amount:   8,
					},
				}).
					OnConflict(table1.Col("pk1"), table1.Col("pk2")).
					DoUpdate(table1.Col("amount"), "=", table1.Col("amount").Excluded()).
					DoUpdate(table1.Col("cost"), "=", table1.Col("cost").Excluded())
			},
			wantSql: `INSERT INTO table1 (pk1, pk2, amount, cost)
VALUES ($1,$2,$3,$4),($5,$6,$7,$8)
ON CONFLICT (pk1, pk2) DO UPDATE SET
 amount = excluded.amount , cost = excluded.cost`,
			wantArgs: []any{"1", 2, 3, "4testa", "5", 6, 7, "8testa"},
		},
		{
			name: "INSERT INTO TABLE ON CONFLICT DO UPDATE except PKs",
			builder: func() *SqlBuilder {
				table1 := UseTable[testStruct1]().Seal()
				return InsertInto(table1).Values(testStruct1{
					Pk1:    "1",
					Pk2:    2,
					Amount: 3,
					Cost: Money{
						Currency: "testa",
						Amount:   4,
					},
				}, testStruct1{
					Pk1:    "5",
					Pk2:    6,
					Amount: 7,
					Cost: Money{
						Currency: "testa",
						Amount:   8,
					},
				}).
					OnConflict(table1.Col("pk1"), table1.Col("pk2")).
					DoUpdateExceptPrimaryKeys()
			},
			wantSql: `INSERT INTO table1 (pk1, pk2, amount, cost)
VALUES ($1,$2,$3,$4),($5,$6,$7,$8)
ON CONFLICT (pk1, pk2) DO UPDATE SET
 amount = excluded.amount , cost = excluded.cost`,
			wantArgs: []any{"1", 2, 3, "4testa", "5", 6, 7, "8testa"},
		},
		{
			name: "INSERT INTO TABLE ON CONFLICT DO UPDATE WHERE",
			builder: func() *SqlBuilder {
				table1 := UseTable[testStruct1]().Seal()
				return InsertInto(table1).Values(testStruct1{
					Pk1:    "1",
					Pk2:    2,
					Amount: 3,
					Cost: Money{
						Currency: "testa",
						Amount:   4,
					},
				}, testStruct1{
					Pk1:    "5",
					Pk2:    6,
					Amount: 7,
					Cost: Money{
						Currency: "testa",
						Amount:   8,
					},
				}).
					OnConflict(table1.PrimaryKeyColumns()...).
					DoUpdate(table1.Col("amount"), "=", table1.Col("amount").Excluded()).
					DoUpdate(table1.Col("cost"), "=", table1.Col("cost").Excluded()).
					Where(table1.Col("cost"), ">", table1.Col("cost").Excluded())
			},
			wantSql: `INSERT INTO table1 (pk1, pk2, amount, cost)
VALUES ($1,$2,$3,$4),($5,$6,$7,$8)
ON CONFLICT (pk1, pk2) DO UPDATE SET
 amount = excluded.amount , cost = excluded.cost
WHERE table1.cost > excluded.cost`,
			wantArgs: []any{"1", 2, 3, "4testa", "5", 6, 7, "8testa"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSql, gotArgs := tt.builder().buildInsert()
			require.Equal(t, tt.wantSql, gotSql)
			require.Equal(t, tt.wantArgs, gotArgs)
		})
	}
}

func TestSqlBuilder_registerUsingTable(t *testing.T) {
	sb := &SqlBuilder{
		aliasToTableUniqueId: make(map[string]int64),
		tableUniqueIdToAlias: make(map[int64]string),
	}

	table1 := UseTable[testStruct1]().Alias("t1").Seal()
	table2 := UseTable[testStruct2]().Alias("t2").Seal()

	sb.registerUsingTable(table1)

	tests := []struct {
		name      string
		table     GenericTableToUse
		wantPanic bool
	}{
		{
			name:  "pass - can register",
			table: table1,
		},
		{
			name:  "pass - can register another table",
			table: table2,
		},
		{
			name:  "pass - can register another instance of the same table",
			table: UseTable[testStruct1]().Alias("still1").Seal(),
		},
		{
			name:      "fail - reject same table if alias taken",
			table:     UseTable[testStruct1]().Alias(table1.tableAlias()).Seal(),
			wantPanic: true,
		},
		{
			name:      "fail - reject table if alias taken",
			table:     UseTable[testStruct2]().Alias(table1.tableAlias()).Seal(),
			wantPanic: true,
		},
		{
			name:      "fail - reject another instance with same alias",
			table:     UseTable[testStruct1]().Alias(table1.tableAlias()).Seal(),
			wantPanic: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantPanic {
				require.Panics(t, func() {
					sb.registerUsingTable(tt.table)
				})
				return
			}

			sb.registerUsingTable(tt.table)
		})
	}
}
