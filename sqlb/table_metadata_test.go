package sqlb

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestTableMetadata(t *testing.T) {
	t.Run("NewRow() must return new struct", func(t *testing.T) {
		row := tableTest1.NewRow()
		require.Equal(t, reflect.Struct, reflect.TypeOf(row).Kind())
		require.NotPanics(t, func() {
			row.Amount = 1
			row.Cost = Money{
				Currency: "test",
				Amount:   1,
			}
		})
	})

	t.Run("can set to struct value", func(t *testing.T) {
		columnName, selectAmount := tableTest1.MustGetColumnByName("amount").SelectSpec()
		require.Equal(t, "amount", columnName)

		row := tableTest1.NewRow()
		rs := selectAmount(&row)
		v := rs.ToQueryArg()
		*v.(*int) = 1

		require.Equal(t, 1, row.Amount)
	})

	t.Run("can set & transform to struct value", func(t *testing.T) {
		columnName, selectCost := tableTest1.MustGetColumnByName("cost").SelectSpec()
		require.Equal(t, "cost", columnName)

		row := tableTest1.NewRow()
		rs := selectCost(&row)
		v := rs.ToQueryArg()
		*v.(*string) = "100test"

		err := rs.OptionalTransform()
		require.NoError(t, err)

		require.Equal(t, Money{
			Currency: "test",
			Amount:   100,
		}, row.Cost)
	})
}

func TestGenericTableMetadata(t *testing.T) {
	test := func(t *testing.T, selectAmount ResultColumnSelectSpec, selectCost ResultColumnSelectSpec, valueFunc func() any) {
		v1 := selectAmount.ToQueryArg()
		v2 := selectCost.ToQueryArg()

		*v1.(*int) = 1

		*v2.(*string) = "100test"
		err := selectCost.OptionalTransform()
		require.NoError(t, err)

		row := valueFunc().(testStruct1)
		require.Equal(t, 1, row.Amount)
		require.Equal(t, Money{
			Currency: "test",
			Amount:   100,
		}, row.Cost)
	}

	t.Run("compatible interface", func(t *testing.T) {
		_, ok := any(tableTest1).(genericTableMetadata)
		require.True(t, ok)

		func(tablesMeta ...any) {
			for _, tableMeta := range tablesMeta {
				_, ok := tableMeta.(genericTableMetadata)
				require.True(t, ok)
			}
		}(tableTest1, tableTest1)
	})

	t.Run("can set to struct value (traditional)", func(t *testing.T) {
		row := tableTest1.NewRow()
		_, rsAmount := tableTest1.MustGetColumnByName("amount").SelectSpec()
		_, rsCost := tableTest1.MustGetColumnByName("cost").SelectSpec()

		selectAmount := rsAmount(&row)
		selectCost := rsCost(&row)

		test(t, selectAmount, selectCost, func() any {
			return row
		})
	})

	t.Run("can set to struct value (generic)", func(t *testing.T) {
		gtm := tableTest1.asGeneric()
		kts, selectSpecs := gtm.selectSpecOfColumns("amount", "cost")
		require.Len(t, selectSpecs, 2)

		selectAmount := selectSpecs[0]
		selectCost := selectSpecs[1]

		test(t, selectAmount, selectCost, kts)
	})
}

type Money struct {
	Currency string
	Amount   int64
}

func (c Money) String() string {
	return fmt.Sprintf("%d%s", c.Amount, c.Currency)
}

func parseMoney(moneyStr string) (Money, error) {
	// This is a mock implementation for the sake of the example.
	if moneyStr == "" {
		return Money{}, errors.New("empty money string")
	}
	var money Money
	if _, err := fmt.Sscanf(moneyStr, "%d%s", &money.Amount, &money.Currency); err != nil {
		return Money{}, errors.Wrapf(err, "failed to parse money from string: %s", moneyStr)
	}
	return money, nil
}

type testStruct1 struct {
	Pk1    string
	Pk2    int
	Amount int
	Cost   Money
}

type testStruct2 struct {
	Pk1 string // fk1
	Pk2 int    // fk2
	Pk3 int64

	Amount Money
}

var tableTest1 = NewTableMetadata[testStruct1]("table1").
	AddColumns(
		NewColumnMetadata[testStruct1]("pk1").
			PrimaryKey().
			InsertSpec(func(b testStruct1) any {
				return b.Pk1
			}).
			SelectSpec(func(b *testStruct1) ResultColumnSelectSpec {
				return ResultColumnSelectSpec{
					ToQueryArg: func() any {
						return &b.Pk1
					},
				}
			}),
		NewColumnMetadata[testStruct1]("pk2").
			PrimaryKey().
			InsertSpec(func(b testStruct1) any {
				return b.Pk2
			}).
			SelectSpec(func(b *testStruct1) ResultColumnSelectSpec {
				return ResultColumnSelectSpec{
					ToQueryArg: func() any {
						return &b.Pk2
					},
				}
			}),
		NewColumnMetadata[testStruct1]("amount").
			InsertSpec(func(b testStruct1) any {
				return b.Amount
			}).
			SelectSpec(func(b *testStruct1) ResultColumnSelectSpec {
				return ResultColumnSelectSpec{
					ToQueryArg: func() any {
						return &b.Amount
					},
				}
			}),
		NewColumnMetadata[testStruct1]("cost").
			InsertSpec(func(b testStruct1) any {
				return b.Cost.String()
			}).
			SelectSpec(func(b *testStruct1) ResultColumnSelectSpec {
				var rawCost string
				return ResultColumnSelectSpec{
					ToQueryArg: func() any {
						return &rawCost
					},
					OptionalTransform: func() error {
						var err error
						if b.Cost, err = parseMoney(rawCost); err != nil {
							return errors.Wrapf(err, "failed to convert cost: %s", rawCost)
						}
						return nil
					},
				}
			}),
	).Build(TableMetadataBuildOption{
	ExpectedPkColumns: []string{"pk1", "pk2"},
})

var tableTest2 = NewTableMetadata[testStruct2]("table2").
	AddColumns(
		NewColumnMetadata[testStruct2]("pk1").
			PrimaryKey().
			InsertSpec(func(b testStruct2) any {
				return b.Pk1
			}).
			SelectSpec(func(b *testStruct2) ResultColumnSelectSpec {
				return ResultColumnSelectSpec{
					ToQueryArg: func() any {
						return &b.Pk1
					},
				}
			}),
		NewColumnMetadata[testStruct2]("pk2").
			PrimaryKey().
			InsertSpec(func(b testStruct2) any {
				return b.Pk2
			}).
			SelectSpec(func(b *testStruct2) ResultColumnSelectSpec {
				return ResultColumnSelectSpec{
					ToQueryArg: func() any {
						return &b.Pk2
					},
				}
			}),
		NewColumnMetadata[testStruct2]("pk3").
			PrimaryKey().
			InsertSpec(func(b testStruct2) any {
				return b.Pk3
			}).
			SelectSpec(func(b *testStruct2) ResultColumnSelectSpec {
				return ResultColumnSelectSpec{
					ToQueryArg: func() any {
						return &b.Pk3
					},
				}
			}),
		NewColumnMetadata[testStruct2]("amount").
			InsertSpec(func(b testStruct2) any {
				return b.Amount.String()
			}).
			SelectSpec(func(b *testStruct2) ResultColumnSelectSpec {
				var rawAmount string
				return ResultColumnSelectSpec{
					ToQueryArg: func() any {
						return &rawAmount
					},
					OptionalTransform: func() error {
						var err error
						if b.Amount, err = parseMoney(rawAmount); err != nil {
							return errors.Wrapf(err, "failed to convert cost: %s", rawAmount)
						}
						return nil
					},
				}
			}),
	).Build(TableMetadataBuildOption{
	ExpectedPkColumns: []string{"pk1", "pk2", "pk3"},
})
