## Simple (Postgres) SQL builder using Go.

It helps:
- Automatically construct INSERT and SELECT queries
- Make it easy to insert multiple records in a single query
- Minimal adjustment needed to existing raw SQL code when update table structure
- Internal development with bulk read/write operations faster.

NOTE: developed for internal use, feel free to use it but no guarantee for stability and maintenance.

### Usage
SQL SELECT
```go
tableTransaction := sqlb.UseTable[types.Transaction]().Seal()

rows, err := sqlb.Select(tableTransaction.Columns()...).
    From(tableTransaction).
    Where(tableTransaction.Col("country"), "= $1").Args(country).
    OrderBy(tableTransaction.Col("creation_date"), sqlb.DESC).
    Query(sqlDB)
if err != nil {
    return nil, err
}

var txs []types.Transaction
for rows.Next() {
    tx := tableTransaction.ReadFromRow(rows)
    txs = append(txs, tx)
}

return txs, nil
```
Beside that, SELECT EXISTS and SELECT COUNT are also supported.

___

SQL INSERT
```go
tableTransaction := sqlb.UseTable[types.Transaction]().Seal()
stmt, params := sqlb.InsertInto(tableTransaction).
    Values(tableTransaction.ValuesToAny(txs)...).
    OnConflict(tableTransaction.PrimaryKeyColumns()...).
    DoUpdate(
        tableTransaction.Col("version").EqualsToCurrent(), "+ 1", ",\n", // increase total count
        tableTransaction.Col("creation_date").FromExcluded()
    ).
	WHERE(tableTransaction.Col("version"), "< $1").Args(100).
    Build()

_, err := c.ExecWithContext(stmt, params...)
if err != nil {
    return errors.Wrap(err, "failed to insert or update transactions")
}
return nil
```

___

### Installation
Require define metadata for tables.
It is complex to define a table metadata, but after this hardest part, usage is very simple.
```go
type testStruct1 struct {
	Pk1    string
	Pk2    int
	Cost   Money // custom type
}

_ = NewTableMetadata[testStruct1]("table1").
    AddColumns(
        NewColumnMetadata[testStruct1]("pk1").
            PrimaryKey(). // pk this column is a part of composite primary key
            InsertSpec(func(b testStruct1) any { // define how to retrieve value from struct for INSERT
                return b.Pk1 // return value from struct
            }).
            SelectSpec(func(b *testStruct1) ResultColumnSelectSpec { // define how to retrieve value from struct for SELECT
                return ResultColumnSelectSpec{
                    ToQueryArg: func() any {
                        return &b.Pk1 // return pointer to value from struct, we are familiar when using raw SQL
                    },
                }
            }),
        NewColumnMetadata[testStruct1]("pk2").
            PrimaryKey(). // pk this column is a part of composite primary key
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
        NewColumnMetadata[testStruct1]("cost").
            InsertSpec(func(b testStruct1) any { // define how to retrieve value from struct for INSERT
                return b.Cost.String() // convert custom type to sql supported type
            }).
            SelectSpec(func(b *testStruct1) ResultColumnSelectSpec { // define how to retrieve value from struct for SELECT
                var rawCost string // define variable to store raw value from SQL
                return ResultColumnSelectSpec{
                    ToQueryArg: func() any {
                        return &rawCost // return pointer to value from struct, to be able to read from SQL exec, we are familiar when using raw SQL
                    },
                    OptionalTransform: func() error { // define how to convert raw value from SQL to custom type
                        cost, err := calc.ParseCurrencyBasedCost(rawCost)
                        if err != nil {
                            return errors.Wrapf(err, "failed to convert cost: %s", rawCost)
                        }
						b.Cost = cost // assign converted value to struct
                        return nil
                    },
                }
            }),
        ).Build(TableMetadataBuildOption{
            ExpectedPkColumns: []string{"pk1", "pk2"}, // double check primary key are defined correctly
        })
```
