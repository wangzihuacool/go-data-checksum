# go-data-check 数据核对工具

## DESCRIPTION
go-data-checksum is a high-performance data check tool to verify data integrity between MySQL databases/tables. go-data-checksum supports full data check via primary key and incremental data check via specified time field; supports full field check or specified field check also.

go-data-checksum是一款高性能的MySQL数据库/表数据核对工具。go-data-checksum 可以支持按照主键的全量数据核对，和按照时间字段的增量数据核对；可以支持全字段核对或者指定字段核对。
go-data-checksum 可以支持跨MySQL实例的多表并行核对，并且支持目标表的数据是源表的超集的场景。核对原理为，计算并比较待核对数据的CRC32值。

## BUILD
```
go build go-data-checksum
```

## USAGE
```
# 使用帮助
go --help

  -check-column-names string
        Column names to check,eg: col1,col2,col3. By default, all columns are used.
  -chunk-size int
        amount of rows to handle in each iteration (allowed range: 10-100,000) (default 1000)
  -conn-db-timeout int
        connect db timeout (default 30)
  -debug
        debug mode (very verbose)
  -default-retries int
        Default number of retries for various operations before panicking (default 10)
  -ignore-row-count-check
        Shall we ignore check by counting rows? Default: false
  -is-superset-as-equal
        Shall we think that the records in target table is the superset of the source as equal? By default, we think the records are exactly equal as equal.
  -logfile string
        Log file name.
  -source-db-host string
        Source MySQL hostname (default "127.0.0.1")
  -source-db-name string
        Source database list separated by comma, eg: db1 or db1,db2.
  -source-db-password string
        MySQL password
  -source-db-port int
        Source MySQL port (default 3306)
  -source-db-user string
        MySQL user
  -source-table-name string
        Source tables list separated by comma, eg: table1 or table1,table2.
  -source-table-regexp string
        Source table names regular expression, eg: 'test_[0-9][0-9]\\.test_20.*'
  -specified-time-begin string
        Specified begin time of time column to check.
  -specified-time-column string
        Specified time column for range dataCheck.
  -specified-time-end string
        Specified end time of time column to check.
  -target-database-add-suffix string
        Target database name add a suffix to the source database name.
  -target-database-as-source
        Is target database name as source?  default: true. (default true)
  -target-db-host string
        Target MySQL hostname (default "127.0.0.1")
  -target-db-name string
        Target database list separated by comma, eg: db1 or db1,db2.
  -target-db-password string
        MySQL password
  -target-db-port int
        Target MySQL port (default 3306)
  -target-db-user string
        MySQL user
  -target-table-add-suffix string
        Target table name add a suffix to the source table name.
  -target-table-as-source
        Is target table name as source? default: true. (default true)
  -target-table-name string
        Target tables list separated by comma, eg: table1 or table1,table2.
  -threads int
        Parallel threads of table checksum. (default 1)
  -time-range-per-step duration
        time range per step for specified time column check,default 5m,eg:1h/2m/3s/4ms (default 5m0s)
```

## TEST
```
# 测试语句
./go-data-checksum --source-db-host="1.1.1.1" --source-db-port=3307 --source-db-user="test" --source-db-password="xxxx" --target-db-host="8.8.8.8" --target-db-port=3306 --target-db-user="test" --target-db-password="xxxx" --source-table-regexp="test\.sbtest.*" --ignore-row-count-check --is-superset-as-equal --threads=4
```

