# go-data-check 数据核对工具

```
# 使用帮助
go --help
```

```
# 测试语句
./go-data-checksum --source-db-host="9.135.14.173" --source-db-port=3307 --source-db-user="test" --source-db-password="test" --target-db-host="9.135.14.173" --target-db-port=3307 --target-db-user="test" --target-db-password="test" --source-table-regexp="test\.sbtest.*" --ignore-row-count-check --is-superset-as-equal --threads=4
```