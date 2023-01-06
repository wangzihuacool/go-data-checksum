/*
@Author: wangzihuacool
@Date: 2022-08-28
*/

package logic

import (
	gosql "database/sql"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"
)

type ChecksumContext struct {
	CheckColumns                    *ColumnList
	UniqueKey                       *ColumnList
	UniqueIndexName                 string
	TimeColumn                      *ColumnList
	UniqueKeyRangeMinValues         *ColumnValues
	UniqueKeyRangeMaxValues         *ColumnValues
	TimeIterationRangeMinValue      time.Time
	TimeIterationRangeMaxValue      time.Time
	ChecksumIterationRangeMinValues *ColumnValues
	ChecksumIterationRangeMaxValues *ColumnValues
	Context                         *BaseContext
	PerTableContext                 *TableContext
}

func NewChecksumContext(context *BaseContext, perTableContext *TableContext) *ChecksumContext {
	return &ChecksumContext{
		Context:         context,
		PerTableContext: perTableContext,
	}
}

// GetIteration 获取当前核对批次
func (this *ChecksumContext) GetIteration() int64 {
	return atomic.LoadInt64(&this.PerTableContext.Iteration)
}

// GetChunkSize 获取chunk大小
func (this *ChecksumContext) GetChunkSize() int64 {
	return atomic.LoadInt64(&this.Context.ChunkSize)
}

// GetCheckColumns investigates a table and returns the list of columns candidate for calculating checksum. default all columns.
func (this *ChecksumContext) GetCheckColumns() (err error) {
	if this.Context.RequestedColumnNames != "" {
		this.CheckColumns = ParseColumnList(this.Context.RequestedColumnNames)
		return nil
	}

	// GROUP_CONCAT()函数默认只能返回1024字节，如果表字段过多可能超过这个限制，需要先设置会话级参数 SET SESSION group_concat_max_len = 10240;
	// GO的MySQL驱动不能保证前后两个查询是在同一个session，除非使用transaction。
	query := `
    select 
      GROUP_CONCAT(COLUMN_NAME ORDER BY ORDINAL_POSITION ASC) AS COLUMN_NAMES
      from information_schema.columns 
     where table_schema= ? and table_name = ?
    order by ORDINAL_POSITION ASC
  `
	err = func() error {
		trx, err := this.Context.SourceDB.Begin()
		if err != nil {
			return err
		}
		defer trx.Rollback()
		groupConcatMaxLength := 10240
		sessionQuery := fmt.Sprintf(`SET SESSION group_concat_max_len = %d`, groupConcatMaxLength)
		if _, err := trx.Exec(sessionQuery); err != nil {
			return err
		}

		var columnNames string
		if err := trx.QueryRow(query, this.PerTableContext.SourceDatabaseName, this.PerTableContext.SourceTableName).Scan(&columnNames); err != nil {
			this.Context.Log.Errorf("Critical: table %s.%s get CheckColumns failed.\n", this.PerTableContext.SourceDatabaseName, this.PerTableContext.SourceTableName)
			return err
		}
		this.Context.Log.Debugf("Debug: table %s.%s CheckColumns are %s\n", this.PerTableContext.SourceDatabaseName, this.PerTableContext.SourceTableName, columnNames)
		this.CheckColumns = ParseColumnList(columnNames)
		if err := trx.Commit(); err != nil {
			return err
		}
		return nil
	}()
	return nil
}

// GetUniqueKeys investigates a table and returns the list of unique keys
// candidate for chunking
func (this *ChecksumContext) GetUniqueKeys() (err error) {
	query := `
    SELECT
      UNIQUES.INDEX_NAME,
      UNIQUES.FIRST_COLUMN_NAME,
      UNIQUES.COLUMN_NAMES,
      UNIQUES.COUNT_COLUMN_IN_INDEX,
      COLUMNS.DATA_TYPE,
      IFNULL(COLUMNS.CHARACTER_SET_NAME, '') as CHARACTER_SET_NAME,
	  has_nullable
    FROM INFORMATION_SCHEMA.COLUMNS INNER JOIN (
      SELECT
        TABLE_SCHEMA,
        TABLE_NAME,
        INDEX_NAME,
        COUNT(*) AS COUNT_COLUMN_IN_INDEX,
        GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX ASC) AS COLUMN_NAMES,
        SUBSTRING_INDEX(GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX ASC), ',', 1) AS FIRST_COLUMN_NAME,
        SUM(NULLABLE='YES') > 0 AS has_nullable
      FROM INFORMATION_SCHEMA.STATISTICS
      WHERE
				NON_UNIQUE=0
				AND TABLE_SCHEMA = ?
      	AND TABLE_NAME = ?
      GROUP BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME
    ) AS UNIQUES
    ON (
      COLUMNS.COLUMN_NAME = UNIQUES.FIRST_COLUMN_NAME
    )
    WHERE
      COLUMNS.TABLE_SCHEMA = ?
      AND COLUMNS.TABLE_NAME = ?
    ORDER BY
      COLUMNS.TABLE_SCHEMA, COLUMNS.TABLE_NAME,
      CASE UNIQUES.INDEX_NAME
        WHEN 'PRIMARY' THEN 0
        ELSE 1
      END,
      CASE has_nullable
        WHEN 0 THEN 0
        ELSE 1
      END,
      CASE IFNULL(CHARACTER_SET_NAME, '')
          WHEN '' THEN 0
          ELSE 1
      END,
      CASE DATA_TYPE
        WHEN 'tinyint' THEN 0
        WHEN 'smallint' THEN 1
        WHEN 'int' THEN 2
        WHEN 'bigint' THEN 3
        ELSE 100
      END,
      COUNT_COLUMN_IN_INDEX
    limit 1
  `

	var indexName string
	var firstColumnName string
	var columnNames string
	var countColumninIndex int
	var dataType string
	var characterSetName string
	var hasNullable bool

	if err := this.Context.SourceDB.QueryRow(query, this.PerTableContext.SourceDatabaseName, this.PerTableContext.SourceTableName, this.PerTableContext.SourceDatabaseName, this.PerTableContext.SourceTableName).Scan(&indexName, &firstColumnName, &columnNames, &countColumninIndex, &dataType, &characterSetName, &hasNullable); err != nil {
		return fmt.Errorf("critical: table %s.%s get uniqueKey failed", this.PerTableContext.SourceDatabaseName, this.PerTableContext.SourceTableName)
	}
	// fmt.Printf("%s, %s, %s, %d, %s, %s, %t\n", indexName, firstColumnName, columnNames, countColumninIndex, dataType, characterSetName, hasNullable)
	if hasNullable {
		return fmt.Errorf("critical: table %s.%s got an uniqueKey with null values", this.PerTableContext.SourceDatabaseName, this.PerTableContext.SourceTableName)
	}
	this.Context.Log.Debugf("Debug: UniqueKeys of source table: %s.%s is %s", this.PerTableContext.SourceDatabaseName, this.PerTableContext.SourceTableName, columnNames)
	this.UniqueKey = ParseColumnList(columnNames)
	this.UniqueIndexName = indexName
	return nil
}

// ReadUniqueKeyRangeMinValues returns the minimum values to be iterated on checksum
func (this *ChecksumContext) ReadUniqueKeyRangeMinValues() (err error) {
	// 构造获取唯一键最小值的SQL
	query, err := BuildUniqueKeyMinValuesPreparedQuery(this.PerTableContext.SourceDatabaseName, this.PerTableContext.SourceTableName, this.UniqueKey)
	if err != nil {
		return err
	}
	rows, err := this.Context.SourceDB.Query(query)
	if err != nil {
		return err
	}
	// UniqueKeyRangeMinValues 为 ColumnValues结构体
	this.UniqueKeyRangeMinValues = NewColumnValues(len(this.UniqueKey.columns))
	for rows.Next() {
		// SQL查询结构赋值给UniqueKeyRangeMinValues
		if err = rows.Scan(this.UniqueKeyRangeMinValues.ValuesPointers...); err != nil {
			return err
		}
	}
	this.Context.Log.Debugf("Debug: UniqueKey min values: [%s] of source table: %s.%s", this.UniqueKeyRangeMinValues, this.PerTableContext.SourceDatabaseName, this.PerTableContext.SourceTableName)
	return err
}

// ReadUniqueKeyRangeMaxValues returns the maximum values to be iterated on checksum
func (this *ChecksumContext) ReadUniqueKeyRangeMaxValues() (err error) {
	// 构造获取唯一键最小值的SQL
	query, err := BuildUniqueKeyMaxValuesPreparedQuery(this.PerTableContext.SourceDatabaseName, this.PerTableContext.SourceTableName, this.UniqueKey)
	if err != nil {
		return err
	}
	rows, err := this.Context.SourceDB.Query(query)
	if err != nil {
		return err
	}
	// UniqueKeyRangeMinValues 为 ColumnValues结构体
	this.UniqueKeyRangeMaxValues = NewColumnValues(len(this.UniqueKey.columns))
	for rows.Next() {
		// SQL查询结构赋值给UniqueKeyRangeMinValues
		if err = rows.Scan(this.UniqueKeyRangeMaxValues.ValuesPointers...); err != nil {
			return err
		}
	}
	this.Context.Log.Debugf("Debug: UniqueKey max values: [%s] of source table: %s.%s", this.UniqueKeyRangeMaxValues, this.PerTableContext.SourceDatabaseName, this.PerTableContext.SourceTableName)
	return err
}

// CalculateNextIterationRangeEndValues 计算下一批次核对的起始值
func (this *ChecksumContext) CalculateNextIterationRangeEndValues() (hasFurtherRange bool, err error) {
	this.ChecksumIterationRangeMinValues = this.ChecksumIterationRangeMaxValues
	if this.ChecksumIterationRangeMinValues == nil {
		this.ChecksumIterationRangeMinValues = this.UniqueKeyRangeMinValues
	}

	// 正常使用BuildUniqueKeyRangeEndPreparedQueryViaOffset返回最大值
	// 最后一批时使用BuildUniqueKeyRangeEndPreparedQueryViaOffset返回值为空，进入第二次循环，通过BuildUniqueKeyRangeEndPreparedQueryViaTemptable查询最大值
	for i := 0; i < 2; i++ {
		// 构造分批范围的分批下限值查询SQL
		buildFunc := BuildUniqueKeyRangeEndPreparedQueryViaOffset
		if i == 1 {
			// BuildUniqueKeyRangeEndPreparedQueryViaTemptable 构造分批范围的分批下限值查询SQL，与BuildUniqueKeyRangeEndPreparedQueryViaOffset稍有差异
			buildFunc = BuildUniqueKeyRangeEndPreparedQueryViaTemptable
		}
		query, explodedArgs, err := buildFunc(
			this.PerTableContext.SourceDatabaseName,
			this.PerTableContext.SourceTableName,
			this.UniqueKey,
			this.ChecksumIterationRangeMinValues.AbstractValues(),
			this.UniqueKeyRangeMaxValues.AbstractValues(),
			atomic.LoadInt64(&this.Context.ChunkSize),
			this.GetIteration() == 0,
			fmt.Sprintf("iteration:%d", this.GetIteration()),
			this.UniqueIndexName,
		)
		if err != nil {
			return hasFurtherRange, err
		}
		// 实际执行查询
		rows, err := this.Context.SourceDB.Query(query, explodedArgs...)
		if err != nil {
			return hasFurtherRange, err
		}
		// NewColumnValues 将ColumnValues结构体的abstractValues的值复制到ValuesPointers
		iterationRangeMaxValues := NewColumnValues(len(this.UniqueKey.columns))
		// rows.Next() 遍历读取结果期，使用rows.Scan()将结果集存到变量。
		// 结果集(rows)未关闭前，底层的连接处于繁忙状态。当遍历读到最后一条记录时，会发生一个内部EOF错误，自动调用rows.Close()。
		// 但是如果提前退出循环，rows不会关闭，连接不会回到连接池中，连接也不会关闭。所以手动关闭非常重要。rows.Close()可以多次调用，是无害操作。
		for rows.Next() {
			// Scan()函数将查询结果赋值给iterationRangeMaxValues.ValuesPointers...
			if err = rows.Scan(iterationRangeMaxValues.ValuesPointers...); err != nil {
				return hasFurtherRange, err
			}
			hasFurtherRange = true
		}
		if err = rows.Err(); err != nil {
			return hasFurtherRange, err
		}
		// 如果还有下一批次结果，将当前查询结果赋值给context的 MigrationIterationRangeMaxValues
		if hasFurtherRange {
			this.ChecksumIterationRangeMaxValues = iterationRangeMaxValues
			return hasFurtherRange, nil
		}
	}
	this.Context.Log.Debugf("Debug: Iteration complete: no further range to iterate of source table: %s.%s", this.PerTableContext.SourceDatabaseName, this.PerTableContext.SourceTableName)
	return hasFurtherRange, nil
}

// IterationQueryChecksum issues a chunk-Checksum query on the table.
// 1. 批次核对：单个批次内聚合结果CRC32值按位异或结果，计算方式：COALESCE(LOWER(CONV(BIT_XOR(cast(crc32(CONCAT_WS('#',C1,C2,C3,Cn)) as UNSIGNED)), 10, 16)), 0)
// 2. 记录级核对：单个批次内每条记录的CRC32值，判断源端的CRC32是不是目标端的CRC32的子集，计算方式: COALESCE(LOWER(CONV(cast(crc32(CONCAT_WS('#',id, ftime, c1, c2)) as UNSIGNED), 10, 16)), 0)
func (this ChecksumContext) IterationQueryChecksum() (isChunkChecksumEqual bool, duration time.Duration, err error) {
	startTime := time.Now()
	defer func() {
		duration = time.Since(startTime)
	}()

	// 获取ChunkChecksum结果(聚合CRC32XOR 或者 逐行CRC32)
	QueryChecksumFunc := func(db *gosql.DB, databaseName, tableName string, checkLevel int64) (ret []string, err error) {
		query, explodedArgs, err := BuildRangeChecksumPreparedQuery(
			databaseName,
			tableName,
			this.CheckColumns,
			this.UniqueKey,
			this.ChecksumIterationRangeMinValues.AbstractValues(),
			this.ChecksumIterationRangeMaxValues.AbstractValues(),
			this.GetIteration() == 0,
			checkLevel,
		)
		if err != nil {
			return ret, err
		}

		rows, err := db.Query(query, explodedArgs...)
		if err != nil {
			return ret, err
		}
		defer rows.Close()
		for rows.Next() {
			rowValues := NewColumnValues(1)
			if err := rows.Scan(rowValues.ValuesPointers...); err != nil {
				return ret, err
			}
			ret = append(ret, rowValues.StringColumn(0))
		}
		err = rows.Err()
		if err != nil {
			return ret, err
		}
		return ret, nil
	}

	// 判断元素是否在slice中
	ElementInSliceFunc := func(inSlice []string, element string) bool {
		for _, e := range inSlice {
			if e == element {
				return true
			}
		}
		return false
	}

	// 计算CRC32XOR聚合值，还是逐行CRC32值
	var checkLevel int64 = 1
	if this.Context.IsSuperSetAsEqual {
		checkLevel = 2
	}

	var sourceResult []string
	var targetResult []string
	// SourceDB
	if sourceResult, err = QueryChecksumFunc(this.Context.SourceDB, this.PerTableContext.SourceDatabaseName, this.PerTableContext.SourceTableName, checkLevel); err != nil {
		return false, duration, err
	}
	// TargetDB
	if targetResult, err = QueryChecksumFunc(this.Context.TargetDB, this.PerTableContext.TargetDatabaseName, this.PerTableContext.TargetTableName, checkLevel); err != nil {
		return false, duration, err
	}

	atomic.AddInt64(&this.PerTableContext.Iteration, 1)

	if reflect.DeepEqual(sourceResult, targetResult) {
		return true, duration, nil
	} else if checkLevel == 2 {
		for _, v := range sourceResult {
			if isInTarget := ElementInSliceFunc(targetResult, v); isInTarget == false {
				return false, duration, nil
			}
		}
		return true, duration, nil
	}
	return false, duration, nil
}

// DataChecksumByCount 比较源表和目标表的总记录数，如果IsSuperSetAsEqual为false则只有记录数相等才认为核平，否则源表记录数少于等于目标表则认为核平，返回是否核平以及是否需要继续核对
func (this *ChecksumContext) DataChecksumByCount() (isTableCountEqual bool, isMoreCheckNeeded bool, err error) {
	SourceQueryTableCount := fmt.Sprintf("select /* dataChecksum */ count(*) from %s.%s", EscapeName(this.PerTableContext.SourceDatabaseName), EscapeName(this.PerTableContext.SourceTableName))
	TargetQueryTableCount := fmt.Sprintf("select /* dataChecksum */ count(*) from %s.%s", EscapeName(this.PerTableContext.TargetDatabaseName), EscapeName(this.PerTableContext.TargetTableName))
	var sourceRowCount int64
	var targetRowCount int64
	if err = this.Context.SourceDB.QueryRow(SourceQueryTableCount).Scan(&sourceRowCount); err != nil {
		return false, false, fmt.Errorf("critical: Table %s.%s query sourceRowCount failed", this.PerTableContext.SourceDatabaseName, this.PerTableContext.SourceTableName)
	}
	if err = this.Context.TargetDB.QueryRow(TargetQueryTableCount).Scan(&targetRowCount); err != nil {
		return false, false, fmt.Errorf("critical: Table %s.%s query TargetRowCount failed", this.PerTableContext.TargetDatabaseName, this.PerTableContext.TargetTableName)
	}

	if sourceRowCount == targetRowCount {
		this.Context.Log.Debugf("Debug: Record check result (sourceTable %d rows, targetTable %d rows) is equal of table pair: %s.%s => %s.%s. More check needed.", sourceRowCount, targetRowCount, this.PerTableContext.SourceDatabaseName, this.PerTableContext.SourceTableName, this.PerTableContext.TargetDatabaseName, this.PerTableContext.TargetTableName)
		isTableCountEqual = true
		isMoreCheckNeeded = true
	} else if this.Context.IsSuperSetAsEqual && sourceRowCount < targetRowCount {
		this.Context.Log.Debugf("Debugf: Record check result (sourceTable %d rows, targetTable %d rows) is equal of table pair: %s.%s => %s.%s. Need more check due to IsSuperSetAsEqual=%t", sourceRowCount, targetRowCount, this.PerTableContext.SourceDatabaseName, this.PerTableContext.SourceTableName, this.PerTableContext.TargetDatabaseName, this.PerTableContext.TargetTableName, this.Context.IsSuperSetAsEqual)
		isTableCountEqual = false
		isMoreCheckNeeded = true
	} else if this.Context.IsSuperSetAsEqual && sourceRowCount > targetRowCount {
		this.Context.Log.Errorf("Critical: Record check result (sourceTable %d rows, targetTable %d rows) is not equal of table pair: %s.%s => %s.%s.", sourceRowCount, targetRowCount, this.PerTableContext.SourceDatabaseName, this.PerTableContext.SourceTableName, this.PerTableContext.TargetDatabaseName, this.PerTableContext.TargetTableName)
		isTableCountEqual = false
		isMoreCheckNeeded = false
	} else if !this.Context.IsSuperSetAsEqual {
		this.Context.Log.Errorf("Critical: Record check result (sourceTable %d rows, targetTable %d rows) is not equal of table pair: %s.%s => %s.%s.", sourceRowCount, targetRowCount, this.PerTableContext.SourceDatabaseName, this.PerTableContext.SourceTableName, this.PerTableContext.TargetDatabaseName, this.PerTableContext.TargetTableName)
		isTableCountEqual = false
		isMoreCheckNeeded = false
	}

	return isTableCountEqual, isMoreCheckNeeded, nil
}
