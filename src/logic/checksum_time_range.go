/*
@Author: wangzihuacool
@Date: 2022-08-28
*/

package logic

import (
	gosql "database/sql"
	"fmt"
	"reflect"
	"strconv"
	"sync/atomic"
	"time"
)

func (this *ChecksumContext) GetTimeColumn() error {
	query := `
    SELECT
      COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = ?
      AND TABLE_NAME = ?
      AND COLUMN_NAME = ?
      AND DATA_TYPE = 'datetime'
      AND COLUMN_KEY in ('PRI', 'UNI', 'MUL')
    limit 1
    `
	if err := this.Context.SourceDB.QueryRow(query, this.PerTableContext.SourceDatabaseName, this.PerTableContext.SourceTableName, this.Context.SpecifiedDatetimeColumn).Scan(&this.Context.SpecifiedDatetimeColumn); err != nil {
		return fmt.Errorf("critical, getTimeColumn of table %s.%s failed;Please check TimeColumn", this.PerTableContext.SourceDatabaseName, this.PerTableContext.SourceTableName)
	}
	this.TimeColumn = NewColumnList([]string{this.Context.SpecifiedDatetimeColumn})
	this.Context.Log.Debugf("Debug: datetime column of source table: %s.%s is %s", this.PerTableContext.SourceDatabaseName, this.PerTableContext.SourceTableName, this.Context.SpecifiedDatetimeColumn)
	return nil
}

// EstimateTableRowsViaExplain 获取满足TimeRange核对条件的估算行数
func (this *ChecksumContext) EstimateTableRowsViaExplain() (estimatedRows int, err error) {
	query := fmt.Sprintf(`
    EXPLAIN SELECT /* dataChecksum %s.%s */ * 
              FROM %s.%s 
             WHERE (%s >= ? and %s <= ?)   
    `, this.PerTableContext.SourceDatabaseName, this.PerTableContext.SourceTableName,
		this.PerTableContext.SourceDatabaseName, this.PerTableContext.SourceTableName,
		this.Context.SpecifiedDatetimeColumn, this.Context.SpecifiedDatetimeColumn)
	rows, err := this.Context.SourceDB.Query(query, this.Context.SpecifiedDatetimeRangeBegin, this.Context.SpecifiedDatetimeRangeEnd)
	if err != nil {
		return estimatedRows, err
	}
	explainRes := NewColumnValues(12)
	for rows.Next() {
		if err = rows.Scan(explainRes.ValuesPointers...); err != nil {
			return estimatedRows, err
		}
	}
	estimatedRows, err = strconv.Atoi(explainRes.StringColumn(9))
	if err != nil {
		return estimatedRows, err
	}
	return estimatedRows, nil
}

///*// ReadTimeRangeMinValues returns the minimum values to be iterated on checksum
//func (this *ChecksumContext) ReadTimeRangeMinValues() (err error) {
//	// 构造获取唯一键最小值的SQL
//	hint := "QueryTimeRangeMinValues"
//	queryOfMinValue := fmt.Sprintf(`
//				SELECT  /* dataChecksum %s.%s %s */
//						min(%s) as MINVALUE
//                FROM %s.%s
//               WHERE (%s >= ?)
//                 AND (%s <= ?)
//    `, this.PerTableContext.SourceDatabaseName, this.PerTableContext.SourceTableName, hint,
//	    this.Context.SpecifiedDatetimeColumn,
//	    this.PerTableContext.SourceDatabaseName, this.PerTableContext.SourceTableName,
//	    this.Context.SpecifiedDatetimeColumn,
//		this.Context.SpecifiedDatetimeColumn,
//	)
//	rows, err := this.Context.SourceDB.Query(queryOfMinValue, this.Context.SpecifiedDatetimeRangeBegin, this.Context.SpecifiedDatetimeRangeEnd)
//	if err != nil {
//		return err
//	}
//	// TimeRangeMinValues 为 ColumnValues结构体
//	this.TimeRangeMinValues = NewColumnValues(1)
//	for rows.Next() {
//		// SQL查询结构赋值给TimeRangeMinValues
//		if err = rows.Scan(this.TimeRangeMinValues.ValuesPointers...); err != nil {
//			return err
//		}
//	}
//	this.Context.Log.Debugf("Debug: time range column min values: [%s] of source table: %s.%s", this.TimeRangeMinValues, this.PerTableContext.SourceDatabaseName, this.PerTableContext.SourceTableName)
//	return nil
//}
//
//// ReadTimeRangeMaxValues returns the maximum values to be iterated on checksum
//func (this *ChecksumContext) ReadTimeRangeMaxValues() (err error) {
//	// 构造获取唯一键最小值的SQL
//	hint := "QueryTimeRangeMaxValues"
//	queryOfMaxValue := fmt.Sprintf(`
//				SELECT  /* dataChecksum %s.%s %s */
//						max(%s) as MAXVALUE
//                FROM %s.%s
//               WHERE (%s >= ?)
//                 AND (%s <= ?)
//    `, this.PerTableContext.SourceDatabaseName, this.PerTableContext.SourceTableName, hint,
//		this.Context.SpecifiedDatetimeColumn,
//		this.PerTableContext.SourceDatabaseName, this.PerTableContext.SourceTableName,
//		this.Context.SpecifiedDatetimeColumn,
//		this.Context.SpecifiedDatetimeColumn,
//	)
//	rows, err := this.Context.SourceDB.Query(queryOfMaxValue, this.Context.SpecifiedDatetimeRangeBegin, this.Context.SpecifiedDatetimeRangeEnd)
//	if err != nil {
//		return err
//	}
//	// TimeRangeMaxValues 为 ColumnValues结构体
//	this.TimeRangeMaxValues = NewColumnValues(1)
//	for rows.Next() {
//		// SQL查询结构赋值给TimeRangeMaxValues
//		if err = rows.Scan(this.TimeRangeMaxValues.ValuesPointers...); err != nil {
//			return err
//		}
//	}
//	this.Context.Log.Debugf("Debug: time range column max values: [%s] of source table: %s.%s", this.TimeRangeMaxValues, this.PerTableContext.SourceDatabaseName, this.PerTableContext.SourceTableName)
//	return nil
//}
//*/

// CalculateNextIterationTimeRange 计算下一批次核对的起始值
func (this *ChecksumContext) CalculateNextIterationTimeRange() (hasFurtherRange bool, err error) {
	this.TimeIterationRangeMinValue = this.TimeIterationRangeMaxValue
	if this.TimeIterationRangeMinValue.IsZero() {
		this.TimeIterationRangeMinValue = this.Context.SpecifiedDatetimeRangeBegin
	}
	if this.TimeIterationRangeMinValue.After(this.Context.SpecifiedDatetimeRangeEnd) || this.TimeIterationRangeMinValue == this.Context.SpecifiedDatetimeRangeEnd {
		hasFurtherRange = false
		return hasFurtherRange, nil
	}

	this.TimeIterationRangeMaxValue = this.TimeIterationRangeMinValue.Add(this.Context.SpecifiedTimeRangePerStep)
	if this.TimeIterationRangeMaxValue.After(this.Context.SpecifiedDatetimeRangeEnd) {
		this.TimeIterationRangeMaxValue = this.Context.SpecifiedDatetimeRangeEnd
	}
	this.ChecksumIterationRangeMinValues = ToColumnValues([]interface{}{this.TimeIterationRangeMinValue})
	this.ChecksumIterationRangeMaxValues = ToColumnValues([]interface{}{this.TimeIterationRangeMaxValue})
	hasFurtherRange = true
	return hasFurtherRange, nil
}

// IterationTimeRangeQueryChecksum issues a chunk-Checksum query on the table with datetime range.
// 1. 批次核对：单个批次内聚合结果CRC32值按位异或结果，计算方式：COALESCE(LOWER(CONV(BIT_XOR(cast(crc32(CONCAT_WS('#',C1,C2,C3,Cn)) as UNSIGNED)), 10, 16)), 0)
// 2. 记录级核对：单个批次内每条记录的CRC32值，判断源端的CRC32是不是目标端的CRC32的子集，计算方式: COALESCE(LOWER(CONV(cast(crc32(CONCAT_WS('#',id, ftime, c1, c2)) as UNSIGNED), 10, 16)), 0)
func (this ChecksumContext) IterationTimeRangeQueryChecksum() (isChunkChecksumEqual bool, duration time.Duration, err error) {
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
			this.TimeColumn,
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
