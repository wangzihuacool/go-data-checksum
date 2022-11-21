/*
@Author: wangzihuacool
@Date: 2022-08-28
*/

package main

import (
	"flag"
	"fmt"
	"logic"
	"sort"
	"strings"
	"sync"
	"time"
)

// ChecksumJob 用于协程池管理
type ChecksumJob struct {
	ChecksumJobChan chan int
	wg              *sync.WaitGroup
}

func NewChecksumJob(threads int) *ChecksumJob {
	return &ChecksumJob{
		ChecksumJobChan: make(chan int, threads),
		wg:              &sync.WaitGroup{},
	}
}

// GenerateTableList 生成源表和目标表对应关系
func GenerateTableList(baseContext *logic.BaseContext) (err error) {
	queryWithDatabase := func(databases []string, tablesRegexp string, hint string) (tablesList []string, err error) {
		var query string
		if hint == "QueryTableNameWithDatabase" {
			query = fmt.Sprintf(`
              select concat(table_schema, '.', table_name) as table_name
                from information_schema.tables
               where table_schema in ("%s")
               order by 1
            `, strings.Join(databases, ", "),
			)
		} else if hint == "QueryTableNameWithRegexp" {
			query = fmt.Sprintf(`
              select concat(table_schema, '.', table_name) as table_name
                from information_schema.tables
               where concat(table_schema, '.', table_name) regexp "%s"
               order by 1
            `, tablesRegexp)
		}
		rows, err := baseContext.SourceDB.Query(query)
		if err != nil {
			return tablesList, err
		}
		defer rows.Close()
		for rows.Next() {
			rowValues := logic.NewColumnValues(1)
			if err := rows.Scan(rowValues.ValuesPointers...); err != nil {
				return tablesList, err
			}
			tablesList = append(tablesList, rowValues.StringColumn(0))
		}
		err = rows.Err()
		if err != nil {
			return tablesList, err
		}
		return tablesList, nil
	}

	// 指定源端库表名
	if baseContext.SourceDatabases != "" && baseContext.SourceTables != "" {
		baseContext.SourceDatabaseList = strings.Split(baseContext.SourceDatabases, ",")
		baseContext.SourceTableList = strings.Split(baseContext.SourceTables, ",")
		for _, databaseName := range baseContext.SourceDatabaseList {
			for _, tableName := range baseContext.SourceTableList {
				baseContext.SourceTableFullNameList = append(baseContext.SourceTableFullNameList, fmt.Sprintf("%s.%s", databaseName, tableName))
			}
		}
	} else if baseContext.SourceDatabases != "" && baseContext.SourceTables == "" {
		// 指定源库的所有表
		baseContext.SourceDatabaseList = strings.Split(baseContext.SourceDatabases, ",")
		baseContext.TableQueryHint = "QueryTableNameWithDatabase"
	} else if baseContext.SourceTableNameRegexp != "" {
		// 源表正则匹配
		baseContext.TableQueryHint = "QueryTableNameWithRegexp"
	}

	if baseContext.SourceTableFullNameList == nil {
		if baseContext.SourceTableFullNameList, err = queryWithDatabase(baseContext.SourceDatabaseList, baseContext.SourceTableNameRegexp, baseContext.TableQueryHint); err != nil {
			return fmt.Errorf("critical: Get source table names failed. Please check arguments")
		}
		if baseContext.SourceTableFullNameList == nil {
			return fmt.Errorf("get source table names failed. Please check arguments")
		}
	}

	baseContext.PairOfSourceAndTargetTables = make(map[string]string, len(baseContext.SourceTableFullNameList))
	if baseContext.TargetDatabases != "" && baseContext.TargetTables != "" {
		// 指定目标库表
		baseContext.TargetDatabaseList = strings.Split(baseContext.TargetDatabases, ",")
		baseContext.TargetTableList = strings.Split(baseContext.TargetTables, ",")
		for _, databaseName := range baseContext.TargetDatabaseList {
			for _, tableName := range baseContext.TargetTableList {
				baseContext.TargetTableFullNameList = append(baseContext.TargetTableFullNameList, fmt.Sprintf("%s.%s", databaseName, tableName))
			}
		}
		if len(baseContext.SourceTableFullNameList) == len(baseContext.TargetTableFullNameList) {
			for i, tableName := range baseContext.SourceTableFullNameList {
				baseContext.PairOfSourceAndTargetTables[tableName] = baseContext.TargetTableFullNameList[i]
			}
		}
	} else if baseContext.TargetDatabaseAddSuffix != "" && baseContext.TargetTableAddSuffix != "" {
		// 目标库表名加后缀
		for _, tableName := range baseContext.SourceTableFullNameList {
			sourceDatabaseName := strings.Split(tableName, ".")[0]
			sourceTableName := strings.Split(tableName, ".")[1]
			targetTableName := fmt.Sprintf("%s.%s", sourceDatabaseName+baseContext.TargetDatabaseAddSuffix, sourceTableName+baseContext.TargetTableAddSuffix)
			baseContext.PairOfSourceAndTargetTables[tableName] = targetTableName
		}
	} else if baseContext.TargetDatabaseAsSource && baseContext.TargetTableAddSuffix != "" {
		// 目标库名相同，表名加后缀
		for _, tableName := range baseContext.SourceTableFullNameList {
			sourceDatabaseName := strings.Split(tableName, ".")[0]
			sourceTableName := strings.Split(tableName, ".")[1]
			targetTableName := fmt.Sprintf("%s.%s", sourceDatabaseName, sourceTableName+baseContext.TargetTableAddSuffix)
			baseContext.PairOfSourceAndTargetTables[tableName] = targetTableName
		}
	} else if baseContext.TargetDatabaseAddSuffix != "" && baseContext.TargetTableAsSource {
		// 目标库名加后缀，表名相同
		for _, tableName := range baseContext.SourceTableFullNameList {
			sourceDatabaseName := strings.Split(tableName, ".")[0]
			sourceTableName := strings.Split(tableName, ".")[1]
			targetTableName := fmt.Sprintf("%s.%s", sourceDatabaseName+baseContext.TargetDatabaseAddSuffix, sourceTableName)
			baseContext.PairOfSourceAndTargetTables[tableName] = targetTableName
		}
	} else if baseContext.TargetDatabaseAsSource && baseContext.TargetTableAsSource {
		// 目标库表名与源库表一致
		for _, tableName := range baseContext.SourceTableFullNameList {
			targetTableName := tableName
			baseContext.PairOfSourceAndTargetTables[tableName] = targetTableName
		}
	}

	if baseContext.PairOfSourceAndTargetTables == nil {
		return err
	}

	return nil
}

// ChecksumPerTable 先判断总记录数，然后逐个分片判断checksum值
func (this *ChecksumJob) ChecksumPerTable(baseContext *logic.BaseContext, tableContext *logic.TableContext) (err error) {
	startTime := time.Now()
	var tableCheckDuration time.Duration
	defer func() {
		<-this.ChecksumJobChan
		this.wg.Done()
	}()

	ChecksumContext := logic.NewChecksumContext(baseContext, tableContext)
	baseContext.Log.Infof("Starting check table pair: %s.%s => %s.%s .", ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName)

	// 先核对全表count(*)值是否一致
	if !baseContext.IgnoreRowCountCheck {
		baseContext.Log.Debugf("DataChecksumByCount of table pair: %s.%s => %s.%s .", ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName)
		_, isMoreCheckNeeded, err := ChecksumContext.DataChecksumByCount()
		if err != nil {
			baseContext.ChecksumErrChan <- err
		}
		if !isMoreCheckNeeded {
			baseContext.ChecksumErrChan <- nil
			baseContext.ChecksumResChan <- false
			return nil
		}
	} else {
		baseContext.Log.Debugf("Ignore DataChecksumByCount of table pair: %s.%s => %s.%s due to IgnoreRowCountCheck=true.", ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName)
	}

	// 判断用户有没有输入checkcolumn，没有则取全表所有字段作为核对字段
	baseContext.Log.Debugf("Get user-request check columns of table pair: %s.%s => %s.%s .", ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName)
	if ChecksumContext.CheckColumns == nil {
		if err := ChecksumContext.GetCheckColumns(); err != nil {
			baseContext.ChecksumErrChan <- err
			baseContext.ChecksumResChan <- false
		}
	}

	// 获取唯一键、最大最小值
	baseContext.Log.Debugf("GetUniqueKeys of table pair: %s.%s => %s.%s .", ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName)
	if err := ChecksumContext.GetUniqueKeys(); err != nil {
		baseContext.ChecksumErrChan <- err
		baseContext.ChecksumResChan <- false
		return err
	}
	baseContext.Log.Debugf("ReadUniqueKeyRangeMinValues of table pair: %s.%s => %s.%s .", ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName)
	if err := ChecksumContext.ReadUniqueKeyRangeMinValues(); err != nil {
		baseContext.ChecksumErrChan <- err
		baseContext.ChecksumResChan <- false
		return err
	}
	baseContext.Log.Debugf("ReadUniqueKeyRangeMaxValues of table pair: %s.%s => %s.%s .", ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName)
	if err := ChecksumContext.ReadUniqueKeyRangeMaxValues(); err != nil {
		baseContext.ChecksumErrChan <- err
		baseContext.ChecksumResChan <- false
		return err
	}

	// 计算checksum值
	var hasFurtherRange = true
	for hasFurtherRange {
		hasFurtherRange, err = ChecksumContext.CalculateNextIterationRangeEndValues()
		baseContext.Log.Debugf("CalculateNextIterationRangeEndValues of table pair: %s.%s => %s.%s .", ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName)

		var isChunkChecksumEqual bool
		var duration time.Duration
		if hasFurtherRange {
			// 增加重试机制
			for i := 0; i < int(ChecksumContext.Context.DefaultNumRetries); i++ {
				if i != 0 {
					time.Sleep(3 * time.Second)
					baseContext.Log.Debugf("IterationQueryChecksum [%s-%s] retry times %d of table pair: %s.%s => %s.%s .", ChecksumContext.ChecksumIterationRangeMinValues.AbstractValues(), ChecksumContext.ChecksumIterationRangeMaxValues.AbstractValues(), i, ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName)
				} else {
					baseContext.Log.Debugf("IterationQueryChecksum [%s-%s] of table pair: %s.%s => %s.%s .", ChecksumContext.ChecksumIterationRangeMinValues.AbstractValues(), ChecksumContext.ChecksumIterationRangeMaxValues.AbstractValues(), ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName)
				}
				isChunkChecksumEqual, duration, err = ChecksumContext.IterationQueryChecksum()
				if err == nil {
					break
				}
			}
			if err != nil {
				baseContext.ChecksumErrChan <- err
				baseContext.ChecksumResChan <- false
				tableCheckDuration = time.Since(startTime)
				baseContext.Log.Debugf("Debug: Iteration %d, record CRC32 checksum value is not equal of table pair: %s.%s => %s.%s , Duration=%+v", int(ChecksumContext.GetIteration()), ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName, duration)
				baseContext.Log.Errorf("Critical: record CRC32 checksum value is not equal of table pair: %s.%s => %s.%s , tableCheckDuration=%+v", ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName, tableCheckDuration)
				return err
			}
			if isChunkChecksumEqual == false {
				baseContext.ChecksumErrChan <- nil
				baseContext.ChecksumResChan <- false
				tableCheckDuration = time.Since(startTime)
				baseContext.Log.Debugf("Debug: Iteration %d, record CRC32 checksum value is not equal of table pair: %s.%s => %s.%s , Duration=%+v", int(ChecksumContext.GetIteration()), ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName, duration)
				baseContext.Log.Errorf("Critical: record CRC32 checksum value is not equal of table pair: %s.%s => %s.%s , tableCheckDuration=%+v", ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName, tableCheckDuration)
				return nil
			}
			baseContext.Log.Debugf("Debug: Iteration %d, record CRC32 checksum value is equal of table pair: %s.%s => %s.%s , Duration=%+v", int(ChecksumContext.GetIteration()), ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName, duration)
		}
	}
	estimatedRows := int(ChecksumContext.GetIteration() * ChecksumContext.GetChunkSize())
	tableCheckDuration = time.Since(startTime)
	elapsedSecond := int(tableCheckDuration / time.Second)
	if elapsedSecond < 1 {
		elapsedSecond = 1
	}
	CheckSpeed := estimatedRows / elapsedSecond
	baseContext.Log.Infof("Info: record CRC32 checksum value is equal of table pair: %s.%s => %s.%s , tableCheckDuration=%+v, tableCheckSpeed= %+v rows/second.", ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName, tableCheckDuration, CheckSpeed)
	baseContext.Log.Infof("End check table pair: %s.%s => %s.%s .", ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName)
	baseContext.ChecksumErrChan <- nil
	baseContext.ChecksumResChan <- true
	return nil
}

// ChecksumPerTableViaTimeColumn 使用指定的时间字段增量核对
func (this *ChecksumJob) ChecksumPerTableViaTimeColumn(baseContext *logic.BaseContext, tableContext *logic.TableContext) (err error) {
	startTime := time.Now()
	var tableCheckDuration time.Duration
	defer func() {
		<-this.ChecksumJobChan
		this.wg.Done()
	}()

	ChecksumContext := logic.NewChecksumContext(baseContext, tableContext)
	baseContext.Log.Infof("Starting check table pair: %s.%s => %s.%s .", ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName)

	// 判断用户有没有输入checkcolumn，没有则取全表左右核对字段
	baseContext.Log.Debugf("Get user-request check columns of table pair: %s.%s => %s.%s .", ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName)
	if ChecksumContext.CheckColumns == nil {
		if err := ChecksumContext.GetCheckColumns(); err != nil {
			baseContext.ChecksumErrChan <- err
			baseContext.ChecksumResChan <- false
			return err
		}
	}

	// 获取时间列、最大最小值
	baseContext.Log.Debugf("GetTimeColumn of table pair: %s.%s => %s.%s.", ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName)
	if err := ChecksumContext.GetTimeColumn(); err != nil {
		baseContext.ChecksumErrChan <- err
		baseContext.ChecksumResChan <- false
		return err
	}
	baseContext.Log.Debugf("Time column values range [%s-%s] of table pair: %s.%s => %s.%s .", ChecksumContext.Context.SpecifiedDatetimeRangeBegin, ChecksumContext.Context.SpecifiedDatetimeRangeEnd, ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName)

	// 获取满足TimeRange核对条件的估算行数
	estimatedRows, err := ChecksumContext.EstimateTableRowsViaExplain()
	if err != nil {
		baseContext.ChecksumErrChan <- err
		baseContext.ChecksumResChan <- false
		return err
	}

	// 计算checksum值
	var hasFurtherRange = true
	for hasFurtherRange {
		hasFurtherRange, err = ChecksumContext.CalculateNextIterationTimeRange()
		baseContext.Log.Debugf("CalculateNextIterationTimeRange [hasFurtherRange=%t] of table pair: %s.%s => %s.%s.", hasFurtherRange, ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName)

		var isChunkChecksumEqual bool
		var duration time.Duration
		if hasFurtherRange {
			// 增加重试机制
			for i := 0; i < int(ChecksumContext.Context.DefaultNumRetries); i++ {
				if i != 0 {
					time.Sleep(3 * time.Second)
					baseContext.Log.Debugf("IterationTimeRangeQueryChecksum [%s-%s] retry times %d of table pair: %s.%s => %s.%s .", ChecksumContext.ChecksumIterationRangeMinValues.AbstractValues(), ChecksumContext.ChecksumIterationRangeMaxValues.AbstractValues(), i, ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName)
				} else {
					baseContext.Log.Debugf("IterationTimeRangeQueryChecksum [%s-%s] of table pair: %s.%s => %s.%s .", ChecksumContext.ChecksumIterationRangeMinValues.AbstractValues(), ChecksumContext.ChecksumIterationRangeMaxValues.AbstractValues(), ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName)
				}
				isChunkChecksumEqual, duration, err = ChecksumContext.IterationTimeRangeQueryChecksum()
				if err == nil {
					break
				}
			}
			if err != nil {
				baseContext.ChecksumErrChan <- err
				baseContext.ChecksumResChan <- false
				tableCheckDuration = time.Since(startTime)
				baseContext.Log.Debugf("Debug: Iteration %d, record CRC32 checksum value is not equal of table pair: %s.%s => %s.%s , Duration=%+v", int(ChecksumContext.GetIteration()), ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName, duration)
				baseContext.Log.Errorf("Critical: record CRC32 checksum value is not equal of table pair: %s.%s => %s.%s , tableCheckDuration=%+v", ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName, tableCheckDuration)
				return err
			}
			if isChunkChecksumEqual == false {
				baseContext.ChecksumErrChan <- nil
				baseContext.ChecksumResChan <- false
				tableCheckDuration = time.Since(startTime)
				baseContext.Log.Debugf("Debug: Iteration %d, record CRC32 checksum value is not equal of table pair: %s.%s => %s.%s , Duration=%+v", int(ChecksumContext.GetIteration()), ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName, duration)
				baseContext.Log.Errorf("Critical: record CRC32 checksum value is not equal of table pair: %s.%s => %s.%s , tableCheckDuration=%+v", ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName, tableCheckDuration)
				return nil
			}
			baseContext.Log.Debugf("Debug: Iteration %d, record CRC32 checksum value is equal of table pair: %s.%s => %s.%s , Duration=%+v", int(ChecksumContext.GetIteration()), ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName, duration)
		}
	}

	tableCheckDuration = time.Since(startTime)
	elapsedSecond := int(tableCheckDuration / time.Second)
	if elapsedSecond < 1 {
		elapsedSecond = 1
	}
	CheckSpeed := estimatedRows / elapsedSecond
	baseContext.Log.Infof("Info: record CRC32 checksum value is equal of table pair: %s.%s => %s.%s , tableCheckDuration=%+v, tableCheckSpeed= %+v rows/second.", ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName, tableCheckDuration, CheckSpeed)
	baseContext.Log.Infof("End check table pair: %s.%s => %s.%s .", ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName)
	baseContext.ChecksumErrChan <- nil
	baseContext.ChecksumResChan <- true
	return nil
}

// 核对任务
func (this *ChecksumJob) checksum(baseContext *logic.BaseContext) {
	// 获取源表和目标表
	if err := GenerateTableList(baseContext); err != nil {
		baseContext.Log.Errorf("Generating source and target tables failed, %s", err.Error())
		baseContext.PanicAbort <- err
	}

	// 逐个表进行核对, 按顺序去除map的元素
	tableNum := len(baseContext.PairOfSourceAndTargetTables)
	tableResultEqualNum := 0
	baseContext.ChecksumResChan = make(chan bool, tableNum)
	baseContext.ChecksumErrChan = make(chan error, tableNum)
	var keys []string
	for key := range baseContext.PairOfSourceAndTargetTables {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	baseContext.Log.Infof("%d pairs of source and target tables:", tableNum)
	for _, key := range keys {
		baseContext.Log.Infof("Table map: %s => %s .", key, baseContext.PairOfSourceAndTargetTables[key])
	}

	for _, key := range keys {
		sourceFullTableName := key
		targetFullTableName := baseContext.PairOfSourceAndTargetTables[key]
		sourceDatabase := strings.Split(sourceFullTableName, ".")[0]
		sourceTable := strings.Split(sourceFullTableName, ".")[1]
		targetDatabase := strings.Split(targetFullTableName, ".")[0]
		targetTable := strings.Split(targetFullTableName, ".")[1]
		tableContext := logic.NewTableContext(sourceDatabase, sourceTable, targetDatabase, targetTable)

		this.ChecksumJobChan <- 1
		this.wg.Add(1)
		// 使用主键/时间字段核对
		if isDatetimeColumnSpecified := baseContext.IsDatetimeColumnSpecified(); isDatetimeColumnSpecified {
			go this.ChecksumPerTableViaTimeColumn(baseContext, tableContext)
		} else {
			go this.ChecksumPerTable(baseContext, tableContext)
		}
	}

	for i := 0; i < len(baseContext.PairOfSourceAndTargetTables); i++ {
		if ret := <-baseContext.ChecksumResChan; ret {
			tableResultEqualNum += 1
		}
		err := <-baseContext.ChecksumErrChan
		if err != nil {
			baseContext.Log.Errorf("%s", err.Error())
		}
	}
	if tableResultEqualNum == tableNum && tableResultEqualNum != 0 {
		baseContext.Log.Infof("All pairs of tables check result is equal.")
	} else {
		baseContext.Log.Errorf("Table records check result %d equal, %d not equal.", tableResultEqualNum, tableNum-tableResultEqualNum)
	}
	this.wg.Wait()
}

func main() {
	baseContext := logic.NewBaseContext()
	flag.StringVar(&baseContext.SourceDatabases, "source-db-name", "", "Source database list separated by comma, eg: db1 or db1,db2.")
	flag.StringVar(&baseContext.SourceTables, "source-table-name", "", "Source tables list separated by comma, eg: table1 or table1,table2.")
	flag.StringVar(&baseContext.TargetDatabases, "target-db-name", "", "Target database list separated by comma, eg: db1 or db1,db2.")
	flag.StringVar(&baseContext.TargetTables, "target-table-name", "", "Target tables list separated by comma, eg: table1 or table1,table2.")
	flag.BoolVar(&baseContext.TargetDatabaseAsSource, "target-database-as-source", true, "Is target database name as source?  default: true.")
	flag.BoolVar(&baseContext.TargetTableAsSource, "target-table-as-source", true, "Is target table name as source? default: true.")
	flag.StringVar(&baseContext.TargetDatabaseAddSuffix, "target-database-add-suffix", "", "Target database name add a suffix to the source database name.")
	flag.StringVar(&baseContext.TargetTableAddSuffix, "target-table-add-suffix", "", "Target table name add a suffix to the source table name.")
	flag.StringVar(&baseContext.SourceTableNameRegexp, "source-table-regexp", "", "Source table names regular expression, eg: 'test_[0-9][0-9]\\.test_20.*'")

	flag.StringVar(&baseContext.SourceDBHost, "source-db-host", "127.0.0.1", "Source MySQL hostname")
	flag.IntVar(&baseContext.SourceDBPort, "source-db-port", 3306, "Source MySQL port")
	flag.StringVar(&baseContext.SourceDBUser, "source-db-user", "", "MySQL user")
	flag.StringVar(&baseContext.SourceDBPass, "source-db-password", "", "MySQL password")
	flag.StringVar(&baseContext.TargetDBHost, "target-db-host", "127.0.0.1", "Target MySQL hostname")
	flag.IntVar(&baseContext.TargetDBPort, "target-db-port", 3306, "Target MySQL port")
	flag.StringVar(&baseContext.TargetDBUser, "target-db-user", "", "MySQL user")
	flag.StringVar(&baseContext.TargetDBPass, "target-db-password", "", "MySQL password")
	flag.IntVar(&baseContext.Timeout, "conn-db-timeout", 30, "connect db timeout")
	flag.StringVar(&baseContext.RequestedColumnNames, "check-column-names", "", "Column names to check,eg: col1,col2,col3. By default, all columns are used.")
	flag.StringVar(&baseContext.SpecifiedDatetimeColumn, "specified-time-column", "", "Specified time column for range dataCheck.")
	flag.DurationVar(&baseContext.SpecifiedTimeRangePerStep, "time-range-per-step", 5*time.Minute, "time range per step for specified time column check,default 5m,eg:1h/2m/3s/4ms")
	specifiedDatetimeRangeBegin := flag.String("specified-time-begin", "", "Specified begin time of time column to check.")
	specifiedDatetimeRangeEnd := flag.String("specified-time-end", "", "Specified end time of time column to check.")
	chunkSize := flag.Int64("chunk-size", 1000, "amount of rows to handle in each iteration (allowed range: 10-100,000)")
	defaultRetries := flag.Int64("default-retries", 10, "Default number of retries for various operations before panicking")
	flag.BoolVar(&baseContext.IsSuperSetAsEqual, "is-superset-as-equal", false, "Shall we think that the records in target table is the superset of the source as equal? By default, we think the records are exactly equal as equal.")
	flag.BoolVar(&baseContext.IgnoreRowCountCheck, "ignore-row-count-check", false, "Shall we ignore check by counting rows? Default: false")
	flag.IntVar(&baseContext.ParallelThreads, "threads", 1, "Parallel threads of table checksum.")
	debug := flag.Bool("debug", false, "debug mode (very verbose)")
	logFile := flag.String("logfile", "", "Log file name.")

	flag.Parse()
	go baseContext.ListenOnPanicAbort()

	if err := baseContext.SetSpecifiedDatetimeRange(*specifiedDatetimeRangeBegin, *specifiedDatetimeRangeEnd); err != nil {
		baseContext.Log.Errorf("Illegal time range for time column, please check!")
		baseContext.PanicAbort <- err
	}
	baseContext.SetChunkSize(*chunkSize)
	baseContext.SetDefaultNumRetries(*defaultRetries)
	baseContext.SetLogLevel(*debug, *logFile)

	startTime := time.Now()
	defer func() {
		baseContext.CloseDB()
		baseContext.Log.Infof("Finished go-data-checksum. TotalDuration=%+v", time.Since(startTime))
	}()
	// 初始化源和目标库连接
	baseContext.Log.Infof("Staring go-data-checksum...")
	if err := baseContext.InitDB(); err != nil {
		baseContext.Log.Errorf("DB connection initiate failed.")
		baseContext.PanicAbort <- err
	}

	// 核对任务
	ChecksumJob := NewChecksumJob(baseContext.ParallelThreads)
	ChecksumJob.checksum(baseContext)

}
