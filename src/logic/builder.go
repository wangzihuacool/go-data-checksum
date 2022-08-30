package logic

import (
	"fmt"
"strconv"
"strings"
)

type ValueComparisonSign string

const (
	LessThanComparisonSign            ValueComparisonSign = "<"
	LessThanOrEqualsComparisonSign    ValueComparisonSign = "<="
	EqualsComparisonSign              ValueComparisonSign = "="
	GreaterThanOrEqualsComparisonSign ValueComparisonSign = ">="
	GreaterThanComparisonSign         ValueComparisonSign = ">"
	NotEqualsComparisonSign           ValueComparisonSign = "!="
)

// EscapeName will escape a db/table/column/... name by wrapping with backticks(反引号).
// It is not fool proof. I'm just trying to do the right thing here, not solving
// SQL injection issues, which should be irrelevant for this tool.
func EscapeName(name string) string {
	if unquoted, err := strconv.Unquote(name); err == nil {
		name = unquoted
	}
	return fmt.Sprintf("`%s`", name)
}

// buildColumnsPreparedValues 构造columns的prepared values，做一些时区转换和json转换等
func buildColumnsPreparedValues(columns *ColumnList) []string {
	values := make([]string, columns.Len(), columns.Len())
	for i, column := range columns.Columns() {
		var token string
		if column.enumToTextConversion {
			token = fmt.Sprintf("ELT(?, %s)", column.EnumValues)
		} else if column.Type == JSONColumnType {
			token = "convert(? using utf8mb4)"
		} else {
			token = "?"
		}
		values[i] = token
	}
	return values
}

// buildPreparedValues 构造prepared values
func buildPreparedValues(length int) []string {
	values := make([]string, length, length)
	for i := 0; i < length; i++ {
		values[i] = "?"
	}
	return values
}

// 复制slice的值
func duplicateNames(names []string) []string {
	duplicate := make([]string, len(names), len(names))
	// copy() 可以将一个数组切片复制到另一个数组切片中 copy(destSlice, srcSlice []T) int
	copy(duplicate, names)
	return duplicate
}

// BuildValueComparison 构造比较表达式，譬如 "(column = ?) 或者 (column > ?)"
func BuildValueComparison(column string, value string, comparisonSign ValueComparisonSign) (result string, err error) {
	if column == "" {
		return "", fmt.Errorf("Empty column in GetValueComparison")
	}
	if value == "" {
		return "", fmt.Errorf("Empty value in GetValueComparison")
	}
	comparison := fmt.Sprintf("(%s %s %s)", EscapeName(column), string(comparisonSign), value)
	return comparison, err
}

// BuildEqualsComparison 返回所有columns的条件表达式，譬如((col1 = ?) and (col2 = ?) and (col3 = ?))
func BuildEqualsComparison(columns []string, values []string) (result string, err error) {
	if len(columns) == 0 {
		return "", fmt.Errorf("Got 0 columns in GetEqualsComparison")
	}
	if len(columns) != len(values) {
		return "", fmt.Errorf("Got %d columns but %d values in GetEqualsComparison", len(columns), len(values))
	}
	comparisons := []string{}
	for i, column := range columns {
		value := values[i]
		// EqualsComparisonSign 为 "="
		// BuildValueComparison 构造比较表达式，譬如 "(column = ?)"
		comparison, err := BuildValueComparison(column, value, EqualsComparisonSign)
		if err != nil {
			return "", err
		}
		comparisons = append(comparisons, comparison)
	}
	// 返回所有columns的条件表达式，譬如((col1 = ?) and (col2 = ?) and (col3 = ?))
	result = strings.Join(comparisons, " and ")
	result = fmt.Sprintf("(%s)", result)
	return result, nil
}

// BuildEqualsPreparedComparison 构造columns的where条件等值表达式，譬如((col1 = ?) and (col2 = ?) and (col3 = ?))
func BuildEqualsPreparedComparison(columns []string) (result string, err error) {
	// buildPreparedValues 构造PreparedValues,这里就是Len(columns)个"?"
	values := buildPreparedValues(len(columns))
	// BuildEqualsComparison 返回所有columns的条件表达式，譬如((col1 = ?) and (col2 = ?) and (col3 = ?))
	return BuildEqualsComparison(columns, values)
}

// BuildSetPreparedClause 构造update语句的set子句，譬如 col1=?, col2=?
func BuildSetPreparedClause(columns *ColumnList) (result string, err error) {
	if columns.Len() == 0 {
		return "", fmt.Errorf("Got 0 columns in BuildSetPreparedClause")
	}
	setTokens := []string{}
	for _, column := range columns.Columns() {
		var setToken string
		// 时区转换成零时区+00:00
		if column.timezoneConversion != nil {
			setToken = fmt.Sprintf("%s=convert_tz(?, '%s', '%s')", EscapeName(column.Name), column.timezoneConversion.ToTimezone, "+00:00")
		} else if column.enumToTextConversion {
			setToken = fmt.Sprintf("%s=ELT(?, %s)", EscapeName(column.Name), column.EnumValues)
		} else if column.Type == JSONColumnType {
			setToken = fmt.Sprintf("%s=convert(? using utf8mb4)", EscapeName(column.Name))
		} else {
			setToken = fmt.Sprintf("%s=?", EscapeName(column.Name))
		}
		setTokens = append(setTokens, setToken)
	}
	return strings.Join(setTokens, ", "), nil
}

// BuildRangeComparison 构造唯一键的分批范围查询where条件
// 返回结果类似"result = ((col1 > ?) or ((col1 = ?) and (col2 > ?)) or (((col1 = ?) and (col2 = ?)) and (col3 > ?)) or ((col1 = ?) and (col2 = ?) and (col3 = ?)))"
// 返回结果类似"explodedArgs = [v1, v1, v2, v1, v2, v3, v1, v2, v3]"
func BuildRangeComparison(columns []string, values []string, args []interface{}, comparisonSign ValueComparisonSign) (result string, explodedArgs []interface{}, err error) {
	if len(columns) == 0 {
		return "", explodedArgs, fmt.Errorf("Got 0 columns in GetRangeComparison")
	}
	if len(columns) != len(values) {
		return "", explodedArgs, fmt.Errorf("Got %d columns but %d values in GetEqualsComparison", len(columns), len(values))
	}
	if len(columns) != len(args) {
		return "", explodedArgs, fmt.Errorf("Got %d columns but %d args in GetEqualsComparison", len(columns), len(args))
	}
	includeEquals := false
	if comparisonSign == LessThanOrEqualsComparisonSign {
		comparisonSign = LessThanComparisonSign
		includeEquals = true
	}
	if comparisonSign == GreaterThanOrEqualsComparisonSign {
		comparisonSign = GreaterThanComparisonSign
		includeEquals = true
	}
	comparisons := []string{}

	for i, column := range columns {
		value := values[i]
		// BuildValueComparison 构造比较表达式，譬如 "(column < ?) 或者 (column > ?)", 这里的comparisonSign只有>或<
		rangeComparison, err := BuildValueComparison(column, value, comparisonSign)
		if err != nil {
			return "", explodedArgs, err
		}
		if i > 0 {
			// BuildEqualsComparison 返回所有columns的条件表达式，譬如((col1 = ?) and (col2 = ?) and (col3 = ?))
			equalitiesComparison, err := BuildEqualsComparison(columns[0:i], values[0:i])
			if err != nil {
				return "", explodedArgs, err
			}
			comparison := fmt.Sprintf("(%s AND %s)", equalitiesComparison, rangeComparison)
			comparisons = append(comparisons, comparison)
			// args[0:i]... 等同于 args[0], args[1], ..., args[i]
			explodedArgs = append(explodedArgs, args[0:i]...)
			explodedArgs = append(explodedArgs, args[i])
		} else {
			comparisons = append(comparisons, rangeComparison)
			explodedArgs = append(explodedArgs, args[i])
		}
	}

	if includeEquals {
		// BuildEqualsComparison 返回所有columns的条件表达式，譬如((col1 = ?) and (col2 = ?) and (col3 = ?))
		comparison, err := BuildEqualsComparison(columns, values)
		if err != nil {
			return "", explodedArgs, nil
		}
		comparisons = append(comparisons, comparison)
		explodedArgs = append(explodedArgs, args...)
	}
	result = strings.Join(comparisons, " or ")
	result = fmt.Sprintf("(%s)", result)
	return result, explodedArgs, nil
}

// BuildRangePreparedComparison 返回唯一键的分批[上限/下限]范围查询where条件
func BuildRangePreparedComparison(columns *ColumnList, args []interface{}, comparisonSign ValueComparisonSign) (result string, explodedArgs []interface{}, err error) {
	// buildColumnsPreparedValues 构造columns的prepared values，做一些时区转换和json转换等
	values := buildColumnsPreparedValues(columns)
	// BuildRangeComparison 构造唯一键的分批范围查询where条件
	return BuildRangeComparison(columns.Names(), values, args, comparisonSign)
}

// BuildChunkChecksumSQL 构造分批计算CRC32聚合结果的SQL，或者分批计算每行CRC32值的SQL。
// 这里分批范围是 (rangeMin, rangeMax] ，第一批是 [rangeMin, rangeMax]
// 最终SQL类似：select /* dataChecksum */
//                   COALESCE(LOWER(CONV(BIT_XOR(cast(crc32(CONCAT_WS('#',id, ftime, c1, c2)) as UNSIGNED)), 10, 16)), 0) as CRC32XOR
//                OR COALESCE(LOWER(CONV(cast(crc32(CONCAT_WS('#',id, ftime, c1, c2)) as UNSIGNED), 10, 16)), 0) as CRC32
//              from test.t_time
//             where (((col1 > ?) or ((col1 = ?) and (col2 > ?)) or (((col1 = ?) and (col2 = ?)) and (col3 > ?)))
//	             and ((col1 < ?) or ((col1 = ?) and (col2 < ?)) or (((col1 = ?) and (col2 = ?)) and (col3 < ?)) or ((col1 = ?) and (col2 = ?) and (col3 = ?))))
func BuildChunkChecksumSQL(databaseName, tableName string, checkColumns, uniqueKeyColumns *ColumnList, rangeStartValues, rangeEndValues []string, rangeStartArgs, rangeEndArgs []interface{}, includeRangeStartValues bool, checkLevel int64) (result string, explodedArgs []interface{}, err error) {
	// 库表名转义
	databaseName = EscapeName(databaseName)
	tableName = EscapeName(tableName)

	// 复制checkColumns的值到 checkColumns,对列名进行转义
	checkColumnNames := duplicateNames(checkColumns.Names())
	for i := range checkColumnNames {
		checkColumnNames[i] = EscapeName(checkColumnNames[i])
	}
	// 源表和影子表共享列(包含重命名列)的字符串表示: sharedCol1, sharedCol2, sharedCol3
	checkColumnNamesListing := strings.Join(checkColumnNames, ", ")

	// minRangeComparisonSign = ">"，如果包含范围最小值，则为 ">=" 。包含范围最小值的情况是第一次循环插入
	var minRangeComparisonSign ValueComparisonSign = GreaterThanComparisonSign
	if includeRangeStartValues {
		minRangeComparisonSign = GreaterThanOrEqualsComparisonSign
	}

	// 构造唯一键的分批查询的起始范围where条件.
	// 构造的SQL条件rangeStartComparison类似：((col1 > ?) or ((col1 = ?) and (col2 > ?)) or (((col1 = ?) and (col2 = ?)) and (col3 > ?)) or ((col1 = ?) and (col2 = ?) and (col3 = ?)))"
	rangeStartComparison, rangeExplodedArgs, err := BuildRangeComparison(uniqueKeyColumns.Names(), rangeStartValues, rangeStartArgs, minRangeComparisonSign)
	if err != nil {
		return "", explodedArgs, err
	}
	explodedArgs = append(explodedArgs, rangeExplodedArgs...)

	// 构造唯一键的分批查询的中止范围where条件.
	// 构造的SQL条件rangeEndComparison类似：((col1 < ?) or ((col1 = ?) and (col2 < ?)) or (((col1 = ?) and (col2 = ?)) and (col3 < ?)) or ((col1 = ?) and (col2 = ?) and (col3 = ?)))"
	rangeEndComparison, rangeExplodedArgs, err := BuildRangeComparison(uniqueKeyColumns.Names(), rangeEndValues, rangeEndArgs, LessThanOrEqualsComparisonSign)
	if err != nil {
		return "", explodedArgs, err
	}
	explodedArgs = append(explodedArgs, rangeExplodedArgs...)

	var checkClause string
	if checkLevel == 1 {
		checkClause = fmt.Sprintf("COALESCE(LOWER(CONV(BIT_XOR(cast(crc32(CONCAT_WS('#', %s)) as UNSIGNED)), 10, 16)), 0) as CRC32XOR",
			checkColumnNamesListing)
	} else if checkLevel == 2 {
		checkClause = fmt.Sprintf("COALESCE(LOWER(CONV(cast(crc32(CONCAT_WS('#', %s)) as UNSIGNED), 10, 16)), 0) as CRC32",
			checkColumnNamesListing)
	} else {
		return "", nil, fmt.Errorf("Critical: table %s.%s wrong checkLevelFlag input in BuildChunkChecksumSQL.",
			databaseName, tableName)
	}
	result = fmt.Sprintf(`
      select /* dataChecksum %s.%s */ %s
        from %s.%s 
       where (%s and %s)
    `, databaseName, tableName, checkClause, databaseName, tableName, rangeStartComparison, rangeEndComparison)
	return result, explodedArgs, nil
}


// BuildRangeChecksumPreparedQuery 返回分批计算CRC32的checksum值的SQL，这里分批范围是 (rangeMin, rangeMax] ，第一批是 [rangeMin, rangeMax]
func BuildRangeChecksumPreparedQuery(databaseName, tableName string, checkColumns, uniqueKeyColumns *ColumnList, rangeStartArgs, rangeEndArgs []interface{}, includeRangeStartValues bool, checkLevel int64) (result string, explodedArgs []interface{}, err error) {
	// buildColumnsPreparedValues 构造唯一键的columns的prepared values，做一些时区转换和json转换等
	rangeStartValues := buildColumnsPreparedValues(uniqueKeyColumns)
	rangeEndValues := buildColumnsPreparedValues(uniqueKeyColumns)
	return BuildChunkChecksumSQL(databaseName, tableName, checkColumns, uniqueKeyColumns, rangeStartValues, rangeEndValues, rangeStartArgs, rangeEndArgs, includeRangeStartValues, checkLevel)
}


// BuildUniqueKeyRangeEndPreparedQueryViaOffset 构造分批范围的分批下限值查询SQL
// 最终SQL类似：select  /* gh-ost db.tab iteration:5 */
//					col1, col2, col3
//				from
//					db.tab
//			   where ((col1 > ?) or ((col1 = ?) and (col2 > ?)) or (((col1 = ?) and (col2 = ?)) and (col3 > ?)))
//		         and ((col1 < ?) or ((col1 = ?) and (col2 < ?)) or (((col1 = ?) and (col2 = ?)) and (col3 < ?)) or ((col1 = ?) and (col2 = ?) and (col3 = ?)))
//			order by
//					col1 asc, col2 asc, col3 asc
//			 limit 1
//			 offset {chunkSize -1}
func BuildUniqueKeyRangeEndPreparedQueryViaOffset(databaseName, tableName string, uniqueKeyColumns *ColumnList, rangeStartArgs, rangeEndArgs []interface{}, chunkSize int64, includeRangeStartValues bool, hint string) (result string, explodedArgs []interface{}, err error) {
	if uniqueKeyColumns.Len() == 0 {
		return "", explodedArgs, fmt.Errorf("Got 0 columns in BuildUniqueKeyRangeEndPreparedQuery")
	}
	databaseName = EscapeName(databaseName)
	tableName = EscapeName(tableName)

	// 包含范围起始值，则为GreaterThanOrEqualsComparisonSign，即">="；不包含范围起始值，则为GreaterThanComparisonSign 即 ">"
	var startRangeComparisonSign ValueComparisonSign = GreaterThanComparisonSign
	if includeRangeStartValues {
		startRangeComparisonSign = GreaterThanOrEqualsComparisonSign
	}

	// 返回唯一键的分批下限范围查询where条件，rangeStartArgs即唯一键各字段传入的值
	rangeStartComparison, rangeExplodedArgs, err := BuildRangePreparedComparison(uniqueKeyColumns, rangeStartArgs, startRangeComparisonSign)
	if err != nil {
		return "", explodedArgs, err
	}
	explodedArgs = append(explodedArgs, rangeExplodedArgs...)
	// LessThanOrEqualsComparisonSign 即 "<="
	// 返回唯一键的分批上限范围查询where条件，rangeEndArgs即唯一键各字段传入的值
	rangeEndComparison, rangeExplodedArgs, err := BuildRangePreparedComparison(uniqueKeyColumns, rangeEndArgs, LessThanOrEqualsComparisonSign)
	if err != nil {
		return "", explodedArgs, err
	}
	explodedArgs = append(explodedArgs, rangeExplodedArgs...)

	// uniqueKeyColumnNames 为唯一键的字段名，uniqueKeyColumnAscending 为唯一键字段名+asc，uniqueKeyColumnDescending为唯一键字段名+desc
	uniqueKeyColumnNames := duplicateNames(uniqueKeyColumns.Names())
	uniqueKeyColumnAscending := make([]string, len(uniqueKeyColumnNames), len(uniqueKeyColumnNames))
	uniqueKeyColumnDescending := make([]string, len(uniqueKeyColumnNames), len(uniqueKeyColumnNames))
	for i, column := range uniqueKeyColumns.Columns() {
		uniqueKeyColumnNames[i] = EscapeName(uniqueKeyColumnNames[i])
		if column.Type == EnumColumnType {
			uniqueKeyColumnAscending[i] = fmt.Sprintf("concat(%s) asc", uniqueKeyColumnNames[i])
			uniqueKeyColumnDescending[i] = fmt.Sprintf("concat(%s) desc", uniqueKeyColumnNames[i])
		} else {
			uniqueKeyColumnAscending[i] = fmt.Sprintf("%s asc", uniqueKeyColumnNames[i])
			uniqueKeyColumnDescending[i] = fmt.Sprintf("%s desc", uniqueKeyColumnNames[i])
		}
	}
	result = fmt.Sprintf(`
				select  /* dataChecksum %s.%s %s */
						%s
					from
						%s.%s
					where %s and %s
					order by
						%s
					limit 1
					offset %d
    `, databaseName, tableName, hint,
		strings.Join(uniqueKeyColumnNames, ", "),
		databaseName, tableName,
		rangeStartComparison, rangeEndComparison,
		strings.Join(uniqueKeyColumnAscending, ", "),
		(chunkSize - 1),
	)
	return result, explodedArgs, nil
}

// BuildUniqueKeyRangeEndPreparedQueryViaTemptable 构造分批范围的分批下限值查询SQL，与BuildUniqueKeyRangeEndPreparedQueryViaOffset稍有差异
// 最终SQL类似：  select /* dataChecksum db.tab iteration:5 */
//                     col1, col2, col3
//				 from (
//					select
//							col1, col2, col3
//						from
//							db.tab
//						where ((col1 > ?) or ((col1 = ?) and (col2 > ?)) or (((col1 = ?) and (col2 = ?)) and (col3 > ?)))
//                        and ((col1 < ?) or ((col1 = ?) and (col2 < ?)) or (((col1 = ?) and (col2 = ?)) and (col3 < ?)) or ((col1 = ?) and (col2 = ?) and (col3 = ?)))
//						order by
//							col1 asc, col2 asc, col3 asc
//						limit {chunkSize}
//				) select_osc_chunk
//			order by
//				col1 desc, col2 desc, col3 desc
//			limit 1
func BuildUniqueKeyRangeEndPreparedQueryViaTemptable(databaseName, tableName string, uniqueKeyColumns *ColumnList, rangeStartArgs, rangeEndArgs []interface{}, chunkSize int64, includeRangeStartValues bool, hint string) (result string, explodedArgs []interface{}, err error) {
	if uniqueKeyColumns.Len() == 0 {
		return "", explodedArgs, fmt.Errorf("Got 0 columns in BuildUniqueKeyRangeEndPreparedQuery")
	}
	databaseName = EscapeName(databaseName)
	tableName = EscapeName(tableName)

	// 包含范围起始值，则为GreaterThanOrEqualsComparisonSign，即">="；不包含范围起始值，则为GreaterThanComparisonSign 即 ">"
	var startRangeComparisonSign ValueComparisonSign = GreaterThanComparisonSign
	if includeRangeStartValues {
		startRangeComparisonSign = GreaterThanOrEqualsComparisonSign
	}

	// 返回唯一键的分批下限范围查询where条件，rangeStartArgs即唯一键各字段传入的值
	rangeStartComparison, rangeExplodedArgs, err := BuildRangePreparedComparison(uniqueKeyColumns, rangeStartArgs, startRangeComparisonSign)
	if err != nil {
		return "", explodedArgs, err
	}
	explodedArgs = append(explodedArgs, rangeExplodedArgs...)
	// LessThanOrEqualsComparisonSign 即 "<="
	// 返回唯一键的分批上限范围查询where条件，rangeEndArgs即唯一键各字段传入的值
	rangeEndComparison, rangeExplodedArgs, err := BuildRangePreparedComparison(uniqueKeyColumns, rangeEndArgs, LessThanOrEqualsComparisonSign)
	if err != nil {
		return "", explodedArgs, err
	}
	explodedArgs = append(explodedArgs, rangeExplodedArgs...)

	// uniqueKeyColumnNames 为唯一键的字段名，uniqueKeyColumnAscending 为唯一键字段名+asc，uniqueKeyColumnDescending为唯一键字段名+desc
	uniqueKeyColumnNames := duplicateNames(uniqueKeyColumns.Names())
	uniqueKeyColumnAscending := make([]string, len(uniqueKeyColumnNames), len(uniqueKeyColumnNames))
	uniqueKeyColumnDescending := make([]string, len(uniqueKeyColumnNames), len(uniqueKeyColumnNames))
	for i, column := range uniqueKeyColumns.Columns() {
		uniqueKeyColumnNames[i] = EscapeName(uniqueKeyColumnNames[i])
		if column.Type == EnumColumnType {
			uniqueKeyColumnAscending[i] = fmt.Sprintf("concat(%s) asc", uniqueKeyColumnNames[i])
			uniqueKeyColumnDescending[i] = fmt.Sprintf("concat(%s) desc", uniqueKeyColumnNames[i])
		} else {
			uniqueKeyColumnAscending[i] = fmt.Sprintf("%s asc", uniqueKeyColumnNames[i])
			uniqueKeyColumnDescending[i] = fmt.Sprintf("%s desc", uniqueKeyColumnNames[i])
		}
	}
	result = fmt.Sprintf(`
      select /* dataChecksum %s.%s %s */ %s
				from (
					select
							%s
						from
							%s.%s
						where %s and %s
						order by
							%s
						limit %d
				) select_osc_chunk
			order by
				%s
			limit 1
    `, databaseName, tableName, hint, strings.Join(uniqueKeyColumnNames, ", "),
		strings.Join(uniqueKeyColumnNames, ", "), databaseName, tableName,
		rangeStartComparison, rangeEndComparison,
		strings.Join(uniqueKeyColumnAscending, ", "), chunkSize,
		strings.Join(uniqueKeyColumnDescending, ", "),
	)
	return result, explodedArgs, nil
}

// BuildUniqueKeyMinValuesPreparedQuery 构造查询唯一键最小值的SQL
func BuildUniqueKeyMinValuesPreparedQuery(databaseName, tableName string, uniqueKeyColumns *ColumnList) (string, error) {
	return buildUniqueKeyMinMaxValuesPreparedQuery(databaseName, tableName, uniqueKeyColumns, "asc")
}
// BuildUniqueKeyMaxValuesPreparedQuery 构造查询唯一键最大值的SQL
func BuildUniqueKeyMaxValuesPreparedQuery(databaseName, tableName string, uniqueKeyColumns *ColumnList) (string, error) {
	return buildUniqueKeyMinMaxValuesPreparedQuery(databaseName, tableName, uniqueKeyColumns, "desc")
}

// buildUniqueKeyMinMaxValuesPreparedQuery 生成实际构造语句，在此基础上加上asc/desc排序来构造最小/最大值的对应SQL
func buildUniqueKeyMinMaxValuesPreparedQuery(databaseName, tableName string, uniqueKeyColumns *ColumnList, order string) (string, error) {
	if uniqueKeyColumns.Len() == 0 {
		return "", fmt.Errorf("Got 0 columns in BuildUniqueKeyMinMaxValuesPreparedQuery")
	}
	// 使用反引号转义库表名
	databaseName = EscapeName(databaseName)
	tableName = EscapeName(tableName)
	// 拷贝uniqueKeyColumns.Names()到新的sliceuniqKeyColumnNames
	uniqueKeyColumnNames := duplicateNames(uniqueKeyColumns.Names())
	uniqueKeyColumnOrder := make([]string, len(uniqueKeyColumnNames), len(uniqueKeyColumnNames))
	for i, column := range uniqueKeyColumns.Columns() {
		uniqueKeyColumnNames[i] = EscapeName(uniqueKeyColumnNames[i])
		if column.Type == EnumColumnType {
			uniqueKeyColumnOrder[i] = fmt.Sprintf("concat(%s) %s", uniqueKeyColumnNames[i], order)
		} else {
			uniqueKeyColumnOrder[i] = fmt.Sprintf("%s %s", uniqueKeyColumnNames[i], order)
		}
	}
	// select /* dataChecksum `db`.`tab` */ col1,col2 from `db`.`tab` order by col1 asc/desc, col2 asc/desc limit 1
	query := fmt.Sprintf(`
      select /* dataChecksum %s.%s */ %s
				from
					%s.%s
				order by
					%s
				limit 1
    `, databaseName, tableName, strings.Join(uniqueKeyColumnNames, ", "),
		databaseName, tableName,
		strings.Join(uniqueKeyColumnOrder, ", "),
	)
	return query, nil
}