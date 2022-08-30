package logic

import (
	gosql "database/sql"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
)

type ColumnType int

const (
	UnknownColumnType ColumnType = iota
	TimestampColumnType
	DateTimeColumnType
	EnumColumnType
	MediumIntColumnType
	JSONColumnType
	FloatColumnType
	BinaryColumnType
)

const maxMediumintUnsigned int32 = 16777215

type TableContext struct {
	SourceDatabaseName              string
	SourceTableName                 string
	TargetDatabaseName              string
	TargetTableName                 string
	FinishedFlag                    int64
	Iteration                       int64
}

func NewTableContext(sourceDatabaseName, sourceTableName, targetDatabaseName, targetTableName string) *TableContext {
	return &TableContext{
		SourceDatabaseName:             sourceDatabaseName,
		SourceTableName:                sourceTableName,
		TargetDatabaseName:             targetDatabaseName,
		TargetTableName:                targetTableName,
		FinishedFlag:                   0,
		Iteration:                      0,
	}
}

type BaseContext struct {
	SourceDBHost                    string
	SourceDBPort                    int
	SourceDBUser                    string
	SourceDBPass                    string
	TargetDBHost                    string
	TargetDBPort                    int
	TargetDBUser                    string
	TargetDBPass                    string
	Timeout                         int
	SourceDB                        *gosql.DB
	TargetDB                        *gosql.DB

	SourceDatabases                 string
	SourceTables                    string
	TargetDatabases                 string
	TargetTables                    string
	TargetDatabaseAsSource          bool
	TargetTableAsSource             bool
	TargetDatabaseAddSuffix         string
	TargetTableAddSuffix            string
	SourceTableNameRegexp           string
	TableQueryHint                  string

	SourceTableFullNameList         []string
	TargetTableFullNameList         []string
	PairOfSourceAndTargetTables     map[string]string
	SourceDatabaseList              []string
	SourceTableList                 []string
	TargetDatabaseList              []string
    TargetTableList                 []string
	RequestedColumnNames            string
	SpecifiedDatetimeColumn         string
	SpecifiedTimeRangePerStep       time.Duration
	SpecifiedDatetimeRangeBegin     time.Time
	SpecifiedDatetimeRangeEnd       time.Time
	IgnoreRowCountCheck             bool

	ChunkSize                       int64
	DefaultNumRetries               int64
	IsSuperSetAsEqual               bool
	ParallelThreads                 int
	ChecksumResChan                 chan bool
	ChecksumErrChan                 chan error
	PanicAbort                      chan error
	throttleMutex                   *sync.Mutex
	Log                             *log.Logger
	Logfile                         string
}


func NewBaseContext() *BaseContext {
	return &BaseContext{
		ChunkSize:                   1000,
		DefaultNumRetries:           10,
		PanicAbort:                  make(chan error),
		throttleMutex:               &sync.Mutex{},
		Log:                         log.New(),
	}
}

// SetChunkSize 设置chunksize范围：10-100000
func (this *BaseContext) SetChunkSize(chunkSize int64) {
	if chunkSize < 10 {
		chunkSize = 10
	}
	if chunkSize > 100000 {
		chunkSize = 100000
	}
	atomic.StoreInt64(&this.ChunkSize, chunkSize)
}

// SetDefaultNumRetries 设置最大重试次数,默认10
func (this *BaseContext) SetDefaultNumRetries(retries int64) {
	this.throttleMutex.Lock()
	defer this.throttleMutex.Unlock()
	if retries > 0 {
		this.DefaultNumRetries = retries
	}
}

// SetLogLevel 设置日志级别
func (this *BaseContext) SetLogLevel(debug bool, logFile string) {
	this.Log.SetLevel(log.InfoLevel)
	if debug {
		this.Log.SetLevel(log.DebugLevel)
	}
	this.Log.SetOutput(os.Stdout)
	if logFile != "" {
		file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err == nil {
			this.Log.Out = file
		} else {
			this.Log.Infof("Failed to log to file, using defualt stdout.")
		}
	}
}

// SetSpecifiedDatetimeRange 设置按时间字段核对的起止时间
func (this *BaseContext) SetSpecifiedDatetimeRange(specifiedTimeBegin, specifiedTimeEnd string) (err error) {
	if specifiedTimeBegin != "" {
		if this.SpecifiedDatetimeRangeBegin, err = time.ParseInLocation("2006-01-02 15:04:05", specifiedTimeBegin, time.Local); err != nil {
			return err
		}
	}
	if specifiedTimeEnd != "" {
		if this.SpecifiedDatetimeRangeEnd, err = time.ParseInLocation("2006-01-02 15:04:05", specifiedTimeEnd, time.Local); err != nil {
			return err
		}
	}
	if this.SpecifiedDatetimeRangeEnd.Before(this.SpecifiedDatetimeRangeBegin) {
		return fmt.Errorf("illegal time range, specified-time-end is before specified-time-begin")
	}
	return nil
}

// IsDatetimeColumnSpecified Check whether datetime column is specified and begin/end time provided.
func (this *BaseContext) IsDatetimeColumnSpecified() bool {
	if this.SpecifiedDatetimeColumn != "" && !this.SpecifiedDatetimeRangeBegin.IsZero() && !this.SpecifiedDatetimeRangeEnd.IsZero() {
		return true
	} else {
		return false
	}
}

func (this *BaseContext) GetDBUri(databaseName string) (string, string) {
	sourceDBUri := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&loc=Local&timeout=%ds&readTimeout=%ds&writeTimeout=%ds&interpolateParams=true&charset=latin1", this.SourceDBUser, this.SourceDBPass, this.SourceDBHost, this.SourceDBPort, databaseName, this.Timeout, this.Timeout, this.Timeout)
	targetDBUri := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&loc=Local&timeout=%ds&readTimeout=%ds&writeTimeout=%ds&interpolateParams=true&charset=latin1", this.TargetDBUser, this.TargetDBPass, this.TargetDBHost, this.TargetDBPort, databaseName, this.Timeout, this.Timeout, this.Timeout)
	return sourceDBUri, targetDBUri
}

// InitDB 建立数据库连接
func (this *BaseContext) InitDB() (err error) {
	databaseName := "information_schema"
	sourceDBUri, targetDBUri := this.GetDBUri(databaseName)

	initDBConnect := func(dbUri string) (db *gosql.DB, err error) {
		if db, err = gosql.Open("mysql", dbUri); err != nil {
			return db, err
		}
		if err = db.Ping(); err != nil {
			return db, err
		}
		db.SetConnMaxLifetime(time.Minute * 3)
		db.SetMaxIdleConns(30)
		return db, err
	}

	if this.SourceDB, err = initDBConnect(sourceDBUri); err != nil {
		return err
	}
	if this.TargetDB, err = initDBConnect(targetDBUri); err != nil {
		return err
	}
	return nil
}

// CloseDB 关闭数据库连接
func (this *BaseContext) CloseDB() {
	this.TargetDB.Close()
	this.SourceDB.Close()
}




// ListenOnPanicAbort aborts on abort request
func (this *BaseContext) ListenOnPanicAbort() {
	err := <- this.PanicAbort
	this.Log.Fatalf(err.Error())
}




type TimezoneConversion struct {
	ToTimezone string
}


type Column struct {
	Name                 string
	IsUnsigned           bool
	Charset              string
	Type                 ColumnType
	EnumValues           string
	timezoneConversion   *TimezoneConversion
	enumToTextConversion bool
	BinaryOctetLength uint
}


func NewColumns(names []string) []Column {
	result := make([]Column, len(names))
	for i := range names {
		result[i].Name = names[i]
	}
	return result
}

func ParseColumns(names string) []Column {
	namesArray := strings.Split(names, ",")
	return NewColumns(namesArray)
}

// ColumnsMap maps a column name onto its ordinal position
type ColumnsMap map[string]int

func NewEmptyColumnsMap() ColumnsMap {
	columnsMap := make(map[string]int)
	return ColumnsMap(columnsMap)
}

func NewColumnsMap(orderedColumns []Column) ColumnsMap {
	columnsMap := NewEmptyColumnsMap()
	for i, column := range orderedColumns {
		columnsMap[column.Name] = i
	}
	return columnsMap
}

// ColumnList makes for a named list of columns
type ColumnList struct {
	columns  []Column
	Ordinals ColumnsMap
}

// NewColumnList creates an object given ordered list of column names
func NewColumnList(names []string) *ColumnList {
	result := &ColumnList{
		columns: NewColumns(names),
	}
	result.Ordinals = NewColumnsMap(result.columns)
	return result
}

// ParseColumnList parses a comma delimited list of column names
func ParseColumnList(names string) *ColumnList {
	result := &ColumnList{
		columns: ParseColumns(names),
	}
	result.Ordinals = NewColumnsMap(result.columns)
	return result
}

func (this *ColumnList) Columns() []Column {
	return this.columns
}

func (this *ColumnList) Names() []string {
	names := make([]string, len(this.columns))
	for i := range this.columns {
		names[i] = this.columns[i].Name
	}
	return names
}

func (this *ColumnList) GetColumn(columnName string) *Column {
	if ordinal, ok := this.Ordinals[columnName]; ok {
		return &this.columns[ordinal]
	}
	return nil
}

func (this *ColumnList) SetUnsigned(columnName string) {
	this.GetColumn(columnName).IsUnsigned = true
}

func (this *ColumnList) IsUnsigned(columnName string) bool {
	return this.GetColumn(columnName).IsUnsigned
}

func (this *ColumnList) SetCharset(columnName string, charset string) {
	this.GetColumn(columnName).Charset = charset
}

func (this *ColumnList) GetCharset(columnName string) string {
	return this.GetColumn(columnName).Charset
}

func (this *ColumnList) SetColumnType(columnName string, columnType ColumnType) {
	this.GetColumn(columnName).Type = columnType
}

func (this *ColumnList) GetColumnType(columnName string) ColumnType {
	return this.GetColumn(columnName).Type
}

func (this *ColumnList) SetConvertDatetimeToTimestamp(columnName string, toTimezone string) {
	this.GetColumn(columnName).timezoneConversion = &TimezoneConversion{ToTimezone: toTimezone}
}

func (this *ColumnList) HasTimezoneConversion(columnName string) bool {
	return this.GetColumn(columnName).timezoneConversion != nil
}

func (this *ColumnList) SetEnumToTextConversion(columnName string) {
	this.GetColumn(columnName).enumToTextConversion = true
}

func (this *ColumnList) IsEnumToTextConversion(columnName string) bool {
	return this.GetColumn(columnName).enumToTextConversion
}

func (this *ColumnList) SetEnumValues(columnName string, enumValues string) {
	this.GetColumn(columnName).EnumValues = enumValues
}

func (this *ColumnList) String() string {
	return strings.Join(this.Names(), ",")
}

func (this *ColumnList) Equals(other *ColumnList) bool {
	return reflect.DeepEqual(this.Columns, other.Columns)
}

func (this *ColumnList) EqualsByNames(other *ColumnList) bool {
	return reflect.DeepEqual(this.Names(), other.Names())
}

// IsSubsetOf returns 'true' when column names of this list are a subset of
// another list, in arbitrary order (order agnostic)
func (this *ColumnList) IsSubsetOf(other *ColumnList) bool {
	for _, column := range this.columns {
		if _, exists := other.Ordinals[column.Name]; !exists {
			return false
		}
	}
	return true
}

func (this *ColumnList) Len() int {
	return len(this.columns)
}

// UniqueKey is the combination of a key's name and columns
type UniqueKey struct {
	Name        string
	Columns     ColumnList
	HasNullable bool
	IsAutoIncrement bool
}

// IsPrimary checks if this unique key is primary
func (this *UniqueKey) IsPrimary() bool {
	return this.Name == "PRIMARY"
}

func (this *UniqueKey) Len() int {
	return this.Columns.Len()
}

func (this *UniqueKey) String() string {
	description := this.Name
	if this.IsAutoIncrement {
		description = fmt.Sprintf("%s (auto_increment)", description)
	}
	return fmt.Sprintf("%s: %s; has nullable: %+v", description, this.Columns.Names(), this.HasNullable)
}

type ColumnValues struct {
	abstractValues []interface{}
	ValuesPointers []interface{}
}

// NewColumnValues 将abstractValues的值复制到ValuesPointers
func NewColumnValues(length int) *ColumnValues {
	result := &ColumnValues{
		abstractValues: make([]interface{}, length),
		ValuesPointers: make([]interface{}, length),
	}
	for i := 0; i < length; i++ {
		result.ValuesPointers[i] = &result.abstractValues[i]
	}

	return result
}

func ToColumnValues(abstractValues []interface{}) *ColumnValues {
	result := &ColumnValues{
		abstractValues: abstractValues,
		ValuesPointers: make([]interface{}, len(abstractValues)),
	}
	for i := 0; i < len(abstractValues); i++ {
		result.ValuesPointers[i] = &result.abstractValues[i]
	}

	return result
}

func (this *ColumnValues) AbstractValues() []interface{} {
	return this.abstractValues
}

// StringColumn 返回二进制对应的ASCII字符串
func (this *ColumnValues) StringColumn(index int) string {
	val := this.AbstractValues()[index]
	// uint8 the set of all unsigned  8-bit integers (0 to 255),等同于byte
	if ints, ok := val.([]uint8); ok {
		// string([]uint8) 返回byte对应的字符串
		return string(ints)
	}
	return fmt.Sprintf("%+v", val)
}

func (this *ColumnValues) String() string {
	stringValues := []string{}
	for i := range this.AbstractValues() {
		stringValues = append(stringValues, this.StringColumn(i))
	}
	return strings.Join(stringValues, ",")
}