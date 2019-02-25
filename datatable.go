package dbutil

import (
	"bytes"
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

const timeLayout = "2006-01-02 15:04:05"

//Mapping SQL data type to golang data type.
//Types not listed below will be converted to string.
var sql2golang = map[string]string{
	"BIT":           "bool",
	"TINYINT":       "int64",
	"SMALLINT":      "int64",
	"MEDIUMINT":     "int64",
	"INT":           "int64",
	"INTEGER":       "int64",
	"BIGINT":        "int64",
	"DECIMAL":       "float64",
	"FLOAT":         "float64",
	"DOUBLE":        "float64",
	"REAL":          "float64",
	"SMALLMONEY":    "float64",
	"MONEY":         "float64",
	"DATE":          "float64",
	"TIME":          "float64",
	"DATETIME":      "time.Time",
	"DATETIME2":     "time.Time",
	"SMALLDATETIME": "time.Time",
}

//Column contains the data definition of the column.
type Column struct {
	name             string
	dataType         string
	dataLength       int64
	decimalPrecision int64
	nullable         bool
}

//Name returns the name of the column.
func (col *Column) Name() string {
	return col.name
}

//DataType returns the data type of the column.
func (col *Column) DataType() string {
	return col.dataType
}

//DataLength returns the length of string type column and the scale of decimal type column.
func (col *Column) DataLength() int64 {
	return col.dataLength
}

//DecimalPrecision returns the decimal length part of decimal type column.
func (col *Column) DecimalPrecision() int64 {
	return col.decimalPrecision
}

//Nullable indicates whether the column can contain null value.
func (col *Column) Nullable() bool {
	return col.nullable
}

//NewColumn creates new column object and set properties.
func NewColumn(Name, DataType string, DataLength int64, DecimalPrecision int64, Nullable bool) *Column {
	col := new(Column)
	col.name = Name
	col.dataType = strings.ToUpper(DataType)
	col.dataLength = DataLength
	col.decimalPrecision = DecimalPrecision
	col.nullable = Nullable
	return col
}

//DataTable is a in memory data structure.
type DataTable struct {
	columns  []Column
	colIndex map[string]int
	data     [][]interface{}
	sync.RWMutex
}

//ColumnCounts returns the column counts of the data table.
func (dt *DataTable) ColumnCounts() int {
	dt.RLock()
	defer dt.RUnlock()
	return len(dt.columns)
}

//RowCounts returns the row counts of the data table.
func (dt *DataTable) RowCounts() int {
	dt.RLock()
	defer dt.RUnlock()
	return len(dt.data)
}

//ContainsColumn scans all the columns to check whether the given column exists in the data table.
func (dt *DataTable) ContainsColumn(ColumnName string) (colIndex int, err error) {
	//Grant read lock to prevent a concurrent schema change
	dt.RLock()
	defer dt.RUnlock()
	i, ok := dt.colIndex[ColumnName]
	if !ok {
		return 0, fmt.Errorf("given column name: %s does not exist in the data table", ColumnName)
	}
	return i, nil
}

//AddColumn adds a new column to the datatable and set default values.
func (dt *DataTable) AddColumn(Column Column) (err error) {
	//Lock the datatable during the schema change.
	dt.Lock()
	defer dt.Unlock()

	//Check duplication
	colCounts := len(dt.columns)
	for i := 0; i < colCounts; i++ {
		if dt.columns[i].name == Column.name {
			return fmt.Errorf("duplicate column: %s", Column.name)
		}
	}

	//Add column and set default value.
	dt.columns = append(dt.columns, Column)
	var v interface{}
	if Column.nullable {
		v = nil
	} else {
		switch sql2golang[Column.dataType] {
		case "":
			v = ""
		case "bool":
			v = false
		case "int64":
			v = int64(0)
		case "float64":
			v = float64(0.0)
		case "time.Time":
			v, _ = time.Parse(timeLayout, "1900-01-01 00:00:00")
		}
	}
	rowCounts := len(dt.data)
	for i := 0; i < rowCounts; i++ {
		dt.data[i][colCounts] = v
	}
	dt.flushColumnIndex()
	return nil
}

//AppendRow appends a new row to the data table.
func (dt *DataTable) AppendRow(Values ...interface{}) (err error) {
	dt.Lock()
	defer dt.Unlock()
	if len(Values) != len(dt.columns) {
		return fmt.Errorf("column counts mismatch")
	}
	for i, v := range Values {
		//Nullable validation
		if v == nil && dt.columns[i].nullable == false {
			return fmt.Errorf("column: %s does not allow null", dt.columns[i].name)
		}
		//Datatype validation
		var ok bool
		switch sql2golang[dt.columns[i].dataType] {
		case "":
			_, ok = v.(string)
		case "bool":
			_, ok = v.(bool)
		case "int64":
			_, ok = v.(int64)
		case "float64":
			_, ok = v.(float64)
		case "time.Time":
			_, ok = v.(time.Time)
		}
		if !ok {
			return fmt.Errorf("data type does not match with column: %d", i)
		}
	}
	dt.data = append(dt.data, Values)
	return nil
}

//AppendRowFromString first converts the string value to target data type then append a new row to the data table.
func (dt *DataTable) AppendRowFromString(Values ...string) (err error) {
	dt.Lock()
	defer dt.Unlock()
	if len(Values) != len(dt.columns) {
		return fmt.Errorf("column counts mismatch")
	}
	row := make([]interface{}, 0)
	for i, v := range Values {
		if v == "NULL" {
			row = append(row, nil)
		} else {
			t := sql2golang[dt.columns[i].dataType]
			switch t {
			case "":
				row = append(row, v)
			case "bool":
				if strings.ToUpper(v) == "OK" || strings.ToUpper(v) == "TRUE" || v == "1" {
					row = append(row, true)
				} else if strings.ToUpper(v) == "FALSE" || v == "0" {
					row = append(row, false)
				} else {
					return fmt.Errorf("unable to convert %s to bool", v)
				}
			case "int64":
				a, err := strconv.ParseInt(v, 10, 64)
				if err != nil {
					return fmt.Errorf("unable to convert %s to int64", v)
				}
				row = append(row, a)
			case "float64":
				a, err := strconv.ParseFloat(v, 64)
				if err != nil {
					return fmt.Errorf("unable to convert %s to float64", v)
				}
				row = append(row, a)
			case "time.Time":
				a, err := time.Parse(timeLayout, v)
				if err != nil {
					return fmt.Errorf("unable to convert %s to time.Time", v)
				}
				row = append(row, a)
			default:
				return fmt.Errorf("no mapping for %s", t)
			}
		}
	}
	dt.data = append(dt.data, row)
	return nil
}

//DeleteRow removes the row at the given row index
func (dt *DataTable) DeleteRow(RowID int) (err error) {
	dt.Lock()
	defer dt.Unlock()
	if RowID < 0 || RowID >= len(dt.data) {
		return fmt.Errorf("invalid row id")
	}
	dt.data = append(dt.data[:RowID], dt.data[RowID+1:]...)
	return nil
}

//GetCell returns the value by given column name and row number.
func (dt *DataTable) GetCell(ColumnName string, RowID int) (value interface{}, err error) {
	dt.RLock()
	defer dt.RUnlock()
	i, err := dt.ContainsColumn(ColumnName)
	if err != nil {
		return nil, err
	}
	if RowID < 0 || RowID >= len(dt.data) {
		return nil, fmt.Errorf("given row number: %d does not exist in the data table", RowID)
	}
	return dt.data[RowID][i], nil
}

//GetRow fetches the given id of the row and returns row with slice format.
func (dt *DataTable) GetRow(RowID int) (row []interface{}, err error) {
	dt.RLock()
	defer dt.RUnlock()
	if RowID < 0 || RowID >= len(dt.data) {
		return nil, fmt.Errorf("given row number: %d does not exist in the data table", RowID)
	}
	return dt.data[RowID], nil
}

//SetCell sets the value by given column name and row number
func (dt *DataTable) SetCell(ColumnName string, RowID int, Value sql.RawBytes) (err error) {
	//Using RLock to prevent single cell update operation locks the entire table
	dt.RLock()
	defer dt.RUnlock()
	i, err := dt.ContainsColumn(ColumnName)
	if err != nil {
		return err
	}
	if RowID < 0 || RowID >= len(dt.data) {
		return fmt.Errorf("given row number: %d does not exist in the data table", RowID)
	}
	expectedType, mapped := sql2golang[dt.columns[i].dataType]
	if !mapped {
		expectedType = "string"
	}
	valueType := reflect.TypeOf(Value).Name()
	if valueType != expectedType {
		return fmt.Errorf("the data type of given value (%s) does not match with column: %s (%s)", valueType, ColumnName, expectedType)
	}
	dt.data[RowID][i] = Value
	return nil
}

//AppendDataTable appends one datatable into another
func (dt *DataTable) AppendDataTable(NewDataTable *DataTable) (err error) {
	dt.Lock()
	defer dt.Unlock()
	err = compareSchema(dt, NewDataTable)
	if err != nil {
		return fmt.Errorf("unable to merge datatable\n%s", err.Error())
	}
	dt.data = append(dt.data, NewDataTable.data...)
	return nil
}

//Print prints the data table content to the console
func (dt *DataTable) Print() {
	dt.RLock()
	defer dt.RUnlock()
	for _, col := range dt.columns {
		fmt.Printf("%s\t", col.name)
	}
	fmt.Println()
	for _, row := range dt.data {
		for _, cell := range row {
			switch cell.(type) {
			case time.Time:
				fmt.Printf("%s\t", cell.(time.Time).Format(timeLayout))
			default:
				fmt.Printf("%v\t", cell)
			}
		}
		fmt.Println()
	}
}

//GenerateInsertCommands converts the data table into batched sql insert commands
func (dt *DataTable) GenerateInsertCommands(TableName string, BatchSize int) (cmds []string, err error) {
	if BatchSize <= 0 {
		return nil, fmt.Errorf("invalid batch size number")
	}

	//Grant read lock
	dt.RLock()
	defer dt.RUnlock()

	var cols int
	if len(dt.data) == 0 {
		return cmds, fmt.Errorf("empty datatable")
	} else {
		cols = len(dt.data[0])
	}

	//Generate header
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("INSERT INTO %s (", TableName))
	for i := 0; i < len(dt.columns); i++ {
		buffer.WriteString(dt.columns[i].name)
		if i < len(dt.columns)-1 {
			buffer.WriteString(",")
		}
	}
	buffer.WriteString(") VALUES\n")
	cmdHeader := buffer.String()

	//Generate values
	var rows int
	for i := 0; i < len(dt.data); i++ {
		buffer.WriteString("(")
		for j := 0; j < cols; j++ {
			if dt.data[i][j] == nil {
				buffer.WriteString("NULL")
			} else {
				switch dt.data[i][j].(type) {
				case string:
					buffer.WriteString(fmt.Sprintf("'%s'", strings.Replace(dt.data[i][j].(string), "'", "''", -1)))
				case int64:
					buffer.WriteString(strconv.FormatInt(dt.data[i][j].(int64), 10))
				case float64:
					buffer.WriteString(strconv.FormatFloat(dt.data[i][j].(float64), 'f', -1, 64))
				case bool:
					if dt.data[i][j].(bool) {
						buffer.WriteString("1")
					} else {
						buffer.WriteString("0")
					}
				case time.Time:
					buffer.WriteString(fmt.Sprintf("'%s'", dt.data[i][j].(time.Time).Format(timeLayout)))
				}
			}
			if j < len(dt.columns)-1 {
				//the cell does not belong to last column, add comma as column separator
				buffer.WriteString(",")
			} else {
				buffer.WriteString(")")
			}
		}
		rows++
		if i < len(dt.data)-1 && rows < BatchSize {
			//add comma as row separator if meets the conditions below
			// 1) row is not the last row of datatable
			// 2) current rows in the insert command has not exceeded the BatchSize
			buffer.WriteString(",\n")
		} else {
			//package the batch commands
			buffer.WriteString(";\n")
			cmds = append(cmds, buffer.String())
			buffer.Reset()
			buffer.WriteString(cmdHeader)
			rows = 0
		}
	}
	return cmds, nil
}

func (dt *DataTable) flushColumnIndex() {
	colIndex := make(map[string]int, 0)
	for i, col := range dt.columns {
		colIndex[col.name] = i
	}
	dt.colIndex = colIndex
}

func (dt *DataTable) newEmptyRow() (rowptr []interface{}) {
	colCounts := len(dt.columns)
	values := make([]interface{}, colCounts)
	valuePtrs := make([]interface{}, colCounts)
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	dt.data = append(dt.data, values)
	return valuePtrs
}

//NewDataTable initializes a new data table object.
func NewDataTable() *DataTable {
	dt := new(DataTable)
	dt.columns = make([]Column, 0)
	dt.data = make([][]interface{}, 0, 0)
	dt.colIndex = make(map[string]int, 0)
	return dt
}

//FillDataTable converts the sql.Rows object into data table structure.
func FillDataTable(Rows *sql.Rows) (*DataTable, error) {
	dt := NewDataTable()
	colName, _ := Rows.Columns()
	colTypes, _ := Rows.ColumnTypes()
	for i, col := range colTypes {
		length, _ := col.Length()
		decimalPrecision := int64(0)
		nullable, _ := colTypes[i].Nullable()
		if col.DatabaseTypeName() == "DECIMAL" {
			decimalPrecision, length, _ = col.DecimalSize()
		}
		col := NewColumn(colName[i], col.DatabaseTypeName(), length, decimalPrecision, nullable)
		dt.AddColumn(*col)
	}
	rowid := 0
	for Rows.Next() {
		valuePtrs := dt.newEmptyRow()
		err := Rows.Scan(valuePtrs...)
		if err != nil {
			return nil, fmt.Errorf("error on filling row: %d\n%s", rowid, err.Error())
		}
		//Some of the driver returns everyting in []int8. Code below will do the conversion according to the column defination.
		for i := range dt.columns {
			if dt.data[rowid][i] != nil {
				switch dt.data[rowid][i].(type) {
				case []uint8:
					dt.data[rowid][i] = string(dt.data[rowid][i].([]byte))
				case time.Time:
					dt.data[rowid][i] = dt.data[rowid][i].(time.Time).Format(timeLayout)
				}
			}
		}
		rowid++
	}
	return dt, nil
}

func compareSchema(dt1, dt2 *DataTable) (err error) {
	if len(dt1.columns) != len(dt2.columns) {
		return fmt.Errorf("mismatch count counts, %d vs %d", len(dt1.columns), len(dt2.columns))
	}
	for i, col1 := range dt1.columns {
		col2 := dt2.columns[i]
		if col1.name != col2.name {
			return fmt.Errorf("mismatch column name, %s vs %s", col1.name, col2.name)
		}
		if col1.dataType != col2.dataType {
			return fmt.Errorf("column: %s does not have same data type, %s vs %s", col1.name, col1.dataType, col2.dataType)
		}
		if col1.dataLength != col2.dataLength {
			return fmt.Errorf("column: %s does not have same data length, %d vs %d", col1.name, col1.dataLength, col2.dataLength)
		}
		if col1.decimalPrecision != col2.decimalPrecision {
			return fmt.Errorf("column: %s does not have same decimal precision, %d vs %d", col1.name, col1.decimalPrecision, col2.decimalPrecision)
		}
		if col1.nullable != col2.nullable {
			return fmt.Errorf("column: %s does not have same nullable setting", col1.name)
		}
	}
	return nil
}
