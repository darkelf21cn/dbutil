package dbutil

import (
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
	"TINYINT":       "int",
	"SMALLINT":      "int",
	"MEDIUMINT":     "int",
	"INT":           "int",
	"INTEGER":       "int",
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
	mutex    sync.Mutex
}

//ColumnCounts returns the column counts of the data table.
func (dt *DataTable) ColumnCounts() int {
	return len(dt.columns)
}

//RowCounts returns the row counts of the data table.
func (dt *DataTable) RowCounts() int {
	var v int
	if dt.ColumnCounts() == 0 {
		v = 0
	} else {
		v = len(dt.data)
	}
	return v
}

//ContainsColumn scans all the columns to check whether the given column exists in the data table.
func (dt *DataTable) ContainsColumn(ColumnName string) (colIndex int, err error) {
	i, ok := dt.colIndex[ColumnName]
	if !ok {
		return 0, fmt.Errorf("given column name: %s does not exist in the data table", ColumnName)
	}
	return i, nil
}

//AddColumn adds a new column to the datatable and set default values.
func (dt *DataTable) AddColumn(Column Column) (err error) {
	//Check duplication
	colCounts := len(dt.columns)
	for i := 0; i < colCounts; i++ {
		if dt.columns[i].Name() == Column.Name() {
			return fmt.Errorf("duplicate column: %s", Column.Name())
		}
	}

	//Add column and set default value.
	//Lock the datatable during the schema change.
	dt.mutex.Lock()
	dt.columns = append(dt.columns, Column)
	var v interface{}
	if Column.Nullable() {
		v = nil
	} else {
		switch sql2golang[Column.DataType()] {
		case "":
			v = ""
		case "bool":
			v = false
		case "int":
			v = int(0)
		case "int64":
			v = int64(0)
		case "float":
			v = float64(0.0)
		case "time.Time":
			v, _ = time.Parse(timeLayout, "1900-01-01 00:00:00")
		}
	}
	rowCounts := dt.RowCounts()
	for i := 0; i < rowCounts; i++ {
		dt.data[i][colCounts] = v
	}
	dt.flushColumnIndex()
	dt.mutex.Unlock()
	return nil
}

//AppendRow appends a new row to the data table.
func (dt *DataTable) AppendRow(Values ...interface{}) (err error) {
	if len(Values) != dt.ColumnCounts() {
		return fmt.Errorf("column counts mismatch")
	}
	for i, v := range Values {
		//Nullable validation
		if v == nil && dt.columns[i].Nullable() == false {
			return fmt.Errorf("column: %s does not allow null", dt.columns[i].Name())
		}
		//Datatype validation
		var ok bool
		switch sql2golang[dt.columns[i].DataType()] {
		case "":
			_, ok = v.(string)
		case "bool":
			_, ok = v.(bool)
		case "int":
			_, ok = v.(int)
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
	if len(Values) != dt.ColumnCounts() {
		return fmt.Errorf("column counts mismatch")
	}
	row := make([]interface{}, 0)
	for i, v := range Values {
		t := sql2golang[dt.columns[i].DataType()
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
		case "int":
			a, err := strconv.ParseInt(v, 10, 0)
			if err != nil {
				return fmt.Errorf("unable to convert %s to int", v)
			}
			row = append(row, int(a))
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
	dt.data = append(dt.data, row)
	return nil
}

//DeleteRow removes the row at the given row index
func (dt *DataTable) DeleteRow(RowID int) (err error) {
	if RowID < 0 || RowID >= dt.RowCounts() {
		return fmt.Errorf("invalid row id")
	}
	dt.data = append(dt.data[:RowID], dt.data[RowID+1:]...)
	return nil
}

//GetCellValue returns the value by given column name and row number.
func (dt *DataTable) GetCellValue(ColumnName string, RowID int) (value interface{}, isnull bool, err error) {
	i, err := dt.ContainsColumn(ColumnName)
	if err != nil {
		return nil, false, err
	}
	if RowID < 0 || RowID >= dt.RowCounts() {
		return nil, false, fmt.Errorf("given row number: %d does not exist in the data table", RowID)
	}
	if dt.data[RowID][i] == nil {
		return dt.data[RowID][i], true, nil
	}
	return dt.data[RowID][i], false, nil
}

//SetCellValue sets the value by given column name and row number
func (dt *DataTable) SetCellValue(ColumnName string, RowID int, Value sql.RawBytes) (err error) {
	i, err := dt.ContainsColumn(ColumnName)
	if err != nil {
		return err
	}
	if RowID < 0 || RowID >= dt.RowCounts() {
		return fmt.Errorf("given row number: %d does not exist in the data table", RowID)
	}
	expectedType, mapped := sql2golang[dt.columns[i].DataType()]
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

//Print prints the data table content to the console
func (dt *DataTable) Print() {
	for _, row := range dt.data {
		for _, cell := range row {
			fmt.Printf("%v\t", cell)
		}
		fmt.Println()
	}
}

func (dt *DataTable) flushColumnIndex() {
	colIndex := make(map[string]int, 0)
	for i, col := range dt.columns {
		colIndex[col.Name()] = i
	}
	dt.colIndex = colIndex
}

func (dt *DataTable) newEmptyRow() (rowptr []interface{}) {
	colCounts := dt.ColumnCounts()
	values := make([]interface{}, colCounts)
	valuePtrs := make([]interface{}, colCounts)
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	dt.data = append(dt.data, values)
	return valuePtrs
}

//GenerateInsertCommands converts the data table into batched sql insert commands
func (dt *DataTable) GenerateInsertCommands(TableName string, BatchSize int) (commands []string, err error) {
	if BatchSize <= 0 {
		return nil, fmt.Errorf("invalid batch size number")
	}

	cmds := make([]string, 0)
	//Generate header
	cmdHeader := fmt.Sprintf("INSERT INTO %s (", TableName)
	for _, col := range dt.columns {
		cmdHeader += fmt.Sprintf("%s,", col.Name())
	}
	cmdHeader = strings.TrimRight(cmdHeader, ",")
	cmdHeader = fmt.Sprintf("%s) VALUES\n", cmdHeader)

	//Generate values
	var cmd string
	var curSize int
	for _, row := range dt.data {
		cmd += "("
		for _, cell := range row {
			if cell == nil {
				cmd += "NULL, "
			}
			switch cell.(type) {
			case string:
				cmd += fmt.Sprintf("'%s', ", strings.Replace(cell.(string), "'", "''", -1))
			case int, int64:
				cmd += fmt.Sprintf("%d, ", cell)
			case float64:
				cmd += fmt.Sprintf("%f, ", cell)
			case bool:
				if cell.(bool) {
					cmd += fmt.Sprintf("%d, ", 1)
				} else {
					cmd += fmt.Sprintf("%d, ", 0)
				}
			case time.Time:
				cmd += fmt.Sprintf("'%s', ", cell.(time.Time).Format(timeLayout))
			}
		}
		cmd = strings.TrimRight(cmd, ", ")
		cmd += "),\n"
		curSize++
		if curSize >= BatchSize {
			cmd = strings.TrimRight(cmd, ",\n")
			cmd = fmt.Sprintf("%s%s;", cmdHeader, cmd)
			cmds = append(cmds, cmd)
			cmd = ""
			curSize = 0
		}
	}
	if cmd != "" {
		cmd = strings.TrimRight(cmd, ",\n")
		cmd = fmt.Sprintf("%s%s;", cmdHeader, cmd)
		cmds = append(cmds, cmd)
	}
	return cmds, nil
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
