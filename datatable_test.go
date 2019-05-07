package dbutil

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func Test_AppendDataTable(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	queue := make(chan *DataTable, 10)
	go func() {
		var dt1 *DataTable
		for {
			result, ok := <-queue
			if !ok {
				break
			}
			if dt1 == nil {
				dt1 = result
			} else {
				err := dt1.AppendDataTable(result)
				if err != nil {
					fmt.Printf(err.Error())
				}
			}
		}
		dt1.Print()
		wg.Done()
	}()

	for i := 0; i < 10; i++ {
		dt := NewDataTable()
		col1 := NewColumn("id", "bigint", 0, 0, false)
		col2 := NewColumn("name", "varchar", 10, 0, false)
		col3 := NewColumn("data_float", "float", 0, 0, false)
		col4 := NewColumn("data_decimal", "decimal", 10, 2, false)
		col5 := NewColumn("insert_dt", "datetime", 0, 0, false)
		dt.AddColumn(*col1)
		dt.AddColumn(*col2)
		dt.AddColumn(*col3)
		dt.AddColumn(*col4)
		dt.AddColumn(*col5)
		err := dt.AppendRow(int64(i), "AAA", float64(1.1), float64(2.22), time.Now())
		if err != nil {
			fmt.Println(err.Error())
		}
		queue <- dt
	}
	close(queue)
	wg.Wait()
}

func Test_DataTableDiff(t *testing.T) {
	dt := NewDataTable()
	dt.AddColumn(*NewColumn("key1", "VARCHAR", 10, 0, false))
	dt.AddColumn(*NewColumn("key2", "INT", 0, 0, false))
	dt.AddColumn(*NewColumn("v_time", "DATETIME", 10, 0, false))
	dt.AddColumn(*NewColumn("v_bigint", "BIGINT", 10, 0, false))
	dt.AddColumn(*NewColumn("v_float", "FLOAT", 0, 0, false))
	dt.AddColumn(*NewColumn("v_string", "VARCHAR", 10, 0, false))
	base, err := dt.Clone()
	if err != nil {
		fmt.Println(err.Error())
	}
	dt.AppendRowFromString("svr01", "3306", "2019-01-01 00:00:00", "100", "100.01", "ABC")
	dt.AppendRowFromString("svr01", "3306", "2019-01-01 00:01:00", "111", "101.02", "DEF")
	dt.AppendRowFromString("svr01", "3307", "2019-01-01 00:02:00", "102", "100.03", "EFG")
	dt.AppendRowFromString("svr02", "3306", "2019-01-01 00:03:00", "103", "100.04", "FGH")
	base.AppendRowFromString("svr01", "3306", "2019-01-01 00:00:30", "105", "100.11", "ZZZ")
	base.AppendRowFromString("svr02", "3306", "2019-01-01 00:03:00", "103", "100.04", "DDD")
	result, err := dt.Diff(base, 2)
	if err != nil {
		fmt.Println(err.Error())
	}
	result.Print()
}
