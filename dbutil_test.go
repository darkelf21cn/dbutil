package dbutil

import (
	"context"
	"fmt"
	"testing"
)

func Test_Query(t *testing.T) {
	var db *DBMS
	var dt *DataTable
	var err error
	var ins []string

	//MSSQL
	mssql := NewMSSQL("NTGSQLMONITOR02", "Nova", "DashboardAcct", "G5agc#FdM$+R", "AutoPilot")
	db = NewDBMS(context.Background(), 10, 2, mssql)
	err = db.Open()
	if err != nil {
		fmt.Printf("failed to open database:\n%s\n", err.Error())
		return
	}

	dt, err = db.Query("SELECT * FROM sys.tables;")
	if err != nil {
		fmt.Printf("filed to run the sql query:\n%s\n", err.Error())
		return
	}
	ins, _ = dt.GenerateInsertCommands("test_mssql", 10000)
	fmt.Printf("%v\n", ins)

	//MySQL
	mysql := NewMySQL("10.215.70.141", 3306, "nova", "kwang", "k!m!w@ngAP", "interpolateParams=true", "parseTime=true")
	db = NewDBMS(context.Background(), 10, 2, mysql)
	err = db.Open()
	if err != nil {
		fmt.Printf("failed to open database:\n%s\n", err.Error())
		return
	}

	dt, err = db.Query("SELECT * FROM mysql.user;")
	if err != nil {
		fmt.Printf("filed to run the sql query:\n%s\n", err.Error())
		return
	}
	ins, _ = dt.GenerateInsertCommands("test_mysql", 10000)
	fmt.Printf("%v\n", ins)
}
