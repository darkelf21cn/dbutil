package dbutil

import (
	"context"
	"fmt"
	"testing"
)

func Test_Query(t *testing.T) {
	ctx := context.Background()
	var dt *DataTable
	var err error

	//MSSQL
	insMssql := NewMSSQL("NTGSQLMONITOR02", "Nova", "DashboardAcct", "G5agc#FdM$+R", "AutoPilot")
	mssql := NewDBMS(ctx, 10, 2, insMssql)
	err = mssql.Open()
	if err != nil {
		fmt.Printf("failed to open database:\n%s\n", err.Error())
		return
	}
	defer mssql.Close()
	dt, err = mssql.Query("SELECT * FROM sys.tables;")
	if err != nil {
		fmt.Printf("filed to run the sql query:\n%s\n", err.Error())
		return
	}
	dt.Print()
	err = mssql.Execute("SELECT 1 as col1;")
	if err != nil {
		fmt.Printf("filed to run the sql query:\n%s\n", err.Error())
		return
	}

	err = mssql.ExecuteBatch([]string{"SELECT 1 as col1;", "SELECT 2 as col2;"}, true)
	if err != nil {
		fmt.Printf("filed to run the sql query:\n%s\n", err.Error())
		return
	}

	//MySQL
	insMysql := NewMySQL("10.215.70.141", 3306, "nova", "kwang", "k!m!w@ngAP", "interpolateParams=true", "parseTime=true")
	mysql := NewDBMS(context.Background(), 10, 2, insMysql)
	err = mysql.Open()
	if err != nil {
		fmt.Printf("failed to open database:\n%s\n", err.Error())
		return
	}
	defer mysql.Close()

	dt, err = mysql.Query("SELECT * FROM mysql.user;")
	if err != nil {
		fmt.Printf("filed to run the sql query:\n%s\n", err.Error())
		return
	}
	dt.Print()

	err = mysql.Execute("SELECT 1 as col1;")
	if err != nil {
		fmt.Printf("filed to run the sql query:\n%s\n", err.Error())
		return
	}

	err = mysql.ExecuteBatch([]string{"SELECT 1 as col1;", "SELECT 2 as col2;"}, true)
	if err != nil {
		fmt.Printf("filed to run the sql query:\n%s\n", err.Error())
		return
	}
}

func Test_BlukCopy(t *testing.T) {
	var dt *DataTable
	var err error
	ctx := context.Background()
	insMssql := NewMSSQL("NTGSQLMONITOR02", "MSSQLMonitoringDW", "DashboardAcct", "G5agc#FdM$+R", "AutoPilot")
	mssql := NewDBMS(ctx, 10, 2, insMssql)
	err = mssql.Open()
	if err != nil {
		fmt.Printf("failed to open database:\n%s\n", err.Error())
		return
	}
	defer mssql.Close()

	insMysql := NewMySQL("10.215.70.141", 3306, "nova", "kwang", "k!m!w@ngAP", "interpolateParams=true", "parseTime=true")
	mysql := NewDBMS(context.Background(), 10, 2, insMysql)
	err = mysql.Open()
	if err != nil {
		fmt.Printf("failed to open database:\n%s\n", err.Error())
		return
	}
	defer mysql.Close()

	dt, err = mssql.Query("SELECT TOP 1000 * FROM dbo.mConnectivityHist;")
	if err != nil {
		fmt.Printf("filed to run the sql query:\n%s\n", err.Error())
		return
	}
	msg, err := mysql.BulkCopy(dt, "TestBcp", 0, false, true)
	if err != nil {
		fmt.Printf(err.Error())
		return
	}
	fmt.Printf(msg)
}
