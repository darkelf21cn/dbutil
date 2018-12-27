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
	defer mssql.Close()
	if err != nil {
		fmt.Printf("failed to open database:\n%s\n", err.Error())
		return
	}

	dt, err = mssql.Query("SELECT * FROM sys.tables;")
	if err != nil {
		fmt.Printf("filed to run the sql query:\n%s\n", err.Error())
		return
	}
	dt.Print()

	//MySQL
	insMysql := NewMySQL("10.215.70.141", 3306, "nova", "kwang", "k!m!w@ngAP", "interpolateParams=true", "parseTime=true")
	mysql := NewDBMS(context.Background(), 10, 2, insMysql)
	err = mysql.Open()
	defer mysql.Close()
	if err != nil {
		fmt.Printf("failed to open database:\n%s\n", err.Error())
		return
	}

	dt, err = mysql.Query("SELECT * FROM mysql.user;")
	if err != nil {
		fmt.Printf("filed to run the sql query:\n%s\n", err.Error())
		return
	}
	dt.Print()
}

func Test_BlukCopy(t *testing.T) {
	var dt *DataTable
	var err error
	ctx := context.Background()
	insMssql := NewMSSQL("NTGSQLMONITOR02", "MSSQLMonitoringDW", "DashboardAcct", "G5agc#FdM$+R", "AutoPilot")
	mssql := NewDBMS(ctx, 10, 2, insMssql)
	err = mssql.Open()
	defer mssql.Close()
	if err != nil {
		fmt.Printf("failed to open database:\n%s\n", err.Error())
		return
	}

	insMysql := NewMySQL("10.215.70.141", 3306, "nova", "kwang", "k!m!w@ngAP", "interpolateParams=true", "parseTime=true")
	mysql := NewDBMS(context.Background(), 10, 2, insMysql)
	err = mysql.Open()
	defer mysql.Close()
	if err != nil {
		fmt.Printf("failed to open database:\n%s\n", err.Error())
		return
	}

	dt, err = mssql.Query("SELECT TOP 100000 * FROM dbo.mConnectivityHist;")
	if err != nil {
		fmt.Printf("filed to run the sql query:\n%s\n", err.Error())
		return
	}
	msg, err := mysql.BulkCopy(dt, "TestBcp", 1000, false)
	if err != nil {
		fmt.Printf(err.Error())
		return
	}
	fmt.Printf(msg)
}
