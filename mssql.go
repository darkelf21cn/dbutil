package dbutil

import (
	"database/sql"
	"fmt"
	//Import MSSQL driver
	_ "github.com/denisenkom/go-mssqldb"
)

//MSSQL contains SQL Server connection related properties.
type MSSQL struct {
	connstr string
}

//NewMSSQL initializes a new instance of SQL Server connection.
func NewMSSQL(InstanceName, Database, UserName, Password, AppName string) (db *MSSQL) {
	db = new(MSSQL)
	db.connstr = fmt.Sprintf("server=%s;database=%s;user id=%s;password=%s;app name=%s;", InstanceName, Database, UserName, Password, AppName)
	return db
}

//Open opens a connection to the SQL Server.
func (db *MSSQL) open() (conn *sql.DB, err error) {
	return sql.Open("mssql", db.connstr)
}
