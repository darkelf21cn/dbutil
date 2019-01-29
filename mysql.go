package dbutil

import (
	"database/sql"
	"fmt"
	"strings"

	//Import MySQL driver
	_ "github.com/go-sql-driver/mysql"
)

//MySQL contains MySQL connection related properties.
type MySQL struct {
	connstr string
	addr    string
}

//NewMySQL initializes a new instance of MySQL connection.
func NewMySQL(Hostname string, Port int, Database, UserName, Password string, Parameters ...string) (db *MySQL) {
	db = new(MySQL)
	//[username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
	db.addr = fmt.Sprintf("%s:%d", Hostname, Port)
	connstr := fmt.Sprintf("%s:%s@tcp(%s)/%s", UserName, Password, db.addr, Database)
	if len(Parameters) > 0 {
		parm := ""
		for _, p := range Parameters {
			parm += fmt.Sprintf("&%s", p)
		}
		parm = strings.TrimLeft(parm, "&")
		db.connstr = fmt.Sprintf("%s?%s", connstr, parm)
	} else {
		db.connstr = connstr
	}
	return db
}

//open opens a connection to the MySQL.
func (db *MySQL) open() (conn *sql.DB, err error) {
	return sql.Open("mysql", db.connstr)
}

//address returns the address of mysql instance
func (db *MySQL) address() string {
	return db.addr
}
