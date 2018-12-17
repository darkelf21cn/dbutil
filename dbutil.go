package dbutil

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

//DBMS is the abstraction struct of different database drivers.
type DBMS struct {
	retryIntervalSec int
	maxRetries       int
	conn             *sql.DB
	ctx              context.Context
	dbInstance       interface {
		open() (*sql.DB, error)
	}
}

//NewDBMS initializes the DBMS struct
func NewDBMS(Ctx context.Context, RetryIntervalSec, MaxRetries int, DBInstance interface {
	open() (*sql.DB, error)
}) (db *DBMS) {
	db = new(DBMS)
	db.dbInstance = DBInstance
	db.ctx = Ctx
	db.retryIntervalSec = RetryIntervalSec
	db.maxRetries = MaxRetries
	return db
}

//Open invokes the open function in interface.
func (db *DBMS) Open() (err error) {
	db.conn, err = db.dbInstance.open()
	return err
}

//Close closes the database connection.
func (db *DBMS) Close(conn *sql.DB) (err error) {
	err = conn.Close()
	if err != nil {
		return err
	}
	return nil
}

//Execute executes the sql query with out results.
func (db *DBMS) Execute(SQL string) (err error) {
	retries := 0
	for retries <= db.maxRetries {
		err = db.conn.PingContext(db.ctx)
		if err != nil {
			retries++
			time.Sleep(time.Duration(db.maxRetries) * time.Millisecond)
			continue
		}
		_, err = db.conn.ExecContext(db.ctx, SQL)
		if err != nil {
			retries++
			time.Sleep(time.Duration(db.maxRetries) * time.Millisecond)
			continue
		}
	}
	if retries > db.maxRetries {
		return fmt.Errorf("execution failed after %d retries. the last error was:\n%s", db.maxRetries, err)
	}
	return nil
}

//Query executes the sql query and fill the results into data table.
func (db *DBMS) Query(SQL string) (dt *DataTable, err error) {
	retries := 0
	var rows *sql.Rows
	for retries <= db.maxRetries {
		stmt, err := db.conn.PrepareContext(db.ctx, SQL)
		defer stmt.Close()
		if err != nil {
			retries++
			time.Sleep(time.Duration(db.maxRetries) * time.Millisecond)
			continue
		}
		rows, err = stmt.QueryContext(db.ctx)
		if err != nil {
			retries++
			time.Sleep(time.Duration(db.maxRetries) * time.Millisecond)
			continue
		}
		break
	}
	if retries > db.maxRetries {
		return nil, fmt.Errorf("execution failed after %d retries. the last error was:\n%s", db.maxRetries, err.Error())
	}
	dt, err = FillDataTable(rows)
	if err != nil {
		return nil, err
	}
	return dt, nil
}
