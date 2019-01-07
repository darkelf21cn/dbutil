package dbutil

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

//DBInstance is an interface used for opening different databases
type DBInstance interface {
	open() (*sql.DB, error)
}

//DBMS is the abstraction struct of different database drivers.
type DBMS struct {
	retryIntervalSec int
	maxRetries       int
	conn             *sql.DB
	ctx              context.Context
	dbInstance       DBInstance
}

//NewDBMS initializes the DBMS struct
func NewDBMS(Ctx context.Context, RetryIntervalSec, MaxRetries int, DBInstance DBInstance) (db *DBMS) {
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
func (db *DBMS) Close() (err error) {
	err = db.conn.Close()
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
			if db.ctx.Err() == context.Canceled {
				return err
			}
			retries++
			time.Sleep(time.Duration(db.retryIntervalSec) * time.Second)
			continue
		}
		_, err = db.conn.ExecContext(db.ctx, SQL)
		if err != nil {
			if db.ctx.Err() == context.Canceled {
				return err
			}
			retries++
			time.Sleep(time.Duration(db.retryIntervalSec) * time.Second)
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
		rows, err = db.query(SQL)
		if err != nil {
			if db.ctx.Err() == context.Canceled {
				return nil, err
			}
			retries++
			time.Sleep(time.Duration(db.retryIntervalSec) * time.Second)
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

func (db *DBMS) query(SQL string) (rows *sql.Rows, err error) {
	var stmt *sql.Stmt
	stmt, err = db.conn.PrepareContext(db.ctx, SQL)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	rows, err = stmt.QueryContext(db.ctx)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

//BulkCopy converts data table into batch of inserts. Then executes the insert commands.
func (db *DBMS) BulkCopy(Data *DataTable, TableName string, BatchSize int, SingleTransaction bool) (msg string, err error) {
	rowsInserted := int64(0)
	var cmds []string
	cmds, err = Data.GenerateInsertCommands(TableName, BatchSize)
	if err != nil {
		return "", err
	}
	bcpStart := time.Now()
	if SingleTransaction {
		var tx *sql.Tx
		tx, err = db.conn.BeginTx(db.ctx, nil)
		defer tx.Rollback()
		for _, cmd := range cmds {
			var r sql.Result
			r, err = tx.ExecContext(db.ctx, cmd)
			if err != nil {
				return "bulk copy failed", err
			}
			ra, _ := r.RowsAffected()
			rowsInserted += ra
		}
		tx.Commit()
	} else {
		for _, cmd := range cmds {
			var r sql.Result
			r, err = db.conn.ExecContext(db.ctx, cmd)
			if err != nil {
				return fmt.Sprintf("bulk copy failed, %d rows copied without rollback\n", rowsInserted), err
			}
			ra, _ := r.RowsAffected()
			rowsInserted += ra
		}
	}
	duration := int64(time.Since(bcpStart).Seconds())
	var copyRate int64
	if duration == 0 {
		copyRate = rowsInserted
	} else {
		copyRate = rowsInserted / duration
	}
	return fmt.Sprintf("%d rows copied at %d rows/sec\n", rowsInserted, copyRate), nil
}
