package dbutil

import (
	"fmt"
)

type Mysql struct {
	InstanceName string
	DatabaseName string
	UserName     string
	Password     string
}

func (m *Mysql) TestConnectivity() {
	fmt.Println("Testing MySQL connectivity.")
}

func (m *Mysql) ExecuteSql(Sql string) {
	fmt.Println("Executing MySQL query.")
}
