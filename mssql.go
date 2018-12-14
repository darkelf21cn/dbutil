package dbutil

import (
	"fmt"
)

type Mssql struct {
	InstanceName string
	DatabaseName string
	UserName     string
	Password     string
}

func (m *Mssql) TestConnectivity() {
	fmt.Println("Testing MSSQL connectivity.")
}

func (m *Mssql) ExecuteSql(Sql string) {
	fmt.Println("Executing MSSQL query.")
}
