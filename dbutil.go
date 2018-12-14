package dbutil

type Database interface {
	TestConnectivity()
	ExecuteSql(string)
}

func TestConnectivity(d Database) {
	d.TestConnectivity()
}

func ExecuteSql(d Database, sql string) {
	d.ExecuteSql(sql)
}
