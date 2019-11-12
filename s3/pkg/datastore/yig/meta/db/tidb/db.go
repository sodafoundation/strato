package tidb

import (
	"database/sql"
)

type Tidb struct {
	DB *sql.DB
}

func (t *Tidb) Close() {
	t.DB.Close()
}
