package cockroach

import (
	"database/sql"
	"log"

	// PostgreSQL database driver for CockroachDB
	_ "github.com/lib/pq"
)

func openTable() (*table, error) {
	dbURL := "postgresql://root@localhost:26257/bleve?sslmode=disable"
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatal(err)
	}

	t := table{
		db:   db,
		name: "bleve",
	}
	return &t, nil
}

type table struct {
	db   *sql.DB
	name string
}

func (t *table) Upsert(key, value []byte) error {
	upsertSQL := "UPSERT INTO " + t.name + " (key, value) VALUES ($1, $2)"
	_, err := t.db.Exec(upsertSQL, key, value)
	return err
}

func (t *table) Delete(key []byte) error {
	deleteSQL := "DELETE FROM " + t.name + " WHERE key = $1"
	_, err := t.db.Exec(deleteSQL, key)
	return err
}

func (t *table) Get(key []byte) ([]byte, error) {
	getSQL := "SELECT value FROM " + t.name + " WHERE key = $1"
	val := []byte{}
	err := t.db.QueryRow(getSQL, key).Scan(&val)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return val, err
}

func (t *table) PrefixGet(prefix []byte) (keys, values [][]byte, err error) {
	prefixSQL := "SELECT key, value FROM " + t.name + " WHERE key >= $1 AND key <= $2 ORDER BY key"
	end := append(prefix, 0xff)
	rows, err := t.db.Query(prefixSQL, prefix, end)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var k, v []byte
		err := rows.Scan(&k, &v)
		if err != nil {
			return nil, nil, err
		}
		keys = append(keys, k)
		values = append(values, v)
	}
	return keys, values, nil
}

func (t *table) RangeGet(start, end []byte) (keys, values [][]byte, err error) {
	rangeSQL := "SELECT key, value FROM " + t.name + " WHERE key >= $1 and key < $2 ORDER BY key"
	rows, err := t.db.Query(rangeSQL, start, end)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var k, v []byte
		err := rows.Scan(&k, &v)
		if err != nil {
			return nil, nil, err
		}
		keys = append(keys, k)
		values = append(values, v)
	}
	return keys, values, nil
}

func (t *table) MultiGet(keys [][]byte) ([][]byte, error) {
	getSQL := "SELECT key, value FROM " + t.name + " WHERE key IN($1)"
	vmap := map[string][]byte{}
	rows, err := t.db.Query(getSQL, keys)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var k, v []byte
		err := rows.Scan(&k, &v)
		if err != nil {
			return nil, err
		}
		vmap[string(k)] = v
	}
	out := make([][]byte, len(keys))
	for i, k := range keys {
		out[i] = vmap[string(k)]
	}
	return out, err
}
