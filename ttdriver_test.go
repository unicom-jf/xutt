package xutt

import (
	"database/sql"
	"fmt"
	"math"

	// "math"
	"time"

	// "database/sql/driver"
	// "io"
	"log"
	// "os"
	// "strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	// _ "ttdriver" // Import the driver package (side-effects only
)

// options := ttdriver.Options {
// 	RequireEncryption: true,
// 	Wallet: "/Users/jf10/tt221mac/conf/wallets/sampledbqdCSWallet",
// 	CipherSuites: "SSL_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
// 	QueryTimeout: 3,
// 	Charset: "utf8",
// }
var options = Options {
	RequireEncryption: false,
	Charset: "utf8",
}

// var (
// 	ttc_host = os.Getenv("TTC_SERVER")
// 	ttc_port = os.Getenv("TTC_TCP_PORT")
// 	ttc_uid = os.Getenv("TTC_UID")
// 	ttc_pwd = os.Getenv("TTC_PWD")
// 	ttc_server_dsn = os.Getenv("TTC_SERVER_DSN")
// )

var param *ConnParam
var db *sql.DB
var err error
var rowid string

func init() {
	param , err = ConnParamByEnv()
	if err != nil {
		log.Fatalf("Error in connection parameters: %v", err)
	}
	// Disable logging during tests to keep the output clean
	// log.SetOutput(io.Discard)
	// info := ""
	// if ttc_host == "" {
	// 	info += "TTC_HOST undefined"
	// }
	// if ttc_port == "" {
	// 	if info != "" {
	// 		info += ", "
	// 	}
	// 	info += "TTC_PORT undefined"
	// }
	// if ttc_uid == "" {
	// 	if info != "" {
	// 		info += ", "
	// 	}
	// 	info += "TTC_UID undefined"
	// }
	// if ttc_pwd == "" {
	// 	if info != "" {
	// 		info += ", "
	// 	}
	// 	info += "TTC_PWD undefined"
	// }
	// if ttc_server_dsn == "" {
	// 	if info != "" {
	// 		info += ", "
	// 	}
	// 	info += "TTC_SERVER_DSN undefined"
	// }
	// if info != "" {
	// 	log.Fatalf("Error: %s. ", info)
	// }
}

func TestConnect(t *testing.T) {
	// port, err := strconv.Atoi(ttc_port)
	// if err != nil {
	// 	t.Fatalf("Invalid port number: %v", err)
	// }
	// dsn := ConnParam {
	// 	Host: ttc_host,
	// 	Port: port,
	// 	User: ttc_uid,
	// 	Password: ttc_pwd,
	// 	DBName: ttc_server_dsn,
	// 	Options: options,
	// }
	db, err = sql.Open("xutt", param.Dsn())
	if err != nil {
		t.Fatalf("Failed to open driver: %v", err)
	}
	err = db.Ping()
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
}

func TestCreateTable(t *testing.T) {
	_, err := db.Exec("DROP TABLE flds_test")
	if err != nil {
		log.Printf("Warning: Failed to drop table (may not exist): %v", err)
	}
	cmd := `create table flds_test (
    FLD_VARCHAR2                    VARCHAR2 (4000) NOT INLINE,
    FLD_NUMBER                      NUMBER,
    FLD_DATE                        DATE,
    FLD_BLOB                        BLOB,
    FLD_CLOB                        CLOB,
    FLD_NCLOB                       NCLOB,
    FLD_BINARY_DOUBLE               BINARY_DOUBLE,
    FLD_BINARY_FLOAT                BINARY_FLOAT,
    FLD_DOUBLEPRECISION             FLOAT (126),
    FLD_FLOAT                       FLOAT (126),
    FLD_INTEGER                     NUMBER (38),
    FLD_REAL                        FLOAT (63),
    FLD_ROWID                       ROWID,
    FLD_SMALLINT                    NUMBER (38),
    FLD_TIME                        TT_TIME,
    FLD_TIMESTAMP                   TIMESTAMP (6),
    FLD_TT_BIGINT                   TT_BIGINT,
    FLD_TT_DATE                     TT_DATE,
    FLD_TT_INTEGER                  TT_INTEGER,
    FLD_TT_NVARCHAR                 TT_NVARCHAR (100) NOT INLINE,
    FLD_TT_SMALLINT                 TT_SMALLINT,
    FLD_TT_TIMESTAMP                TT_TIMESTAMP,
    FLD_TT_TINYINT                  TT_TINYINT,
    FLD_TT_VARCHAR                  TT_VARCHAR (100) INLINE,
    FLD_VARBINARY                   VARBINARY (1000) NOT INLINE,
    FLD_TT_NCHAR                    TT_NCHAR (100),
    FLD_NCHAR                       NCHAR (100),
    FLD_TT_CHAR                     TT_CHAR (100),
    FLD_CHAR                        CHAR (100),
    FLD_BINARY                      BINARY (10))`
		// log.Println("Create table command:", cmd)
	_, err = db.Exec(cmd)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	var count int
	db.QueryRow("SELECT COUNT(*) FROM flds_test").Scan(&count)
	assert.Equal(t, 0, count, "Table should be empty after creation")
}

func TestCharFlds(t *testing.T) {
	vals := []string{"char1-中文", "char2-สวัสดี", "char3-GÖ", "char4-こん", "char5-ال", "char6-স", "char7-த", "char8-Señor"}
	cmd := `insert into flds_test (
		FLD_VARCHAR2, FLD_CLOB, FLD_NCLOB, FLD_TT_NVARCHAR, FLD_TT_VARCHAR,
		 FLD_TT_NCHAR, FLD_TT_CHAR, FLD_CHAR) 
		values (:varchar2, ?, ?, :nvarchar, :varchar, :varchar, ?, ?)`
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	res, err := tx.Exec(cmd, 
		sql.NamedArg{Name: "varchar", Value: vals[4]},
		sql.NamedArg{Name: "nvarchar", Value: vals[3]},
		sql.NamedArg{Name: "varchar2", Value: vals[0]},
		vals[1], vals[2], vals[6], vals[7]) 
	
	if err != nil {
		t.Fatalf("Failed to insert char fields: %v", err)
	}
	rowsAffected, _ := res.RowsAffected()
	assert.Equal(t, int64(1), rowsAffected, "One row should be inserted")
	
	rows, err := tx.Query(`SELECT FLD_VARCHAR2, FLD_CLOB, FLD_NCLOB, FLD_TT_NVARCHAR, 
		FLD_TT_VARCHAR, FLD_TT_NCHAR, FLD_TT_CHAR, FLD_CHAR, rowid FROM flds_test
		where FLD_VARCHAR2=:varchar2 and FLD_CLOB like ? and FLD_NCLOB like ? and FLD_TT_NVARCHAR=:nvarchar and 
		FLD_TT_VARCHAR=:varchar and FLD_TT_NCHAR=:varchar and FLD_TT_CHAR like ? and FLD_CHAR like ?`,
		sql.NamedArg{Name: "varchar", Value: vals[4]},
	sql.NamedArg{Name: "nvarchar", Value: vals[3]},
	sql.NamedArg{Name: "varchar2", Value: vals[0]},
	"%" + vals[1] + "%", "%" + vals[2] + "%", "%" + vals[6] + "%", "%" + vals[7] + "%")
	if err != nil {
		t.Fatalf("Failed to select char fields: %v", err)
	}
	defer func() {
		// log.Println("Closing rows: first")
		rows.Close()
	}()
	cnt := 0
	flds := make([]any, len(vals))
	for rows.Next() {
		cnt++
		err = rows.Scan(&flds[0], &flds[1], &flds[2], &flds[3], &flds[4], &flds[5], &flds[6], &flds[7], &rowid)
		if err != nil {
			t.Fatalf("Failed to scan char fields: %v", err)
		}
		for i, v := range vals {
			_v := v
			if i == 5 {
				_v = vals[4]
			}
			assert.Equal(t, _v, (flds[i].(string))[:len(_v)], "Field %d should match", i + 1)
		}
	}
	assert.Equal(t, 1, cnt, "One row should be selected")
	if err = rows.Err(); err != nil {
		t.Fatalf("Row iteration error: %v", err)
	}
	res, err = tx.Exec(`update flds_test
		set FLD_VARCHAR2=null, FLD_CLOB=null, FLD_NCLOB=null, FLD_TT_NVARCHAR=null, 
		FLD_TT_VARCHAR=null, FLD_TT_NCHAR=null, FLD_TT_CHAR=null, FLD_CHAR=null
		where FLD_VARCHAR2=:varchar2 and FLD_CLOB like ? and FLD_NCLOB like ? and FLD_TT_NVARCHAR=:nvarchar and 
		FLD_TT_VARCHAR=:varchar and FLD_TT_NCHAR=:varchar and FLD_TT_CHAR like ? and FLD_CHAR like ?`,
		sql.NamedArg{Name: "varchar", Value: vals[4]},
		sql.NamedArg{Name: "nvarchar", Value: vals[3]},
		sql.NamedArg{Name: "varchar2", Value: vals[0]},
		"%" + vals[1] + "%", "%" + vals[2] + "%", "%" + vals[6] + "%", "%" + vals[7] + "%")
	if err != nil {
		t.Fatalf("Failed to update char fields: %v", err)
	}
	rowsAffected, _ = res.RowsAffected()
	assert.Equal(t, int64(1), rowsAffected, "One row should be updated")

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	rows, err = db.Query(`SELECT FLD_VARCHAR2, FLD_CLOB, FLD_NCLOB, FLD_TT_NVARCHAR, 
		FLD_TT_VARCHAR, FLD_TT_NCHAR, FLD_TT_CHAR, FLD_CHAR FROM flds_test
		where FLD_VARCHAR2 is null and FLD_CLOB is null and FLD_NCLOB is null and FLD_TT_NVARCHAR is null and 
		FLD_TT_VARCHAR is null and FLD_TT_NCHAR is null and FLD_TT_CHAR is null and FLD_CHAR is null and rowid=?`, rowid)
	if err != nil {
		t.Fatalf("Failed to select char fields: %v", err)
	}
	defer func() {
		// log.Println("Closing rows: second")
		rows.Close()
	}()
	cnt = 0
	for rows.Next() {
		cnt++
		err = rows.Scan(&flds[0], &flds[1], &flds[2], &flds[3], &flds[4], &flds[5], &flds[6], &flds[7])
		if err != nil {
			t.Fatalf("Failed to scan char fields: %v", err)
		}
		for i, v := range flds {
			assert.Nil(t, v, "Field %d should be null", i + 1)
		}
	}
	assert.Equal(t, 1, cnt, "One row should be selected")
	if err = rows.Err(); err != nil {
		t.Fatalf("Row iteration error: %v", err)
	}
	tx, err = db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	res, err = tx.Exec("DELETE FROM flds_test WHERE rowid=?", rowid)
	if err != nil {
		t.Fatalf("Failed to delete test row: %v", err)
	}
	rowsAffected, _ = res.RowsAffected()
	assert.Equal(t, int64(1), rowsAffected, "One row should be deleted")
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit delete transaction: %v", err)
	}
}

func TestIntegerFlds(t *testing.T) {
	vals := []int64{1, 2, 3, 4, 5, 6, 7}
	cmd := `insert into flds_test (
		FLD_NUMBER, FLD_INTEGER, FLD_SMALLINT, FLD_TT_BIGINT, FLD_TT_INTEGER, 
		FLD_TT_SMALLINT, FLD_TT_TINYINT) 
		values (:num, ?, ?, :tt_bigint, :tt_int, ?, ?)`
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	res, err := tx.Exec(cmd, 
		sql.NamedArg{Name: "num", Value: vals[0]},
		sql.NamedArg{Name: "tt_bigint", Value: vals[3]},
		sql.NamedArg{Name: "tt_int", Value: vals[4]},
		vals[1], vals[2], vals[5], vals[6]) 
	
	if err != nil {
		t.Fatalf("Failed to insert char fields: %v", err)
	}
	rowsAffected, _ := res.RowsAffected()
	assert.Equal(t, int64(1), rowsAffected, "One row should be inserted")
	
	rows, err := tx.Query(`SELECT FLD_NUMBER, FLD_INTEGER, FLD_SMALLINT, FLD_TT_BIGINT, FLD_TT_INTEGER, 
	FLD_TT_SMALLINT, FLD_TT_TINYINT, rowid FROM flds_test
		where FLD_NUMBER=:num and FLD_INTEGER=? and FLD_SMALLINT=? and FLD_TT_BIGINT=:tt_bigint and FLD_TT_INTEGER=:tt_int and 
		FLD_TT_SMALLINT=? and FLD_TT_TINYINT=?`,
		sql.NamedArg{Name: "num", Value: vals[0]},
		sql.NamedArg{Name: "tt_bigint", Value: vals[3]},
		sql.NamedArg{Name: "tt_int", Value: vals[4]},
		vals[1], vals[2], vals[5], vals[6])
	if err != nil {
		t.Fatalf("Failed to select integer fields: %v", err)
	}
	defer rows.Close()
	cnt := 0
	flds := make([]any, len(vals))
	for rows.Next() {
		cnt++
		err = rows.Scan(&flds[0], &flds[1], &flds[2], &flds[3], &flds[4], &flds[5], &flds[6], &rowid)
		if err != nil {
			t.Fatalf("Failed to scan integer fields: %v", err)
		}
		for i, v := range vals {
			if i == 0 {
				assert.Equal(t, v, int64(flds[i].(float64)), "Field %d should match", i + 1)
			} else {
				assert.Equal(t, v, flds[i].(int64), "Field %d should match", i + 1)
			}
		}
	}
	assert.Equal(t, 1, cnt, "One row should be selected")
	if err = rows.Err(); err != nil {
		t.Fatalf("Row iteration error: %v", err)
	}
	res, err = tx.Exec(`update flds_test
		set FLD_NUMBER=null, FLD_INTEGER=null, FLD_SMALLINT=null, FLD_TT_BIGINT=null, FLD_TT_INTEGER=null, 
		FLD_TT_SMALLINT=null, FLD_TT_TINYINT=null
			where FLD_NUMBER=:num and FLD_INTEGER=? and FLD_SMALLINT=? and FLD_TT_BIGINT=:tt_bigint and FLD_TT_INTEGER=:tt_int and 
			FLD_TT_SMALLINT=? and FLD_TT_TINYINT=?`,
			sql.NamedArg{Name: "num", Value: vals[0]},
			sql.NamedArg{Name: "tt_bigint", Value: vals[3]},
			sql.NamedArg{Name: "tt_int", Value: vals[4]},
			vals[1], vals[2], vals[5], vals[6])
	if err != nil {
		t.Fatalf("Failed to update integer fields: %v", err)
	}
	rowsAffected, _ = res.RowsAffected()
	assert.Equal(t, int64(1), rowsAffected, "One row should be updated")

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit update transaction: %v", err)
	}

	rows, err = db.Query(`SELECT FLD_NUMBER, FLD_INTEGER, FLD_SMALLINT, FLD_TT_BIGINT, FLD_TT_INTEGER, 
	FLD_TT_SMALLINT, FLD_TT_TINYINT FROM flds_test
		where FLD_NUMBER is null and FLD_INTEGER is null and FLD_SMALLINT is null and FLD_TT_BIGINT is null and FLD_TT_INTEGER is null and 
		FLD_TT_SMALLINT is null and FLD_TT_TINYINT is null and rowid=?`, rowid)
	if err != nil {
		t.Fatalf("Failed to select integer fields: %v", err)
	}
	defer rows.Close()
	cnt = 0
	for rows.Next() {
		cnt++
		err = rows.Scan(&flds[0], &flds[1], &flds[2], &flds[3], &flds[4], &flds[5], &flds[6])
		if err != nil {
			t.Fatalf("Failed to scan integer fields: %v", err)
		}
		for i, v := range flds {
			assert.Nil(t, v, "Field %d should be null", i + 1)
		}
	}
	assert.Equal(t, 1, cnt, "One row should be selected")
	if err = rows.Err(); err != nil {
		t.Fatalf("Row iteration error: %v", err)
	}
	tx, err = db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	res, err = tx.Exec("DELETE FROM flds_test WHERE rowid=?", rowid)
	if err != nil {
		t.Fatalf("Failed to delete test row: %v", err)
	}
	rowsAffected, _ = res.RowsAffected()
	assert.Equal(t, int64(1), rowsAffected, "One row should be deleted")
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit delete transaction: %v", err)
	}
}

func TestDatetimeFlds(t *testing.T) {
	// FLD_TT_DATE, date only
	// FLD_TIME, time only
	// FLD_DATE, date and time
	// FLD_TIMESTAMP, FLD_TT_TIMESTAMP, date and time with fractional seconds
	// timesten no timezone support, 
	const FLD_DATE = 0
	const FLD_TIME = 1 //TTTime
	const FLD_TIMESTAMP = 2
	const FLD_TT_DATE = 3
	const FLD_TT_TIMESTAMP = 4
	// nanosecond := 123456789
	// backend saved as milliseconds, FLD_TIMESTAMP:rounded, FLD_TT_TIMESTAMP:truncated
	// date's elements: year, month, day, hour, min, sec, nsec
	nanosecond := 123456789
	dt := time.Date(2023, time.October, 15, 10, 30, 45, nanosecond, time.UTC)
	cmd := `insert into flds_test (
		FLD_DATE, FLD_TIME, FLD_TIMESTAMP, FLD_TT_DATE, FLD_TT_TIMESTAMP) 
		values (:date, ?, ?, :tt_date, :tt_timestamp)`
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	res, err := tx.Exec(cmd, 
		sql.NamedArg{Name: "date", Value: dt},
		sql.NamedArg{Name: "tt_date", Value: dt},
		sql.NamedArg{Name: "tt_timestamp", Value: dt},
		dt, dt) 
	
	if err != nil {
		t.Fatalf("Failed to insert time fields: %v", err)
	}
	rowsAffected, _ := res.RowsAffected()
	assert.Equal(t, int64(1), rowsAffected, "One row should be inserted")
	tx.Commit()
	tx, err = db.Begin()
	rows, err := tx.Query(`SELECT FLD_DATE, FLD_TIME, FLD_TIMESTAMP, FLD_TT_DATE, FLD_TT_TIMESTAMP, rowid FROM flds_test
		where FLD_DATE=:date and FLD_TIME=? and FLD_TIMESTAMP=? and FLD_TT_DATE=:tt_date and FLD_TT_TIMESTAMP=:tt_timestamp`,
		sql.NamedArg{Name: "date", Value: dt},
		sql.NamedArg{Name: "tt_date", Value: dt},
		sql.NamedArg{Name: "tt_timestamp", Value: dt},
		dt, dt)
	if err != nil {
		t.Fatalf("Failed to select time fields: %v", err)
	}
	defer rows.Close()
	cnt := 0
	flds := make([]any, 5)
	for rows.Next() {
		cnt++
		err = rows.Scan(&flds[0], &flds[1], &flds[2], &flds[3], &flds[4], &rowid)
		if err != nil {
			t.Fatalf("Failed to scan time fields: %v", err)
		}
		// dt := time.Date(2023, time.October, 15, 10, 30, 45, 123456, time.UTC)
		for i, v := range flds {
			switch i {
			case FLD_TT_DATE:
				assert.Equal(t, time.Date(2023, time.October, 15, 0, 0, 0, 0, time.UTC), v.(time.Time), "Field %d should match", i + 1)
			case FLD_TIME:

				assert.Equal(t, fmt.Sprintf("%02d:%02d:%02d", 10, 30, 45), v.(TTTime).String(), "Field %d should match", i + 1)
			case FLD_DATE:
				assert.Equal(t, time.Date(2023, time.October, 15, 10, 30, 45, 0, time.UTC), v.(time.Time), "Field %d should match", i + 1)
			case FLD_TIMESTAMP: // nanosecond rounded to milliseconds 
				assert.Equal(t, time.Date(2023, time.October, 15, 10, 30, 45, int(math.Round(float64(nanosecond)/1000.0)) * 1000, time.UTC), v.(time.Time), "Field %d should match", i + 1)
			case FLD_TT_TIMESTAMP: // nanosecond truncated to milliseconds 
				assert.Equal(t, time.Date(2023, time.October, 15, 10, 30, 45, nanosecond/1000 * 1000, time.UTC), v.(time.Time), "Field %d should match", i + 1)
			}
		}
	}
	assert.Equal(t, 1, cnt, "One row should be selected")
	if err = rows.Err(); err != nil {
		t.Fatalf("Row iteration error: %v", err)
	}
	res, err = tx.Exec(`update flds_test
		set FLD_DATE=null, FLD_TIME=null, FLD_TIMESTAMP=null, FLD_TT_DATE=null, FLD_TT_TIMESTAMP=null
			where FLD_DATE=:date and FLD_TIME=? and FLD_TIMESTAMP=? and FLD_TT_DATE=:tt_date and FLD_TT_TIMESTAMP=:tt_timestamp`,
			sql.NamedArg{Name: "date", Value: dt},
			sql.NamedArg{Name: "tt_date", Value: dt},
			sql.NamedArg{Name: "tt_timestamp", Value: dt},
			dt, dt)
	if err != nil {
		t.Fatalf("Failed to update time fields: %v", err)
	}
	rowsAffected, _ = res.RowsAffected()
	assert.Equal(t, int64(1), rowsAffected, "One row should be updated")

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	rows, err = db.Query(`SELECT FLD_DATE, FLD_TIME, FLD_TIMESTAMP, FLD_TT_DATE, FLD_TT_TIMESTAMP FROM flds_test
		where FLD_DATE is null and FLD_TIME is null and FLD_TIMESTAMP is null and FLD_TT_DATE is null and FLD_TT_TIMESTAMP is null and rowid=?`, rowid)
	if err != nil {
		t.Fatalf("Failed to select time fields: %v", err)
	}
	defer rows.Close()
	cnt = 0
	for rows.Next() {
		cnt++
		err = rows.Scan(&flds[0], &flds[1], &flds[2], &flds[3], &flds[4])
		if err != nil {
			t.Fatalf("Failed to scan time fields: %v", err)
		}
		for i, v := range flds {
			assert.Nil(t, v, "Field %d should be null", i + 1)
		}
	}
	assert.Equal(t, 1, cnt, "One row should be selected")
	if err = rows.Err(); err != nil {
		t.Fatalf("Row iteration error: %v", err)
	}
	tx, err = db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	res, err = tx.Exec("DELETE FROM flds_test WHERE rowid=?", rowid)
	if err != nil {
		t.Fatalf("Failed to delete test row: %v", err)
	}
	rowsAffected, _ = res.RowsAffected()
	assert.Equal(t, int64(1), rowsAffected, "One row should be deleted")
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit delete transaction: %v", err)
	}
}

func TestFloatFlds(t *testing.T) {
	vals := []float64{1.1, -2.2, 3.3, -4.4, 5.5, -6.6}
	cmd := `insert into flds_test (
		FLD_NUMBER, FLD_BINARY_DOUBLE, FLD_BINARY_FLOAT, FLD_DOUBLEPRECISION, 
		FLD_FLOAT, FLD_REAL) 
		values (:num, ?, ?, :dblp, :float, ?)`
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	res, err := tx.Exec(cmd, 
		sql.NamedArg{Name: "num", Value: vals[0]},
		sql.NamedArg{Name: "dblp", Value: vals[3]},
		sql.NamedArg{Name: "float", Value: vals[4]},
		vals[1], vals[2], vals[5]) 
	
	if err != nil {
		t.Fatalf("Failed to insert float fields: %v", err)
	}
	rowsAffected, _ := res.RowsAffected()
	assert.Equal(t, int64(1), rowsAffected, "One row should be inserted")
	rows, err := tx.Query(`SELECT FLD_NUMBER, FLD_BINARY_DOUBLE, FLD_BINARY_FLOAT, FLD_DOUBLEPRECISION, 
	FLD_FLOAT, FLD_REAL, rowid FROM flds_test
		where FLD_NUMBER=:num and FLD_BINARY_DOUBLE=? and FLD_BINARY_FLOAT=? and FLD_DOUBLEPRECISION=:dblp and 
		FLD_FLOAT=:float and FLD_REAL=?`,
		sql.NamedArg{Name: "num", Value: vals[0]},
		sql.NamedArg{Name: "dblp", Value: vals[3]},
		sql.NamedArg{Name: "float", Value: vals[4]},
		vals[1], vals[2], vals[5])
	if err != nil {
		t.Fatalf("Failed to select float fields: %v", err)
	}
	defer rows.Close()
	cnt := 0
	flds := make([]any, len(vals))
	for rows.Next() {
		cnt++
		err = rows.Scan(&flds[0], &flds[1], &flds[2], &flds[3], &flds[4], &flds[5], &rowid)
		if err != nil {
			t.Fatalf("Failed to scan float fields: %v", err)
		}
		for i, v := range vals {
			assert.LessOrEqualf(t, math.Abs(v - flds[i].(float64)), 0.000001, "Field %d should match", i + 1)
			// assert.Equal(t, v, flds[i].(float64), "Field %d should match", i + 1)
		}
	}
	assert.Equal(t, 1, cnt, "One row should be selected")
	if err = rows.Err(); err != nil {
		t.Fatalf("Row iteration error: %v", err)
	}
	res, err = tx.Exec(`update flds_test
		set FLD_NUMBER=null, FLD_BINARY_DOUBLE=null, FLD_BINARY_FLOAT=null, FLD_DOUBLEPRECISION=null,
		FLD_FLOAT=null, FLD_REAL=null
		where FLD_NUMBER=:num and FLD_BINARY_DOUBLE=? and FLD_BINARY_FLOAT=? and FLD_DOUBLEPRECISION=:dblp and 
		FLD_FLOAT=:float and FLD_REAL=?`,
		sql.NamedArg{Name: "num", Value: vals[0]},
		sql.NamedArg{Name: "dblp", Value: vals[3]},
		sql.NamedArg{Name: "float", Value: vals[4]},
		vals[1], vals[2], vals[5])
	if err != nil {
		t.Fatalf("Failed to update float fields: %v", err)
	}
	rowsAffected, _ = res.RowsAffected()
	assert.Equal(t, int64(1), rowsAffected, "One row should be updated")

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	rows, err = db.Query(`SELECT FLD_NUMBER, FLD_BINARY_DOUBLE, FLD_BINARY_FLOAT, FLD_DOUBLEPRECISION, 
	FLD_FLOAT, FLD_REAL FROM flds_test
		where FLD_NUMBER is null and FLD_BINARY_DOUBLE is null and FLD_BINARY_FLOAT is null and FLD_DOUBLEPRECISION is null
		and FLD_FLOAT is null and FLD_REAL is null and rowid=?`, rowid)
	if err != nil {
		t.Fatalf("Failed to select float fields: %v", err)
	}
	defer rows.Close()
	cnt = 0
	for rows.Next() {
		cnt++
		err = rows.Scan(&flds[0], &flds[1], &flds[2], &flds[3], &flds[4], &flds[5])
		if err != nil {
			t.Fatalf("Failed to scan integer fields: %v", err)
		}
		for i, v := range flds {
			assert.Nil(t, v, "Field %d should be null", i + 1)
		}
	}
	assert.Equal(t, 1, cnt, "One row should be selected")
	if err = rows.Err(); err != nil {
		t.Fatalf("Row iteration error: %v", err)
	}
	tx, err = db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	res, err = tx.Exec("DELETE FROM flds_test WHERE rowid=?", rowid)
	if err != nil {
		t.Fatalf("Failed to delete test row: %v", err)
	}
	rowsAffected, _ = res.RowsAffected()
	assert.Equal(t, int64(1), rowsAffected, "One row should be deleted")
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit delete transaction: %v", err)
	}
}

func TestBinaryFlds(t *testing.T) {
	vals :=[][]byte {{1, 2, 3, 4, 5}, {1, 2, 3} }
	cmd := `insert into flds_test (FLD_BLOB, FLD_BINARY) 
		values (:blob, ?)`
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	res, err := tx.Exec(cmd, 
		sql.NamedArg{Name: "blob", Value: vals[0]}, vals[1]) 
	
	if err != nil {
		t.Fatalf("Failed to insert binary fields: %v", err)
	}
	rowsAffected, _ := res.RowsAffected()
	assert.Equal(t, int64(1), rowsAffected, "One row should be inserted")
	tx.Commit()
	tx, err = db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	rows, err := tx.Query(`SELECT FLD_BLOB, FLD_BINARY, rowid FROM flds_test
		where FLD_BLOB is not null`)
	if err != nil {
		t.Fatalf("Failed to select binary fields: %v", err)
	}
	defer rows.Close()
	cnt := 0
	flds := make([]any, len(vals))
	// rowid := ""
	for rows.Next() {
		cnt++
		err = rows.Scan(&flds[0], &flds[1], &rowid)
		if err != nil {
			t.Fatalf("Failed to scan binary fields: %v", err)
		}
		for i, v := range vals {
			assert.Equal(t, v, flds[i].([]byte)[:len(v)], "Field %d should match", i + 1)
		}
	}
	assert.Equal(t, 1, cnt, "One row should be selected")
	if err = rows.Err(); err != nil {
		t.Fatalf("Row iteration error: %v", err)
	}
	res, err = tx.Exec(`update flds_test
		set FLD_BLOB=null, FLD_BINARY=null
		where rowid=?`,rowid)
	if err != nil {
		t.Fatalf("Failed to update binary fields: %v", err)
	}
	rowsAffected, _ = res.RowsAffected()
	assert.Equal(t, int64(1), rowsAffected, "One row should be updated")

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	rows, err = db.Query(`SELECT FLD_blob, FLD_BINARY FROM flds_test
		where rowid=?`, rowid)
	if err != nil {
		t.Fatalf("Failed to select binary fields: %v", err)
	}
	defer rows.Close()
	cnt = 0
	for rows.Next() {
		cnt++
		err = rows.Scan(&flds[0], &flds[1])
		if err != nil {
			t.Fatalf("Failed to scan binary fields: %v", err)
		}
		for i, v := range flds {
			assert.Nil(t, v, "Field %d should be null", i + 1)
		}
	}
	assert.Equal(t, 1, cnt, "One row should be selected")
	if err = rows.Err(); err != nil {
		t.Fatalf("Row iteration error: %v", err)
	}
}
