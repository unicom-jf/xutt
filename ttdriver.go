package xutt

/*
#include <stdlib.h>
#include "ttdriver.h"
#include "types.h"
*/
import "C"

import (
	"context"
	_ "context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"
)

type TTTime struct {
	Hour uint8
	Minute uint8
	Second uint8
}
func (t TTTime) String() string {
	return fmt.Sprintf("%02d:%02d:%02d", t.Hour, t.Minute, t.Second)
}
func (t TTTime) Value() (driver.Value, error) {
	dt := time.Now()
	return time.Date(dt.Year(), dt.Month(), dt.Day(), int(t.Hour), int(t.Minute), int(t.Second), 0, time.UTC), nil
}

type NullTTTime struct {
	TTTime TTTime
	Valid  bool // Valid is true if TTTime is not NULL
}
func (t NullTTTime) Name() string {
	return "NullTTTime"
}

func (t NullTTTime) Value() (driver.Value, error) {
	if !t.Valid {
		return nil, nil
	} else {
		return t.TTTime, nil
	}
}
func (t *NullTTTime) Scan(value any) error {
	if value == nil {
		t.TTTime, t.Valid = TTTime{}, false
		return nil
	}
	switch v := value.(type) {
	case TTTime:
		t.TTTime = v
		t.Valid = true
		return nil
	default:
		return fmt.Errorf("cannot scan type %T into NullTTTime", value)
	}
}
type ConnParam struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
	Options
}
type Options struct {
	RequireEncryption bool `default:"false"`
	Wallet            string
	CipherSuites      string
	ConnTimeout       int    `default:"0"` //seconds
	QueryTimeout      int    `default:"0"` //seconds
	Charset           string `default:"utf8"`
}
// get int or string env value, otherwise default
func env2Any(name string, def any) any {
	if v, ok := os.LookupEnv(name); !ok {
		return def
	} else {
		switch def.(type) {
		case int:
			if iv, err := strconv.Atoi(v); err == nil {
				return iv
			} else {
				return def
			}
		case string:
			return v
		default:
			return nil
		}
	}
	// return def
}
func TTClientVersion() int {
	return int(C.TTClientVersion())
}
func ConnParamByEnv() (*ConnParam, error) {
	param := ConnParam {
		os.Getenv("TTC_SERVER"),
		env2Any("TTC_TCP_PORT", 6625).(int),
		os.Getenv("TTC_UID"),
		os.Getenv("TTC_PWD"),
		os.Getenv("TTC_SERVER_DSN"),
		Options{
			RequireEncryption: os.Getenv("TTC_RequireEncryption") == "true",
			Wallet:            os.Getenv("TTC_Wallet"),
			CipherSuites:      os.Getenv("TTC_CipherSuites"),
			ConnTimeout:       env2Any("TTC_ConnTimeout", 0).(int),
			QueryTimeout:      env2Any("TTC_QueryTimeout", 0).(int),
			Charset:           env2Any("TTC_Charset", "utf8").(string),
		},
	}
	if param.Host == "" || param.User == "" || param.Password == "" || param.DBName == "" {
		return nil, errors.New("missing required environment variables: TTC_SERVER, TTC_UID, TTC_PWD, TTC_SERVER_DSN")
	}
	if param.RequireEncryption && 
		(param.Wallet == "" || param.CipherSuites == "") {
		return nil, errors.New("missing required environment variables for encryption: TTC_Wallet, TTC_CipherSuites")
	}
	return &param, nil
}
// type TTParam struct {
// 	SQLQueryTimeout int `default:"0"` //seconds
// 	PLSQLTimeout int `default:"0"` //seconds
// }

func (param ConnParam) Dsn() string {
	dsn := fmt.Sprintf("TTC_SERVER=%s;TCP_PORT=%d;UID=%s;PWD=%s;TTC_SERVER_DSN=%s", param.Host, param.Port, param.User, param.Password, param.DBName)
	if param.Options.RequireEncryption {
		dsn += ";Encryption=Required"
		if param.Options.Wallet != "" {
			dsn += fmt.Sprintf(";Wallet=%s", param.Options.Wallet)
		}
		if param.Options.CipherSuites != "" {
			dsn += fmt.Sprintf(";CipherSuites=%s", param.Options.CipherSuites)
		}
	}

	// if param.Options.ConnTimeout > 0 {
	// 	dsn += fmt.Sprintf(";connection_timeout=%d", param.Options.ConnTimeout)
	// }
	if param.Options.QueryTimeout > 0 {
		dsn += fmt.Sprintf(";QueryTimeout=%d", param.Options.QueryTimeout)
	}
	if param.Options.Charset != "" {
		dsn += fmt.Sprintf(";ConnectionCharacterSet=%s", param.Options.Charset)
	}
	return dsn
}

// type TTColumnConverterdeprecated struct{}
type TTConn struct {
	db C.TTConnPtr
	ConnParam
	mu sync.Mutex
}

type WCharFldStruct struct {
	PFld *uint16
	Len  uint32
}
// Ping with timeout context
func (c *TTConn) Ping(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	ping := make(chan struct{})
	var err error
	go func () {
		err = nil
		var status C.DrvStatus
		C.TTPing((unsafe.Pointer)(c.db), &status)
		if status.no != 0 {
			err = errors.New(C.GoString(&status.msg[0]))
		}
		ping <- struct{}{}
	} ()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ping:
		return err
	}
	
}
// Commit implements driver.Tx.
func (c *TTConn) Commit() error {
	// panic("unimplemented")
	var status C.DrvStatus
	C.TTCommit((unsafe.Pointer)(c.db), &status)
	if status.no != 0 {
		return errors.New(C.GoString(&status.msg[0]))
	}
	return nil
}

// Rollback implements driver.Tx.
func (c *TTConn) Rollback() error {
	// panic("unimplemented")
	var status C.DrvStatus
	C.TTRollback((unsafe.Pointer)(c.db), &status)
	if status.no != 0 {
		return errors.New(C.GoString(&status.msg[0]))
	}
	return nil
}

// ExecContext implement ExecerContext.
func (c *TTConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.exec(ctx, query, args)
}
func (c *TTConn) exec(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	s, err := c.prepare(ctx, query)
	if err != nil {
		return nil, err
	}
	// var res driver.Result
	if s.(*TTStmt).stmt == nil {
		return nil, fmt.Errorf("statement is not prepared")
	} 
	defer s.Close()
	stmtArgs, err := buildStmtArgs(s, args)
	if err != nil { 
		return  nil, err
	}
	return s.(*TTStmt).exec(ctx, stmtArgs)
}

// buildStmtArgs build statement args from query args.
// Support both positional and named parameters. 
func buildStmtArgs(s driver.Stmt, args []driver.NamedValue) ([]driver.NamedValue, error) {
	stmt := s.(*TTStmt)
	stmtArgs := make([]driver.NamedValue, 0, len(stmt.params))
	
	tmpNp := make(map[string]struct{})
	tmpAp := []int{}
	for i, p := range stmt.params {
		stmtArgs = append(stmtArgs, driver.NamedValue{Ordinal: i + 1, Name: p.Name})
		if p.Name != "" {
			tmpNp[p.Name] = struct{}{}
		} else {
			tmpAp = append(tmpAp, i + 1)
		}
	}
	total := len(tmpNp) + len(tmpAp)
	foundAp := 0
	for i := range args {
		if args[i].Name != "" {
			for j := range stmtArgs {
				if args[i].Name == stmtArgs[j].Name {
					stmtArgs[j].Value = args[i].Value
					delete(tmpNp, args[i].Name)
				}
			}
		} else {
			if foundAp < len(tmpAp) {
				stmtArgs[tmpAp[foundAp]-1].Value = args[i].Value
				foundAp++
			}
		}
	}
	if len(tmpNp) > 0 || foundAp < len(tmpAp) {
		return nil, fmt.Errorf("not enough args to execute query: want %d got %d", total, (total - len(tmpNp) - (len(tmpAp) - foundAp)))
	}
	return stmtArgs, nil
}

// QueryContext implement QueryerContext.
func (c *TTConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.query(ctx, query, args)
}

func (c *TTConn) query(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	stmtArgs := make([]driver.NamedValue, 0, len(args))
	s, err := c.prepare(ctx, query)
	if err != nil {
		return nil, err
	}
	stmtArgs, err = buildStmtArgs(s, args)
	if err != nil {
		s.Close()
		return nil, err
	}
	rows, err := s.(*TTStmt).query(ctx, stmtArgs)
	if err != nil {
		s.Close()
		return nil, err
	}
	return rows, err
}

// Begin implements driver.Conn.
func (c *TTConn) Begin() (driver.Tx, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// Close implements driver.Conn.
func (c *TTConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var status C.DrvStatus
	C.TTDisconn((unsafe.Pointer)(c.db), &status)
	c.db = nil
	return nil
}

// Prepare implements driver.Conn.
func (c *TTConn) Prepare(query string) (driver.Stmt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.prepare(context.Background(), query)
}

func (c *TTConn) prepare(ctx context.Context, query string) (driver.Stmt, error) {
	pquery := C.CString(query)
	defer C.free(unsafe.Pointer(pquery))
	var status C.DrvStatus
	var s C.TTCmdPtr
	if s = (C.TTCmdPtr)(C.TTCmdNew(&status)); status.no != 0 {
		return nil, fmt.Errorf("ttdrv: %s", C.GoString(&status.msg[0]))
	}
	// fmt.Printf("new cmd: %v\n", s)
	var p C.TTParam
	p.sqlQueryTimeout = C.uint(c.QueryTimeout)
	if C.TTCmdPrepare((unsafe.Pointer)(c.db), (unsafe.Pointer)(s), p, pquery, &status); status.no != 0 {
		return nil, fmt.Errorf("ttdrv: %s", C.GoString(&status.msg[0]))
	}
	// fmt.Printf("prepare cmd with db: %v, cmd: %v\n", c.db, s)
	params, err := c.parseParams(query)
	if err != nil {
		return nil, err
	}
	ss := &TTStmt{db: c.db, stmt: s, params: params}
	// runtime.SetFinalizer(ss, (*TTStmt).Close)
	return ss, nil
}
// : => named parameter, ? => positional parameter
func (c *TTConn) parseParams(query string) ([]driver.NamedValue, error) {
	params := make([]driver.NamedValue, 0)
	p := ""
	idx := 1
	for pos, runeValue := range query {
		if runeValue == ':' || runeValue == '?' {
			err := c.parseNameedParam(&params, &idx, p, pos, true)
			if err != nil {
				return nil, err
			}
			p = string(runeValue)
		} else if runeValue == ' ' || runeValue == ',' || runeValue == ')' {
			err := c.parseNameedParam(&params, &idx, p, pos, false)
			if err != nil {
				return nil, err
			}
			p = ""
		} else if p != "" {
			p += string(runeValue)
		}
	}
	err := c.parseNameedParam(&params, &idx, p, len(query), false)
	if err != nil {
		return nil, err
	}
	return params, nil
}

func (c *TTConn) parseNameedParam(params *[]driver.NamedValue, idx *int, p string, pos int, chkDouble bool) error {
	if p == "" {
		return nil
	}
	if chkDouble {
		if p == ":" || p == "?" {
			return fmt.Errorf("invalid parameter at position %d", pos)
		}
	}
	if p[0] == ':' {
		if len(p) == 1 {
			return fmt.Errorf("invalid parameter at position %d", pos)
		} else {
			*params = append(*params, driver.NamedValue{Name: p[1:], Ordinal: *idx})
		}
	} else {
		*params = append(*params, driver.NamedValue{Name: "", Ordinal: *idx})
	}
	*idx++
	return nil
}

var _ driver.Conn = (*TTConn)(nil)

// type TTConnBeginTx struct{}

// BeginTx implements driver.ConnBeginTx.
// func (t *TTConnBeginTx) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {

// Timesten does not need to begin transaction explicitly 
func (c *TTConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c, nil
}

// var _ driver.ConnBeginTx = (*TTConnBeginTx)(nil)

// type TTConnPrepareContext struct{}

// // PrepareContext implements driver.ConnPrepareContext.
// func (t *TTConnPrepareContext) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
// 	panic("unimplemented")
// }

// var _ driver.ConnPrepareContext = (*TTConnPrepareContext)(nil)

// type TTConnector struct{}

// // Connect implements driver.Connector.
// func (t *TTConnector) Connect(context.Context) (driver.Conn, error) {
// 	panic("unimplemented")
// }

// // Driver implements driver.Connector.
// func (t *TTConnector) Driver() driver.Driver {
// 	panic("unimplemented")
// }

// var _ driver.Connector = (*TTConnector)(nil)

type TTDriver struct{}

// Open implements driver.Driver.
func (t *TTDriver) Parse(name string) (ConnParam, error) {
	param := ConnParam{}
	cfgs := strings.Split(strings.ToLower(name), ";")
	for _, cfg := range cfgs {
		kv := strings.SplitN(cfg, "=", 2)
		switch strings.TrimSpace(kv[0]) {
		case "ttc_server":
			param.Host = strings.TrimSpace(kv[1])
		case "tcp_port":
			fmt.Sscanf(strings.TrimSpace(kv[1]), "%d", &param.Port)
		case "uid":
			param.User = strings.TrimSpace(kv[1])
		case "pwd":
			param.Password = strings.TrimSpace(kv[1])
		case "ttc_server_dsn":
			param.DBName = strings.TrimSpace(kv[1])
		case "encryption":
			if strings.TrimSpace(kv[1]) == "required" {
				param.Options.RequireEncryption = true
			} else {
				param.Options.RequireEncryption = false
			}
		case "wallet":
			param.Options.Wallet = strings.TrimSpace(kv[1])
		case "ciphersuites":
			param.Options.CipherSuites = strings.TrimSpace(kv[1])
		case "connectioncharacterset":
			param.Options.Charset = strings.TrimSpace(kv[1])
		case "conntimeout":
			fmt.Sscanf(strings.TrimSpace(kv[1]), "%d", &param.Options.ConnTimeout)
		case "querytimeout":
			fmt.Sscanf(strings.TrimSpace(kv[1]), "%d", &param.Options.QueryTimeout)
		}
	}
	return param, nil
}

func (t *TTDriver) Open(name string) (driver.Conn, error) {
	param, err := t.Parse(name)
	if err != nil {
		return nil, err
	}
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	var ttParam C.TTParam
	var status C.DrvStatus
	db := C.TTConnNew(cname, &ttParam, &status)
	if status.no != 0 {
		return nil, fmt.Errorf("TTConnNew error: %s", C.GoString(&status.msg[0]))
	}
	// fmt.Printf("new conn: %v\n", db)
	// timeout forced by tt
	if param.Options.QueryTimeout >= int(ttParam.sqlQueryTimeout) {
		param.QueryTimeout = 0
	}
	return &TTConn{db: (C.TTConnPtr)(db), ConnParam: param}, nil
}

var _ driver.Driver = (*TTDriver)(nil)

// type TTDriverContext struct{}

// // OpenConnector implements driver.DriverContext.
// func (t *TTDriverContext) OpenConnector(name string) (driver.Connector, error) {
// 	panic("unimplemented")
// }

// var _ driver.DriverContext = (*TTDriverContext)(nil)

// type TTExecer struct{}

// // Exec implements driver.Execer.
// func (t *TTExecer) Exec(query string, args []driver.Value) (driver.Result, error) {
// 	panic("unimplemented")
// }

// // deprecated
// var _ driver.Execer = (*TTExecer)(nil)

// type TTExecerContext struct{}

// // ExecContext implements driver.ExecerContext.
// func (t *TTExecerContext) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
// 	panic("unimplemented")
// }

// var _ driver.ExecerContext = (*TTExecerContext)(nil)

// type TTIsolationLevel struct{}

// var _ driver.IsolationLevel = (*TTIsolationLevel)(nil)

// type TTNamedValue struct{}

// var _ driver.NamedValue = (*TTNamedValue)(nil)

// type TTNamedValueChecker struct{}

// // CheckNamedValue implements driver.NamedValueChecker.
// func (t *TTNamedValueChecker) CheckNamedValue(*driver.NamedValue) error {
// 	panic("unimplemented")
// }

// var _ driver.NamedValueChecker = (*TTNamedValueChecker)(nil)

// type TTNotNull struct{}

// var _ driver.NotNull = (*TTNotNull)(nil)

// type TTNull struct{}

// var _ driver.Null = (*TTNull)(nil)

// type TTPinger struct{}

// // Ping implements driver.Pinger.
// func (t *TTPinger) Ping(ctx context.Context) error {
// 	panic("unimplemented")
// }

// var _ driver.Pinger = (*TTPinger)(nil)

// type TTQueryer struct{}

// // Query implements driver.Queryer.
// func (t *TTQueryer) Query(query string, args []driver.Value) (driver.Rows, error) {
// 	panic("unimplemented")
// }

// // deprecated
// var _ driver.Queryer = (*TTQueryer)(nil)

type TTResult struct {
	id           int64
	rowsAffected int64
}

// LastInsertId implements driver.Result.
func (t *TTResult) LastInsertId() (int64, error) {
	return t.id, nil
}

// RowsAffected implements driver.Result.
func (t *TTResult) RowsAffected() (int64, error) {
	return t.rowsAffected, nil
}

var _ driver.Result = (*TTResult)(nil)

type TTRows struct {
	stmt     C.TTCmdPtr
	nc       int32    //number of columns
	cols     []string //column names
	decltype []string //column types
	colstype []int
	scantype []reflect.Type
	ctx      context.Context
}

// Close implements driver.Rows.
func (t *TTRows) Close() error {
	var status C.DrvStatus
	C.TTCmdClose((unsafe.Pointer)(t.stmt), &status)
	if status.no != 0 {
		return fmt.Errorf("TTRows' close error: %s", C.GoString(&status.msg[0]))
	}
	return nil
}

func (t *TTRows) setColumnsAttrs() error {
	var status C.DrvStatus
	t.nc = int32(C.TTCmdColumnCount((unsafe.Pointer)(t.stmt), &status))
	if t.nc == 0 {
		return nil
	}
	// column names
	if t.stmt != nil && t.cols == nil {
		t.cols = make([]string, t.nc)
		var buf [C.MaxColumnNameLen]C.char
		for i := 0; i < int(t.nc); i++ {
			C.TTGetColumnName((unsafe.Pointer)(t.stmt), C.int(i+1), &buf[0], &status)
			t.cols[i] = C.GoString(&buf[0])
		}
	}
	// column types
	if t.stmt != nil && t.decltype == nil {
		t.decltype = make([]string, t.nc)
		t.colstype = make([]int, t.nc)
		t.scantype = make([]reflect.Type, t.nc)
		var buf [C.MaxColumnNameLen]C.char
		for i := 0; i < int(t.nc); i++ {
			C.TTGetColumnTypeName((unsafe.Pointer)(t.stmt), C.int(i+1), &buf[0], &status)
			t.decltype[i] = C.GoString(&buf[0])
			// fmt.Println("rowid: ", t.decltype[i])
			t.setColumnType(i)
		}
	}
	return nil
}

// map typeName to C type
func (t *TTRows) setColumnType(i int) {
	switch t.decltype[i] {
	case C.SQL_GO_INT:
		t.colstype[i] = C.SQL_INTEGER
		// t.scantype[i] = reflect.TypeOf(int64(0))
		t.scantype[i] = reflect.TypeOf(sql.NullInt64{})
	case C.SQL_GO_DOUBLE:
		t.colstype[i] = C.SQL_DOUBLE
		t.scantype[i] = reflect.TypeOf(sql.NullFloat64{})
	case C.SQL_GO_CHAR:
		t.colstype[i] = C.SQL_CHAR
		t.scantype[i] = reflect.TypeOf(sql.NullString{})
	case C.SQL_GO_WCHAR:
		t.colstype[i] = C.TT_NCHAR
		t.scantype[i] = reflect.TypeOf(sql.NullString{})
	case C.SQL_GO_DATE:
		t.colstype[i] = C.TT_DATE
		t.scantype[i] = reflect.TypeOf(sql.NullTime{})
	case C.SQL_GO_TIME:
		t.colstype[i] = C.SQL_TIME
		t.scantype[i] = reflect.TypeOf(NullTTTime{})
		// t.scantype[i] = reflect.TypeOf(sql.NullString{})
		// t.scantype[i] = reflect.TypeOf(sql.NullTime{})
	case C.SQL_GO_DATETIME:
		t.colstype[i] = C.TT_DATETIME
		t.scantype[i] = reflect.TypeOf(sql.NullTime{})
	case C.SQL_GO_BYTES:
		t.colstype[i] = C.TT_BLOB
		t.scantype[i] = reflect.TypeOf(sql.RawBytes{})
	}
}

// Columns implements driver.Rows.
func (t *TTRows) Columns() []string {
	return t.cols
}
func (t *TTRows) ColumnTypeScanType(i int) reflect.Type {
	return t.scantype[i]
}

// DeclTypes return column types.
func (t *TTRows) DeclTypes() []string {
	return t.decltype
}

// Next implements driver.Rows.
func (t *TTRows) Next(dest []driver.Value) error {
	var status C.DrvStatus
	var flag int
	if flag = int(C.TTGetNextRow((unsafe.Pointer)(t.stmt), &status)); status.no != 0 {
		return fmt.Errorf("TTGetNextRow error: %s", C.GoString(&status.msg[0]))
	}
	if flag == 1 {
		return io.EOF
	}
	var isNull uint8
	for i := range dest {
		switch t.colstype[i] {
		case C.SQL_INTEGER:
			var val int64
			C.TTGetColumnValue((unsafe.Pointer)(t.stmt), C.int(i+1), C.SQL_INTEGER, (unsafe.Pointer)(&val), (*C.uchar)(&isNull), &status)
			if isNull == 1 {
				dest[i] = nil
			} else {
				dest[i] = val
			}
		case C.SQL_DOUBLE:
			var val float64
			C.TTGetColumnValue((unsafe.Pointer)(t.stmt), C.int(i+1), C.SQL_DOUBLE, (unsafe.Pointer)(&val), (*C.uchar)(&isNull), &status)
			if isNull == 1 {
				dest[i] = nil
			} else {
				dest[i] = val
			}
		case C.TT_DATE:
			var val C.DATE_STRUCT
			C.TTGetColumnValue((unsafe.Pointer)(t.stmt), C.int(i+1), C.TT_DATE, (unsafe.Pointer)(&val), (*C.uchar)(&isNull), &status)
			if isNull == 1 {
				dest[i] = nil
			} else {
				dest[i] = time.Date(int(val.year), time.Month(val.month), int(val.day), 0, 0, 0, 0, time.UTC)
			}

		case C.SQL_TIME:
			var val C.TIME_STRUCT
			C.TTGetColumnValue((unsafe.Pointer)(t.stmt), C.int(i+1), C.SQL_TIME, (unsafe.Pointer)(&val), (*C.uchar)(&isNull), &status)
			// dest[i] = fmt.Sprintf("%02d:%02d:%02d", int(val.hour), int(val.minute), int(val.second))
			if isNull == 1 {	
				dest[i] = nil
			} else {
				// dest[i] = fmt.Sprintf("%02d:%02d:%02d", int(val.hour), int(val.minute), int(val.second))	
				// dest[i] = val.hour<<16 | val.minute<<8 | val.second
				dest[i] = TTTime{
					Hour:   uint8(val.hour),
					Minute: uint8(val.minute),
					Second: uint8(val.second),
				}
			}
		
		case C.TT_DATETIME:
			var val C.TIMESTAMP_STRUCT
			C.TTGetColumnValue((unsafe.Pointer)(t.stmt), C.int(i+1), C.TT_DATETIME, (unsafe.Pointer)(&val), (*C.uchar)(&isNull), &status)
			// dest[i] = time.Date(int(val.year), time.Month(val.month), int(val.day), int(val.hour), int(val.minute), int(val.second), int(val.fraction), time.Local)
			if isNull == 1 {
				dest[i] = nil
			} else {
				dest[i] = time.Date(int(val.year), time.Month(val.month), int(val.day), int(val.hour), int(val.minute), int(val.second), int(val.fraction), time.UTC)
			}
		case C.SQL_CHAR:
			size := C.TTGetColumnLength((unsafe.Pointer)(t.stmt), C.int(i+1), &status)
			ptr := C.calloc(size+1, C.sizeof_char)
			// defer C.free(unsafe.Pointer(ptr))
			C.TTGetColumnValue((unsafe.Pointer)(t.stmt), C.int(i+1), C.SQL_CHAR, (unsafe.Pointer)(ptr), (*C.uchar)(&isNull), &status)
			if isNull == 1 {
				dest[i] = nil
			} else {
				dest[i] = string(C.GoBytes(ptr, (C.int)(size)))
			}
			C.free(unsafe.Pointer(ptr))
		case C.TT_NCHAR:
			wcharFld := C.TTGetWCharValue((unsafe.Pointer)(t.stmt), C.int(i+1), C.TT_NCHAR, (*C.uchar)(&isNull), &status)
			// defer C.freeWChar(wcharFld)
			if isNull == 1 {
				dest[i] = nil
			} else {
				var runes = []rune{}
				for i := 0; i < int(wcharFld.len); i++ {
					runes = append(runes, rune(C.getWChar(wcharFld, C.long(i))))
				}
				dest[i] = string(runes)	
				C.freeWChar(wcharFld)
			}
			// fmt.Printf("wchar: %v, isnull: %v\n", wcharFld, isNull)
			
		case C.TT_BLOB:
			blobFld := C.TTGetBlobValue((unsafe.Pointer)(t.stmt), C.int(i+1), C.TT_BLOB, (*C.uchar)(&isNull), &status)
			// defer C.freeBlob(blobFld)
			if isNull == 1 {
				dest[i] = nil
			} else {
				dest[i] = C.GoBytes((unsafe.Pointer)(blobFld.pFld), (C.int)(blobFld.len))
				C.freeBlob(blobFld)
			}
		default:
			dest[i] = nil
		}
	}
	return nil
}

var _ driver.Rows = (*TTRows)(nil)

// type TTRowsAffected struct{}

// var _ driver.RowsAffected = (*TTRowsAffected)(nil)


type TTStmt struct {
	db     C.TTConnPtr
	stmt   C.TTCmdPtr
	params []driver.NamedValue
}

// func (s *TTStmt) CheckNamedValue(nv *driver.NamedValue) (err error) {
// 	switch d := nv.Value.(type) {
//         case []int64:
//                 err = nil
// 	default:
// 		nv.Value, err = driver.DefaultParameterConverter.ConvertValue(nv.Value)
// 	}
// 	return err
// }

// Close implements driver.Stmt.
func (t *TTStmt) Close() error {
	var status C.DrvStatus
	C.TTCmdClose((unsafe.Pointer)(t.stmt), &status)
	if status.no != 0 {
		return errors.New(C.GoString(&status.msg[0]))
	} else {
		return nil
	}
}

// Exec implements driver.Stmt.
func (t *TTStmt) Exec(args []driver.Value) (driver.Result, error) {
	list := make([]driver.NamedValue, len(args))
	for i, v := range args {
		list[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   v,
		}
	}
	return t.exec(context.Background(), list)
}

// uint8
// time.Time
// value:  19 4 1
// int
// string
// float64
// []uint16
// []uint8
func (s *TTStmt) bindParams(args []driver.NamedValue) error {
	if len(args) == 0 {
		return nil
	}
	var status C.DrvStatus
	for _, param := range args {
		switch val := param.Value.(type) {
		case time.Time:
			s.bindTimestamp(param.Ordinal, val, status)
		case []byte:
			_val := reflect.ValueOf(param.Value).Interface().([]byte)
			C.TTSetParamBinary((unsafe.Pointer)(s.stmt), (C.int)(param.Ordinal), (unsafe.Pointer)(&(_val[0])), (C.int)(len(_val)), &status)
		case string:
			cstr := C.CString(val)
			defer C.free(unsafe.Pointer(cstr))
			C.TTSetParamChar((unsafe.Pointer)(s.stmt), (C.int)(param.Ordinal), cstr, &status)
		case int8, uint8, int16, uint16, int32, uint32, int64, uint64, int, uint:
			C.TTSetParamBigInt((unsafe.Pointer)(s.stmt), (C.int)(param.Ordinal), C.SQLBIGINT(val.(int64)), &status)
		case float32, float64:
			C.TTSetParamDouble((unsafe.Pointer)(s.stmt), (C.int)(param.Ordinal), C.double(val.(float64)), &status)
		case []int16:
			_val := reflect.ValueOf(param.Value).Interface().([]int16)
			C.TTSetParamWChar((unsafe.Pointer)(s.stmt), (C.int)(param.Ordinal), (unsafe.Pointer)(&(_val[0])), (C.int)(len(_val)*2), &status)
		case []uint16:
			_val := reflect.ValueOf(param.Value).Interface().([]uint16)
			C.TTSetParamWChar((unsafe.Pointer)(s.stmt), (C.int)(param.Ordinal), (unsafe.Pointer)(&(_val[0])), (C.int)(len(_val)*2), &status)
		}
	}
	return nil
}

func (s *TTStmt) bindTimestamp(ordinal int, v time.Time, status C.DrvStatus) {
	var ts C.TIMESTAMP_STRUCT
	ts.year = C.SQLSMALLINT(v.Year())
	ts.month = C.SQLUSMALLINT(v.Month())
	ts.day = C.SQLUSMALLINT(v.Day())
	ts.hour = C.SQLUSMALLINT(v.Hour())
	ts.minute = C.SQLUSMALLINT(v.Minute())
	ts.second = C.SQLUSMALLINT(v.Second())
	ts.fraction = C.SQLUINTEGER(v.Nanosecond())
	C.TTSetParamTimestamp((unsafe.Pointer)(s.stmt), (C.int)(ordinal), (unsafe.Pointer)(&ts), &status)
}

func (s *TTStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.exec(ctx, args)
}

func (s *TTStmt) exec(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	done := make(chan struct{})
	var _result driver.Result = nil
	var _err error = nil
	go func() {
		defer close(done)
		// long exec
		// time.Sleep(1 * time.Minute)

		var stmtArgs []driver.NamedValue
		stmtArgs, _err = buildStmtArgs(s, args)
		if _err != nil {
			return
			// return nil, err
		}
		
		if _err = s.bindParams(stmtArgs); _err != nil {
			return
			// _result := &TTResult{id, rowsAffected}
			// return res, err
		}
		var status C.DrvStatus
		var id C.long = -1
		var rowsAffected C.long = -1
		C.TTCmdExecute((unsafe.Pointer)(s.stmt), (*C.long)(&id), (*C.long)(&rowsAffected), &status)
		if status.no != 0 {
			_err = errors.New(C.GoString(&status.msg[0]))
			return
			// return nil, errors.New(C.GoString(&status.msg[0]))
		}
		_result = &TTResult{int64(id), int64(rowsAffected)}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-done:
		return _result, nil
	}
	// return _result, nil
}

// NumInput implements driver.Stmt.
// NumInput return a number of parameters.
// func (t *TTStmt) NumInput() int {
// 	// panic("unimplemented")
// 	// return int(C.TTGetParamCount((unsafe.Pointer)(t.stmt)))
// 	var status C.DrvStatus
// 	count := (int)(C.TTCmdParamsCount((unsafe.Pointer)(t.stmt), &status))
// 	if status.no != 0 {
// 		return -1
// 	}
// 	return count
// }
// todo: output parameters not supported yet
func (t *TTStmt) NumInput() int {
	count := 0
	np := make(map[string]struct{})
	for _, p := range t.params {
		if p.Name != "" {
			np[p.Name] = struct{}{}
		} else {
			count++
		}
	}
	count += len(np)
	return count
}
func (s *TTStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.query(ctx, args)
}
// Query implements driver.Stmt.
func (s *TTStmt) Query(args []driver.Value) (driver.Rows, error) {
	list := make([]driver.NamedValue, len(args))
	for i, v := range args {
		list[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   v,
		}
	}
	return s.query(context.Background(), list)
}

func (s *TTStmt) query(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	done := make(chan struct{})
	var _rows *TTRows = nil
	var _err error = nil
	go func() {
		defer close(done)
		// long query
		// time.Sleep(1 * time.Minute)

		var stmtArgs []driver.NamedValue
		stmtArgs, _err = buildStmtArgs(s, args)
		if _err != nil {
			return
			// return nil, err
		}
		s.bindParams(stmtArgs)
		var status C.DrvStatus
		if C.TTCmdQuery((unsafe.Pointer)(s.stmt), &status); status.no != 0 {
			_err = errors.New(C.GoString(&status.msg[0]))
			return
			// return nil, errors.New(C.GoString(&status.msg[0]))
		}
		// fmt.Printf("query with db: %v, cmd: %v\n", s.db, s.stmt)
		_rows = &TTRows{
			stmt:     s.stmt,
			cols:     nil,
			decltype: nil,
			colstype: nil,
			ctx:      ctx,
		}
		_rows.setColumnsAttrs()
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-done:
		return _rows, _err
	}
	// return rows, nil
}

var _ driver.Stmt = (*TTStmt)(nil)

// type TTStmtExecContext struct{}

// // ExecContext implements driver.StmtExecContext.
// func (t *TTStmtExecContext) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
// 	panic("unimplemented")
// }

// var _ driver.StmtExecContext = (*TTStmtExecContext)(nil)

// type TTStmtQueryContext struct{}

// // QueryContext implements driver.StmtQueryContext.
// func (t *TTStmtQueryContext) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
// 	panic("unimplemented")
// }

// var _ driver.StmtQueryContext = (*TTStmtQueryContext)(nil)

// type TTTx struct{}

// // Commit implements driver.Tx.
// func (t *TTTx) Commit() error {
// 	panic("unimplemented")
// }

// // Rollback implements driver.Tx.
// func (t *TTTx) Rollback() error {
// 	panic("unimplemented")
// }

// var _ driver.Tx = (*TTTx)(nil)

// type TTTxOptions struct{}

// var _ driver.TxOptions = (*TTTxOptions)(nil)


// type TTValue struct{}
// var _ driver.Value = (*TTValue)(nil)

func init() {
	sql.Register("xutt", &TTDriver{})
}
