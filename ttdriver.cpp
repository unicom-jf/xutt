//go:build !windows

#include <cstring>
#include <iostream>
#include <ttclasses/TTInclude.h>
#include "ttdriver.h"
//debug
void printTimestamp(TIMESTAMP_STRUCT* ts);
void printDate(DATE_STRUCT* d);
void printTime(TIME_STRUCT* t);

TTConnPtr TTConnNew(char* dsn, TTParam* param, DrvStatus* status) {
// TTConnPtr TTConnNew(char* dsn, DrvStatus* status) {
    status->no = 0;
    TTConnection* conn = NULL;
    try {
        conn = new TTConnection();
        // conn->Connect(dsn, TTConnection::DRIVER_COMPLETE);
        conn->Connect(dsn);
        // cout << "conn id: " << conn << endl;
        getTTParam(conn, param);
        return conn;
    }
    catch (TTError const & st) {
        setStatus(status, st.native_error, st.err_msg);
        return conn;
    }
}
void TTPing(TTConnPtr conn, DrvStatus* status) {
    status->no = 0;
    try {
        TTConnection* _conn = (TTConnection*) conn;
        TTCmd cmd;
        cmd.Prepare(_conn, "select 1 from dual");
        cmd.Execute();
        cmd.Close();
    } catch(TTError const & st) {
        setStatus(status, st.native_error, st.err_msg);
    }
}
// void TTCmdPrepare(TTConnPtr conn, TTCmdPtr cmd, char* sql, DrvStatus* status) {
void TTCmdPrepare(TTConnPtr conn, TTCmdPtr cmd, const TTParam param, char* sql, DrvStatus* status) {
    status->no = 0;
    TTConnection* _conn = (TTConnection*)conn;
    TTCmd* _cmd = (TTCmd*)cmd;
    try {
        _cmd->Prepare(_conn, sql);
        if(param.sqlQueryTimeout > 0) {
            _cmd->setQueryTimeout(param.sqlQueryTimeout);
        }
    }
    catch (TTError const & st) {
        setStatus(status, st.native_error, st.err_msg);
    }
}

void TTDisconn(TTConnPtr conn, DrvStatus* status) {
    status->no = 0;
    try {
        TTConnection* _conn = (TTConnection*) conn;
        _conn->Disconnect();
    } catch(TTError const & st) {
        setStatus(status, st.native_error, st.err_msg);
    }
}
void TTCommit(TTConnPtr conn, DrvStatus* status) {
    status->no = 0;
    try {
        TTConnection* _conn = (TTConnection*) conn;
        // cout << "commit id: " << _conn << endl;
        _conn->Commit();
        // cout << "commit is ok." << endl;
    } catch(TTError const & st) {
        cout << st << endl;
        setStatus(status, st.native_error, st.err_msg);
    }
}

void TTCommitTest(TTConnPtr conn){
try {
        TTConnection* _conn = (TTConnection*) conn;
        _conn->Commit();
    } catch(TTError const & st) {
        cout << st << endl;
    }
}
void TTRollback(TTConnPtr conn, DrvStatus* status) {
    status->no = 0;
    try {
        TTConnection* _conn = (TTConnection*) conn;
        _conn->Rollback();
    } catch(TTError const & st) {
        setStatus(status, st.native_error, st.err_msg);
    }
}

void TTCmdQuery(TTCmdPtr cmd, DrvStatus* status) {
    // cout << "query ptr: " << cmd << endl;
    status->no = 0;
    try {
        TTCmd* _cmd = (TTCmd*) cmd;
        _cmd->Execute();
    } catch(TTError const & st) {
        setStatus(status, st.native_error, st.err_msg);
    }
}
void TTCmdExecute(TTCmdPtr cmd, long* id, long* rowsAffected, DrvStatus* status) {
    try {
        TTCmd* _cmd = (TTCmd*) cmd;
        _cmd->Execute();
        *rowsAffected = _cmd->getRowCount();
    } catch(TTError const & st) {
        setStatus(status, st.native_error, st.err_msg);
    }
}
int TTCmdParamsCount(TTCmdPtr cmd, DrvStatus* status) {
    status->no = 0;
    try {
        TTCmd* _cmd = (TTCmd*) cmd;
        return _cmd->getNParameters();
    } catch(TTError const & st) {
        setStatus(status, st.native_error, st.err_msg);
        return -1;
    }
}
int TTCmdColumnCount(TTCmdPtr cmd, DrvStatus* status) {
    status->no = 0;
    try {
        TTCmd* _cmd = (TTCmd*) cmd;
        return _cmd->getNColumns();
    } catch(TTError const & st) {
        setStatus(status, st.native_error, st.err_msg);
        return -1;
    }

}
void TTGetColumnName(TTCmdPtr cmd, int col, char* name, DrvStatus* status) {
    status->no = 0;
    try {
        TTCmd* _cmd = (TTCmd*) cmd;
        strncpy(name, _cmd->getColumnName(col), MaxColumnNameLen - 1);
    } catch(TTError const & st) {
        setStatus(status, st.native_error, st.err_msg);
    }

}
//summary smallint, int etc to SQL_GO_INT
//decimal or number with scale to SQL_GO_DOUBLE
//char, varchar, clob etc to SQL_GO_CHAR
//date, time, timestamp to SQL_GO_TIMESTAMP
//blob to SQL_GO_BYTES
void TTGetColumnTypeName(TTCmdPtr cmd, int col, char* name, DrvStatus* status) {
    status->no = 0;
    try {
        TTCmd* _cmd = (TTCmd*) cmd;
        int type = _cmd->getColumnType(col);
        type2Name(_cmd->getColumnType(col), _cmd->getColumnScale(col), name);
    } catch(TTError const & st) {
        setStatus(status, st.native_error, st.err_msg);
    }
}
size_t TTGetColumnLength(TTCmdPtr cmd, int col, DrvStatus* status){
    status->no = 0;
    try {
        TTCmd* _cmd = (TTCmd*) cmd;
        return (size_t) _cmd->getColumnLength(col);
    } catch(TTError const & st) {
        setStatus(status, st.native_error, st.err_msg);
    }
}
int TTGetNextRow(TTCmdPtr cmd, DrvStatus* status) {
    // cout << "fetch ptr: " << cmd << endl;
    status->no = 0;
    try {
        TTCmd* _cmd = (TTCmd*) cmd;
        const int flag = _cmd->FetchNext();
        if(flag == 1)
            _cmd->Close();
        return flag;
    } catch(TTError const & st) {
        setStatus(status, st.native_error, st.err_msg);
    }
    return -1;

}

void TTGetColumnValue(TTCmdPtr cmd, int col, int type, void* value, unsigned char* isNull, DrvStatus* status) {
    status->no = 0;
    try {
        TTCmd* _cmd = (TTCmd*) cmd;
        switch (type)
        {
        case SQL_INTEGER:
            _cmd->getColumnNullable(col, (int*)value) ? *isNull = 1 : *isNull = 0;
            break;
        case SQL_DOUBLE:
            _cmd->getColumnNullable(col, (double*)value) ? *isNull = 1 : *isNull = 0;
            break;
        case TT_DATE:
            _cmd->getColumnNullable(col, (DATE_STRUCT*)value) ? *isNull = 1 : *isNull = 0;
            // printDate((DATE_STRUCT*)value);
            break;
        case SQL_TIME:
            _cmd->getColumnNullable(col, (TIME_STRUCT*)value) ? *isNull = 1 : *isNull = 0;
            // printTime((TIME_STRUCT*)value);
            break;
        case TT_DATETIME:
            _cmd->getColumnNullable(col, (TIMESTAMP_STRUCT*)value) ? *isNull = 1 : *isNull = 0;
            // printTimestamp((TIMESTAMP_STRUCT*)value);
            break;
        case SQL_CHAR:
            _cmd->getColumnNullable(col, (char*)value) ? *isNull = 1 : *isNull = 0;
            break;        
        default:
            break;
        }
         
    } catch(TTError const & st) {
        setStatus(status, st.native_error, st.err_msg);
    }
}
int TTClientVersion() {
    #ifdef TT18
    return 18;
    #elif TT22
    return 22;
    #else
    return 0;
    #endif
}
//WCharFld* TTGetWCharValue(TTCmdPtr cmd, int col, int type, DrvStatus* status) {
WCharFld TTGetWCharValue(TTCmdPtr cmd, int col, int type, unsigned char* isNull, DrvStatus* status) {
    status->no = 0;
    WCharFld rslt;
    rslt.len = -1;
    try {
        TTCmd* _cmd = (TTCmd*) cmd;
        switch (type)
        {
        case TT_NCHAR:
            SQLWCHAR* fld;//, fld2;
            SQLLEN len;
            int iLen;
            #ifdef TT18
            _cmd->getColumnNullable(col, &fld, &iLen) ? *isNull = 1 : *isNull = 0;
            len = iLen;
            #elif TT22
            _cmd->getColumnNullable(col, &fld, &len) ? *isNull = 1 : *isNull = 0;
            #else
            _cmd->getColumnNullable(col, &fld, &len) ? *isNull = 1 : *isNull = 0;
            #endif
            rslt.len = len / 2;
            rslt.pFld =(unsigned short*) fld;
            break;
        default:
            break;
        }
         
    } catch(TTError const & st) {
        setStatus(status, st.native_error, st.err_msg);
    }
    return rslt;
}

BlobFld TTGetBlobValue(TTCmdPtr cmd, int col, int type, unsigned char* isNull, DrvStatus* status) {
    status->no = 0;
    BlobFld rslt;
    rslt.len = -1;
    try {
        TTCmd* _cmd = (TTCmd*) cmd;
        switch (type)
        {
        case TT_BLOB:
            unsigned char* fld;//, fld2;
            SQLLEN len;
            int iLen;
            #ifdef TT18
            _cmd->getColumnNullable(col, (void **)&fld, &iLen) ? *isNull = 1 : *isNull = 0;
            len = iLen;
            #elif TT22
            _cmd->getColumnNullable(col, (void **)&fld, &len) ? *isNull = 1 : *isNull = 0;
            #else
            _cmd->getColumnNullable(col, (void **)&fld, &len) ? *isNull = 1 : *isNull = 0;
            #endif
            rslt.len = len;
            rslt.pFld = fld;
            break;
        default:
            break;
        }         
    } catch(TTError const & st) {
        setStatus(status, st.native_error, st.err_msg);
    }
    return rslt;
}

void TTSetParamWChar(TTCmdPtr cmd, int pos, void* buf, int len, DrvStatus* status) {
    status->no = 0;
    try {
        TTCmd* _cmd = (TTCmd*) cmd;
        _cmd->setParam(pos, (SQLWCHAR*) buf, len);         
    } catch(TTError const & st) {
        setStatus(status, st.native_error, st.err_msg);
    }
}
void TTSetParamBinary(TTCmdPtr cmd, int pos, void* buf, int len, DrvStatus* status) {
    status->no = 0;
    try {
        TTCmd* _cmd = (TTCmd*) cmd;
        _cmd->setParam(pos, buf, len);         
    } catch(TTError const & st) {
        setStatus(status, st.native_error, st.err_msg);
    }
}
// void TTSetParamChar(TTCmdPtr cmd, int pos, void* buf, DrvStatus* status) {
void TTSetParamChar(TTCmdPtr cmd, int pos, char* buf, DrvStatus* status) {
    status->no = 0;
    try {
        TTCmd* _cmd = (TTCmd*) cmd;
        // _cmd->setParam(pos, (char*)buf);         
        _cmd->setParam(pos, buf);
    } catch(TTError const & st) {
        setStatus(status, st.native_error, st.err_msg);
    }
}
void TTSetParamTimestamp(TTCmdPtr cmd, int pos, void* buf, DrvStatus* status){
    status->no = 0;
    try {
        TTCmd* _cmd = (TTCmd*) cmd;
        _cmd->setParam(pos, *((TIMESTAMP_STRUCT*) buf));  
        //cout << *buf << endl;       
    } catch(TTError const & st) {
        setStatus(status, st.native_error, st.err_msg);
    }
}

void TTSetParamBigInt(TTCmdPtr cmd, int pos, long long buf, DrvStatus* status) {
    status->no = 0;
    try {
        TTCmd* _cmd = (TTCmd*) cmd;
        _cmd->setParam(pos, (SQLBIGINT) buf);  
    } catch(TTError const & st) {
        setStatus(status, st.native_error, st.err_msg);
    }
}
void TTSetParamDouble(TTCmdPtr cmd, int pos, double buf, DrvStatus* status) {
    status->no = 0;
    try {
        TTCmd* _cmd = (TTCmd*) cmd;
        _cmd->setParam(pos, buf);  
    } catch(TTError const & st) {
        setStatus(status, st.native_error, st.err_msg);
    }
}

//SQL_GO_CHAR SQL_GO_TIMESTAMP SQL_GO_BYTES

void type2Name(int type, int scale, char* name) {
    switch (type) {
        case SQL_CHAR:
        case SQL_VARCHAR:
        case TT_CLOB:
        case TT_ROWID:
            strcpy(name, SQL_GO_CHAR);
            break;
        case TT_NCHAR://SQL_WCHAR:
        case TT_NVARCHAR:
        case TT_NCLOB:
            strcpy(name, SQL_GO_WCHAR);
            break;
            break;
        case SQL_NUMERIC:
        case SQL_DECIMAL:
            if(scale == 0) {
                strcpy(name, SQL_GO_INT);
            } else {
                strcpy(name, SQL_GO_DOUBLE);
            }
            break;
        case SQL_INTEGER:
        case SQL_SMALLINT:
        case TT_BIGINT:
        case TT_TINYINT:
            strcpy(name, SQL_GO_INT);
            break;
        case SQL_FLOAT:
        case SQL_REAL:
        case SQL_DOUBLE:
            strcpy(name, SQL_GO_DOUBLE);
            break;
        case TT_DATE:
        // case SQL_TYPE_DATE:        
            strcpy(name, SQL_GO_DATE);
            break;
        case SQL_TIME: //h:m:s
        // case SQL_TYPE_TIME:
            strcpy(name, SQL_GO_TIME);
            break;
        case TT_DATETIME:
        // case SQL_TYPE_TIMESTAMP:    
            strcpy(name, SQL_GO_DATETIME);
            break;
        case TT_BINARY:
        case TT_VARBINARY:
        case TT_BLOB:
            strcpy(name, SQL_GO_BYTES);
            break;
    }
}
TTCmdPtr TTCmdNew(DrvStatus* status) {
    status->no = 0;
    TTCmd* cmd = NULL;
    try {
        cmd = new TTCmd();
        return cmd;
    }
    catch (TTError const & st) {
        setStatus(status, st.native_error, st.err_msg);
        return cmd;
    }

}
void TTCmdClose(TTCmdPtr cmd, DrvStatus* status) {
    status->no = 0;
    try {
        TTCmd* _cmd = (TTCmd*)cmd;
        _cmd->Close();
    }
    catch (TTError const & st) {
        setStatus(status, st.native_error, st.err_msg);
    }
}
void TTCmdDrop(TTCmdPtr cmd, DrvStatus* status) {
    status->no = 0;
    try {
        TTCmd* _cmd = (TTCmd*)cmd;
        _cmd->Drop();
    }
    catch (TTError const & st) {
        setStatus(status, st.native_error, st.err_msg);
    }
}

void setStatus(DrvStatus* status, int no, const char* msg) {
    status->no = no;
    if(status->no == 0) {
        status->no = -1;
    }
    strncpy(status->msg, msg, MaxMsgLen - 1);    
}

void printTimestamp(TIMESTAMP_STRUCT* ts) {
  cout << "TIMESTAMP: " << ts->year << "-" << ts->month << "-" << ts->day << " " << ts->hour << ":" << ts->minute << ":" << ts->second << "." << ts->fraction << std::endl;
}
void printDate(DATE_STRUCT* d) {
  cout << "DATE: " << d->year << "-" << d->month << "-" << d->day << std::endl;
}
void printTime(TIME_STRUCT* t) {
  cout << "TIME: " << t->hour << ":" << t->minute << ":" << t->second << std::endl;
}
// unsigned short getWChar(WCharFld* fld, long pos) {
//     return * (fld->pFld + pos);
// }
unsigned short getWChar(WCharFld fld, long pos) {
    return * (fld.pFld + pos);
}
void freeWChar(WCharFld fld) {
    if(fld.len == 0 || fld.pFld == NULL) {
        return;
    }
    // delete [] fld.pFld;
    free(fld.pFld);
    fld.pFld = NULL;
}
void freeBlob(BlobFld fld) {
    if(fld.pFld == NULL) {
        return;
    }
    // delete [] fld.pFld;
    free(fld.pFld);
    fld.pFld = NULL;
}
void getTTParam(void* conn, TTParam* param) {
    TTConnection* _conn = (TTConnection*) conn;
    TTCmd* cmd = new TTCmd();
    cmd->Prepare(_conn, "call ttconfiguration");
    cmd->Execute();
    char key[1024], val[1024];
    while(cmd->FetchNext() == 0) {
        cmd->getColumn(1, key);
        if(strcmp(key, "SQLQueryTimeout") == 0) {
            cmd->getColumn(2, val);
            param->sqlQueryTimeout = (unsigned int) atoi(val);
        }
    }
    cmd->Close();
    delete cmd;
}