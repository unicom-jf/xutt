#ifndef __ttdriver_h
#define __ttdriver_h
// #include "types.h"

#ifdef _WIN32
	// 为了让头文件同时用于编译DLL和使用DLL，我们定义宏
	#ifdef TT_EXPORTS
		#define TT_API __declspec(dllexport)
	#else
		#define TT_API __declspec(dllimport)
	#endif
#else
	#define TT_API
#endif

#ifdef __cplusplus
extern "C" {
#endif

#define SQL_UNKNOWN_TYPE			0
#define SQL_CHAR				1
// Column #20 : FLD_TT_CHAR, type: 1, typeName:TT_CHAR, scale: 0
#define SQL_NUMERIC				2
#define SQL_DECIMAL				3
// Column #12 : FLD_INTEGER, type: 3, typeName:NUMBER, scale: 0, when scale>0, double
// Column #16 : FLD_SMALLINT, type: 3, typeName:NUMBER, scale: 0
#define SQL_INTEGER				4
// Column #22 : FLD_TT_INTEGER, type: 4, typeName:TT_INTEGER, scale: 0 only
#define SQL_SMALLINT				5
// Column #25 : FLD_TT_SMALLINT, type: 5, typeName:TT_SMALLINT, scale: 0
#define SQL_FLOAT				6
// Column #7 : FLD_BINARY_DOUBLE, type: 6, typeName:BINARY_DOUBLE, scale: 0
#define SQL_REAL				7
// Column #8 : FLD_BINARY_FLOAT, type: 7, typeName:BINARY_FLOAT, scale: 0
#define SQL_DOUBLE				8
// Column #1 : FLD_NUMBER, type: 8, typeName:NUMBER, scale: 0
// Column #10 : FLD_DOUBLEPRECISION, type: 8, typeName:FLOAT, scale: 0
// Column #11 : FLD_FLOAT, type: 8, typeName:FLOAT, scale: 0
// Column #14 : FLD_REAL, type: 8, typeName:FLOAT, scale: 0
#define TT_DATE				9
// Column #21 : FLD_TT_DATE, type: 9, typeName:TT_DATE, scale: 0--2025-07-25
#define SQL_TIME				10
// Column #17 : FLD_TIME, type: 10, typeName:TT_TIME, scale: 0 23:59:59
#define TT_DATETIME				11
// Column #2 : FLD_DATE, type: 11, typeName:DATE, scale: 0 --2025-07-25 11:07:45
// Column #18 : FLD_TIMESTAMP, type: 11, typeName:TIMESTAMP, scale: 6--2025-07-25 11:12:34.000000
// Column #26 : FLD_TT_TIMESTAMP, type: 11, typeName:TT_TIMESTAMP, scale: 6
#define SQL_VARCHAR				12
// Column #0 : FLD_VARCHAR2, type: 12, typeName:VARCHAR2, scale: 0
// Column #28 : FLD_TT_VARCHAR, type: 12, typeName:TT_VARCHAR, scale: 1
#define SQL_TYPE_DATE				91
#define SQL_TYPE_TIME				92
#define SQL_TYPE_TIMESTAMP			93

#define TT_CLOB				-1
// Column #4 : FLD_CLOB, type: -1, typeName:CLOB, scale: 0
#define TT_BINARY			-2
// Column #6 : FLD_BINARY, type: -2, typeName:BINARY, scale: 0
#define TT_VARBINARY		-3
// Column #29 : FLD_VARBINARY, type: -3, typeName:VARBINARY, scale: 0
#define TT_BLOB				-4
// Column #3 : FLD_BLOB, type: -4, typeName:BLOB, scale: 0
#define TT_BIGINT			-5
// Column #19 : FLD_TT_BIGINT, type: -5, typeName:TT_BIGINT, scale: 0
#define TT_TINYINT			-6
// Column #27 : FLD_TT_TINYINT, type: -6, typeName:TT_TINYINT, scale: 0
#define TT_NCHAR			-8
// Column #13 : FLD_NCHAR, type: -8, typeName:NCHAR, scale: 0
// Column #23 : FLD_TT_NCHAR, type: -8, typeName:TT_NCHAR, scale: 0
#define TT_NVARCHAR			-9
// Column #24 : FLD_TT_NVARCHAR, type: -9, typeName:TT_NVARCHAR, scale: 0
#define TT_NCLOB			-10
// Column #5 : FLD_NCLOB, type: -10, typeName:NCLOB, scale: 0
#define TT_ROWID			-101
// Column #15 : FLD_ROWID, type: -101, typeName:ROWID, scale: 0

#define SQL_GO_INT "SQL_GO_INT" 
#define SQL_GO_DOUBLE "SQL_GO_DOUBLE"
//sql_char, sql_varchar
#define SQL_GO_CHAR "SQL_GO_CHAR"
#define SQL_GO_WCHAR "SQL_GO_WCHAR"

#define SQL_GO_DATE "SQL_GO_DATE"
#define SQL_GO_TIME "SQL_GO_TIME"
#define SQL_GO_DATETIME "SQL_GO_DATETIME"
#define SQL_GO_BYTES "SQL_GO_BYTES"

#define MaxMsgLen 1024
#define MaxColumnNameLen 256
// typedef struct tagDATE_STRUCT DATE_STRUCT_C;
// typedef struct tagTIMESTAMP_STRUCT TIMESTAMP_STRUCT_C;
// typedef struct tagTIME_STRUCT TIME_STRUCT_C;
typedef struct DrvStatus {
	int no;
	char msg[MaxMsgLen];
} DrvStatus;
// typedef struct WCharFld {
// 	unsigned long len;
// 	unsigned short* pFld;
// 	// public:
// 	// unsigned short get(long i) {
// 	// 	return *(pFld + i)
// 	// }
// } WCharFld;
typedef struct WCharFld {
	// sizeof wchar
	unsigned long len;
	unsigned short* pFld;
} WCharFld;

typedef struct BlobFld {
	// sizeof char
	unsigned long len;
	unsigned char* pFld;
} BlobFld;

typedef struct TTParam {
	unsigned int sqlQueryTimeout; // in seconds
} TTParam;

#define TTConnPtr void*
#define TTCmdPtr void*

TT_API int TTClientVersion();

TT_API TTConnPtr TTConnNew(char* dsn, TTParam* param, DrvStatus* status);
// TTConnPtr TTConnNew(char* dsn, DrvStatus* status);
TT_API void TTDisconn(TTConnPtr conn, DrvStatus* status);
TT_API void TTCommit(TTConnPtr conn, DrvStatus* status);
TT_API void TTRollback(TTConnPtr conn, DrvStatus* status);
TT_API void TTPing(TTConnPtr conn, DrvStatus* status);
TT_API void TTCommitTest(TTConnPtr conn);

TT_API TTCmdPtr TTCmdNew(DrvStatus* status);
// void TTCmdPrepare(TTConnPtr conn, TTCmdPtr cmd, char* sql, DrvStatus* status);
TT_API void TTCmdPrepare(TTConnPtr conn, TTCmdPtr cmd, const TTParam param, char* sql, DrvStatus* status);

// void TTCmdBind(TTConnPtr conn, TTCmdPtr cmd, char* sql, DrvStatus* status);
TT_API void TTCmdQuery(TTCmdPtr cmd, DrvStatus* status);
TT_API void TTCmdExecute(TTCmdPtr cmd, long* id, long* rowsAffected, DrvStatus* status);
TT_API int TTCmdColumnCount(TTCmdPtr cmd, DrvStatus* status);
// #include <stdbool.h>

TT_API int TTCmdParamsCount(TTCmdPtr cmd, DrvStatus* status);
TT_API void TTGetColumnName(TTCmdPtr cmd, int col, char* name, DrvStatus* status);
TT_API void TTGetColumnTypeName(TTCmdPtr cmd, int col, char* name, DrvStatus* status);
TT_API TT_API int TTGetNextRow(TTCmdPtr cmd, DrvStatus* status);
// void TTGetInt(TTCmdPtr cmd, int col, int* value, DrvStatus* status);
// void TTGetDouble(TTCmdPtr cmd, int col, double* value, DrvStatus* status);
TT_API size_t TTGetColumnLength(TTCmdPtr cmd, int col, DrvStatus* status);
TT_API void TTGetColumnValue(TTCmdPtr cmd, int col, int type, void* value, unsigned char* isNull, DrvStatus* status);
TT_API void TTGetCharValue(TTCmdPtr cmd, int col, int type, void* value, unsigned char* isNull, long* strLen, DrvStatus* status);
// WCharFld* TTGetWCharValue(TTCmdPtr cmd, int col, int type, DrvStatus* status);
TT_API WCharFld TTGetWCharValue(TTCmdPtr cmd, int col, int type, unsigned char* isNull, DrvStatus* status);
TT_API BlobFld TTGetBlobValue(TTCmdPtr cmd, int col, int type, unsigned char* isNull, DrvStatus* status);

TT_API void TTCmdClose(TTCmdPtr cmd, DrvStatus* status);
TT_API void TTCmdDrop(TTCmdPtr cmd, DrvStatus* status);

TT_API void TTSetParamBinary(TTCmdPtr cmd, int pos, void* buf, int len, DrvStatus* status);
// void TTSetParamChar(TTCmdPtr cmd, int pos, void* buf, DrvStatus* status);
TT_API void TTSetParamChar(TTCmdPtr cmd, int pos, char* buf, DrvStatus* status);
TT_API void TTSetParamWChar(TTCmdPtr cmd, int pos, void* buf, int len, DrvStatus* status);
TT_API void TTSetParamTimestamp(TTCmdPtr cmd, int pos, void* buf, DrvStatus* status);

TT_API void TTSetParamBigInt(TTCmdPtr cmd, int pos, long long buf, DrvStatus* status);
TT_API void TTSetParamDouble(TTCmdPtr cmd, int pos, double buf, DrvStatus* status);

TT_API void setStatus(DrvStatus* status, int no, const char* msg);
TT_API void type2Name(int type, int scale, char* name);
// unsigned short getWChar(WCharFld* fld, long pos);
TT_API unsigned short getWChar(WCharFld fld, long pos);
TT_API void freeWChar(WCharFld fld);
TT_API void freeBlob(BlobFld fld);
TT_API void getTTParam(void* conn, TTParam* param);

#ifdef __cplusplus                           
}
#endif
#endif
