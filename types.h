#ifndef __types_h_
#define __types_h_ 
#ifdef __cplusplus
extern "C" {
#endif 
typedef long long SQLBIGINT;
typedef signed short SQLSMALLINT;
typedef unsigned short SQLUSMALLINT;
typedef unsigned int SQLUINTEGER;
typedef struct tagDATE_STRUCT {
    SQLSMALLINT year;
    SQLUSMALLINT month;
    SQLUSMALLINT day;  
}
DATE_STRUCT;

typedef struct tagTIME_STRUCT
  {
    SQLUSMALLINT hour;
    SQLUSMALLINT minute;
    SQLUSMALLINT second;
  }
TIME_STRUCT;

typedef struct tagTIMESTAMP_STRUCT
  {
    SQLSMALLINT year;
    SQLUSMALLINT month;
    SQLUSMALLINT day;
    SQLUSMALLINT hour;
    SQLUSMALLINT minute;
    SQLUSMALLINT second;
    SQLUINTEGER fraction;
  }
TIMESTAMP_STRUCT;

#ifdef __cplusplus
}
#endif
#endif