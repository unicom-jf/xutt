# xutt

# golang sql driver for Oracle Timesten

# 简介

Oracle 的 Timesten 数据库的 go 的驱动程序，支持 Windows（10 及以上）、linux、MacOS 操作系统，支持 Timesten 的 18、22 版本（客户端、服务器端）。

# 部署条件

- g++运行环境（Windows 环境下推荐使用 MSYS2）
- Timesten 的客户端的实例安装

# 部署说明

由于使用 CGO 调用 Timesten 的客户端，需要设置环境变量：

CGO_ENABLED=1

- Windows

  - 根据本地安装的 Timesten 的客户端版本，将提供的对应的预编译的动态库复制到客户端的 bin 目录下

  - 设置环境变量， 以 18 版本为例：

  CGO_LDFLAGS=-L 客户端的安装目录\bin -lttdriver1864

- linux、macos

  - 设置环境变量

  CGO_CPPFLAGS=" -g -Wall -W -Wcast-qual -Wshadow -Wpointer-arith -Wno-return-type -Wno-unused-variable -Wno-unused-parameter -DTTDEBUG -DTT_64BIT -ITimesten 的 home 目录/install/include -ITimesten 的 home 目录/install/include/ttclasses -DGCC -D_THREAD_SAFE -D_REENTRANT"

  CGO_LDFLAGS=" -LTimesten 的 home 目录/install/lib -lttclient -lttclassesCS"

# 代码样例

- 直接构造 dsn 字符串：

  - 不使用 tls：

    options := xutt.Options{

    RequireEncryption: false,

    QueryTimeout: 0,

    Charset: "utf8",

    }

    dsn := xutt.ConnParam{

    Host: TT 数据库 IP,

    Port: TT 服务器对客户端的服务端口,

    User: TT 的用户名,

    Password: TT 的该用户密码,

    DBName: TT 数据库本地访问的 dsn,

    Options: options,

    }

    db, err := sql.Open("xutt", dsn.Dsn())

  - 使用 tls：

    options := xutt.Options{

    RequireEncryption: true,

    Wallet: 从 TT 服务器导入的 wallet 目录,

    CipherSuites: 服务器端配置的加密算法,

    QueryTimeout: 0,

    Charset: "utf8",

    }

    dsn := xutt.ConnParam{

    Host: TT 数据库 IP,

    Port: TT 服务器对客户端的服务端口,

    User: TT 的用户名,

    Password: TT 的该用户密码,

    DBName: TT 数据库本地访问的 dsn,

    Options: options,

    }

    db, err := sql.Open("xutt", dsn.Dsn())

- 参考 ttdriver_test.go，配置对应的环境变量
