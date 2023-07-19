package weSubDatabase

import (
	"fmt"
	"log"
	"time"
)

type Option struct {
	//	等待次数
	//
	//	Number of waits
	WaitCount int
	//	每次等待时间，单位毫秒
	//
	//	Waiting time per time, in milliseconds
	WaitTime int
	//	是否为主键
	//
	//	Whether it is a primary key
	IsPrimaryKey bool
	//	重试时间
	//
	//	Retry time
	RetryTime int
	//	是否输出到控制台
	//
	//	Whether to output to the console
	IsShowPrint bool
}

// 连接MySQL时的可选配置
//
// Optional configuration when connecting to MySQL
type LinkSQLO func(*Option)

// ===============
//
//	设置等待次数
//	WaitCount	int	"等待次数"
//
// ===============
//
//	Set the number of waits
//	WaitCount	int	"Number of waits"
func OLWaitCount(WaitCount int) LinkSQLO {
	return func(o *Option) {
		o.WaitCount = WaitCount
	}
}

// ===============
//
//	设置每次等待时间，单位毫秒
//	WaitTime	int	"每次等待时间，单位毫秒"
//
// ===============
//
//	Set the waiting time per time, in milliseconds
//	WaitTime	int	"Waiting time per time, in milliseconds"
func OLWaitTime(WaitTime int) LinkSQLO {
	return func(o *Option) {
		o.WaitTime = WaitTime
	}
}

// ===============
//
//	设置是输出到控制台
//	IsShowPrint	bool	"是否输出到控制台"
//
// ===============
//
//	Set whether to output to the console
//	IsShowPrint	bool	"Whether to output to the console"
func OLIsShowPrint(IsShowPrint bool) LinkSQLO {
	return func(o *Option) {
		o.IsShowPrint = IsShowPrint
	}
}

// 是否为主键的可选配置
//
// Optional configuration for whether it is a primary key
type IsPrimaryKeyO func(*Option)

// ===============
//
//	设置是否为主键
//	IsPrimaryKey	bool	"是否为主键"
//
// ===============
//
//	Set whether it is a primary key
//	IsPrimaryKey	bool	"Whether it is a primary key"
func OIPKIsPrimaryKey(IsPrimaryKey bool) IsPrimaryKeyO {
	return func(o *Option) {
		o.IsPrimaryKey = IsPrimaryKey
	}
}

// ===============
//
//	是否输出到控制台
//	IsShowPrint	bool	"是否输出到控制台"
//
// ===============
//
//	Whether to output to the console
//	IsShowPrint	bool	"Whether to output to the console"
func OIPKIsShowPrint(IsShowPrint bool) IsPrimaryKeyO {
	return func(o *Option) {
		o.IsShowPrint = IsShowPrint
	}
}

// 是否输出到控制台
//
// Whether to output to the console
type IsShowPrintO func(*Option)

// ===============
//
//	是否输出到控制台
//	IsShowPrint	bool	"是否输出到控制台"
//
// ===============
//
//	Whether to output to the console
//	IsShowPrint	bool	"Whether to output to the console"
func OIsShowPrint(IsShowPrint bool) IsShowPrintO {
	return func(o *Option) {
		o.IsShowPrint = IsShowPrint
	}
}

// ===============
//
//	连接MySQL数据库并放入连接池
//	item		int		"配置文件中的第几个MySQL配置"
//	options		[]LinkSQLO	"配置"
//		WaitCount	int		"等待次数"
//		WaitTime	int		"每次等待时间，单位毫秒"
//	return 1	int		"连接池中的位置"
//	return 2	error		"错误信息"
//
// ===============
//
//	Connect to MySQL database and put it into Connection pool
//	item		int		"Which MySQL configuration in
//									the configuration file"
//	options		[]LinkSQLO	"Configuration"
//		WaitCount	int		"Number of waits"
//		WaitTime	int		"Waiting time per time,
//									in milliseconds"
//	return 1	int		"Position in the connection pool"
//	return 2	error		"Error message"
func (s *Setting) MysqlIsRun(item int, options ...LinkSQLO) (int, error) {
	option := &Option{
		WaitCount:   10,
		WaitTime:    500,
		IsShowPrint: false,
	}
	for _, o := range options {
		o(option)
	}
	if s.LinkNum >= s.MaxLink {
		WaitCount := 0
		for {
			if s.LinkNum < s.MaxLink {
				break
			}
			if WaitCount > option.WaitCount {
				return -1, fmt.Errorf("MySQL connections are full")
			}
			WaitCount += 1
			time.Sleep(time.Duration(option.WaitTime) * time.Millisecond)
		}
	}
	// println("==========\r\nMySQL连接中...")
	wSQLdb, err := s.Link(item)
	if err != nil {
		tn := time.Now()
		s.ConnectFailTime[item] = &tn
		return -1, err
	}
	ii := 0
	for i := 0; i < len(s.MySQLDB); i++ {
		if s.MySQLDB[i] == nil {
			ii = i
			break
		}
	}
	s.LinkNum += 1
	s.MySQLDB[ii] = wSQLdb
	if option.IsShowPrint {
		println("MySQL DB", item, "connection successful!")
	}
	return ii, nil
}

// ===============
//
//	关闭MySQL连接
//	i	int	"连接池中的位置"
//
// ===============
//
//	Close MySQL connection
//	i	int	"Position in the connection pool"
func (s *Setting) MysqlClose(i int, options ...IsShowPrintO) {
	if i < 0 || i >= len(s.MySQLDB) {
		return
	}
	if s.MySQLDB[i] != nil {
		s.MySQLDB[i].Close()
		s.MySQLDB[i] = nil
		s.LinkNum -= 1
		if s.LinkNum < 0 {
			s.LinkNum = 0
		}
		option := &Option{
			IsShowPrint: false,
		}
		for _, o := range options {
			o(option)
		}
		if option.IsShowPrint {
			println("MySQL Close Connection! Current number of connections:", s.LinkNum)
		}
	}
}

// ===============
//
//	根据 *MysqlDB 查询
//	sqlStr		string				"SQL 语句"
//	reqd		chan []map[string]string	"查询结果"
//	reerr		chan error			"错误信息"
//	Debug		*log.Logger			"Debug 日志对象"
//
// ===============
//
//	According to *MysqlDB query
//	sqlStr		string				"SQL statement"
//	reqd		chan []map[string]string	"query result"
//	reerr		chan error			"error message"
//	Debug		*log.Logger			"Debug log object"
func (s *Setting) go_query(i int, sqlStr string, reqd chan []map[string]string, reerr chan error, IsShowPrint bool, Debug *log.Logger) {
	mI, err := s.MysqlIsRun(i, OLIsShowPrint(IsShowPrint))
	if err != nil {
		s.MysqlClose(mI)
		reqd <- nil
		reerr <- err
		return
	}
	qd, err := s.MySQLDB[mI].QueryCMD(sqlStr, Debug, OIsShowPrint(IsShowPrint))
	s.MysqlClose(mI, OIsShowPrint(IsShowPrint))
	reqd <- qd
	reerr <- err
}

// ===============
//
//	根据 *Setting 从数据库集中调用单行SQL查询指令
//	sqlStr		string			"SQL指令"
//	Debug		*log.Logger		"调试输出"
//	options		[]IsShowPrintO		"配置"
//		IsShowPrint	bool			"是否输出到控制台"
//	return 1	[]map[string]string	"查询结果"
//	return 2	[]error			"错误信息"
//
// ===============
//
//	According to *Setting, call single row SQL query instruction from database set
//	sqlStr		string			"SQL instruction"
//	Debug		*log.Logger		"Debug output"
//	options		[]IsShowPrintO		"Configuration"
//		IsShowPrint	bool			"Whether to output to the
//											console"
//	return 1	[]map[string]string	"Query result"
//	return 2	[]error			"Error message"
func (s *MysqlDB) QueryCMD(sqlStr string, Debug *log.Logger, options ...IsShowPrintO) ([]map[string]string, error) {
	option := &Option{
		IsShowPrint: false,
	}
	for _, o := range options {
		o(option)
	}
	if Debug != nil {
		Debug.Println("[Query]", sqlStr)
	}
	if option.IsShowPrint {
		fmt.Println("[Query]", sqlStr)
	}
	query, err := s.DB.Query(sqlStr)
	if err != nil {
		if Debug != nil {
			Debug.Println("MySQL Query Error:", err)
		}
		return nil, err
	}
	return handleQD(query, Debug)
}

// ===============
//
//	根据 *MysqlDB 调用单行SQL查询指令
//	sqlStr		string				"SQL 语句"
//	reqd		chan []map[string]string	"查询结果"
//	reerr		chan error			"错误信息"
//	isShowPrint	bool				"是否输出到控制台"
//	Debug		*log.Logger			"Debug 日志对象"
//
// ===============
//
//	According to *MysqlDB query
//	sqlStr		string				"SQL statement"
//	reqd		chan []map[string]string	"query result"
//	reerr		chan error			"error message"
//	isShowPrint	bool				"Whether to output
//													to the console"
//	Debug		*log.Logger			"Debug log object"
func (s *Setting) go_exec(i int, sqlStr string, reLIid chan int64, reRA chan int64, reerr chan error, isShowPrint bool, Debug *log.Logger) {
	mI, err := s.MysqlIsRun(i)
	if err != nil {
		s.MysqlClose(mI)
		reLIid <- 0
		reRA <- 0
		reerr <- err
		return
	}
	lastInsertId, rowsAffected, err := s.MySQLDB[mI].ExecCMD(sqlStr, Debug, OIsShowPrint(isShowPrint))
	s.MysqlClose(mI)
	if reLIid != nil {
		reLIid <- lastInsertId
	}
	if reRA != nil {
		reRA <- rowsAffected
	}
	reerr <- err
}

// ===============
//
//	根据 *Setting 从数据库集中调用单行SQL指令
//	sqlStr		string			"SQL指令"
//	Debug		*log.Logger		"调试输出"
//	options		[]IsShowPrintO		"配置"
//		IsShowPrint	bool			"是否输出到控制台"
//	return 1	int64			"插入的行数"
//	return 2	int64			"影响的行数"
//	return 3	[]error			"错误信息"
//
// ===============
//
//	According to *Setting, call single row SQL instruction from database set
//	sqlStr		string			"SQL instruction"
//	Debug		*log.Logger		"Debug output"
//	options		[]IsShowPrintO		"Configuration"
//	isShowPrint	bool			"Whether to output
//											to the console"
//	return 1	int64			"Number of rows inserted"
//	return 2	int64			"Number of rows affected"
//	return 3	[]error			"Error message"
func (s *MysqlDB) ExecCMD(sqlStr string, Debug *log.Logger, options ...IsShowPrintO) (int64, int64, error) {
	option := &Option{
		IsShowPrint: false,
	}
	for _, o := range options {
		o(option)
	}
	if Debug != nil {
		Debug.Println("[ExecCMD]", sqlStr)
	}
	if option.IsShowPrint {
		fmt.Println("[ExecCMD]", sqlStr)
	}
	var (
		lastInsertId int64 = 0
		rowsAffected int64 = 0
		err          error = nil
	)
	result, err := s.DB.Exec(sqlStr)
	if err != nil {
		if Debug != nil {
			Debug.Println("MySQL Query Error:", err)
		}
		return lastInsertId, rowsAffected, err
	}
	lastInsertId, err = result.LastInsertId()
	if err != nil {
		if Debug != nil {
			Debug.Println("lastInsertId Error:", err)
		}
		return lastInsertId, rowsAffected, err
	}
	rowsAffected, err = result.RowsAffected()
	if err != nil {
		if Debug != nil {
			Debug.Println("rowsAffected Error:", err)
		}
		return lastInsertId, rowsAffected, err
	}
	return lastInsertId, rowsAffected, err
}
