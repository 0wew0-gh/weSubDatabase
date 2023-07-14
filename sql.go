package weSubDatabase

import (
	"fmt"
	"log"
	"time"
)

type Option struct {
	WaitCount    int  // 等待次数
	WaitTime     int  // 每次等待时间，单位毫秒
	IsPrimaryKey bool // 是否为主键
	RetryTime    int  // 重试时间
}
type LinkSQLOptionConfig func(*Option)

func OptionWaitCount(WaitCount int) LinkSQLOptionConfig {
	return func(o *Option) {
		o.WaitCount = WaitCount
	}
}
func OptionWaitTime(WaitTime int) LinkSQLOptionConfig {
	return func(o *Option) {
		o.WaitTime = WaitTime
	}
}

// ===============
//
//	连接MySQL数据库并放入连接池
//	item		int			配置文件中的第几个MySQL配置
//	options		[]LinkSQLOptionConfig	配置
//		WaitCount	int			等待次数
//		WaitTime	int			每次等待时间，单位毫秒
//
//	返回值1		int			连接池中的位置
//	返回值2		error			错误信息
//
// ===============
//
//	Connect to MySQL database and put it into Connection pool
//	item		int			Which MySQL configuration
//											in the configuration file
//	options		[]LinkSQLOptionConfig	Configuration
//		WaitCount	int			Number of waits
//		WaitTime	int			Waiting time per time,
//											in milliseconds
//
//	return 1	int			Position in the connection
//											pool
//	return 2	error			Error message
func (s *Setting) MysqlIsRun(item int, options ...LinkSQLOptionConfig) (int, error) {
	if s.LinkNum >= s.MaxLink {
		option := &Option{
			WaitCount: 10,
			WaitTime:  500,
		}
		for _, o := range options {
			o(option)
		}
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
	println("MySQL DB", item, "connection successful!")
	return ii, nil
}

// ===============
//
//	关闭MySQL连接
//	i	int	连接池中的位置
//
// ===============
//
//	Close MySQL connection
//	i	int	Position in the connection pool
func (s *Setting) MysqlClose(i int) {
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
		println("MySQL Close Connection! Current number of connections:", s.LinkNum)
	}
}

// ===============
//
//	根据 *MysqlDB 查询
//	sqlStr		string				SQL 语句
//	reqd		chan []map[string]string	查询结果
//	reerr		chan error			错误信息
//	Debug		*log.Logger			Debug 日志对象
//
// ===============
//
//	According to *MysqlDB query
//	sqlStr		string				SQL statement
//	reqd		chan []map[string]string	query result
//	reerr		chan error			error message
//	Debug		*log.Logger			Debug log object
func (s *Setting) go_query(i int, sqlStr string, reqd chan []map[string]string, reerr chan error, Debug *log.Logger) {
	mI, err := s.MysqlIsRun(i)
	if err != nil {
		s.MysqlClose(mI)
		reqd <- nil
		reerr <- err
		return
	}
	qd, err := s.MySQLDB[mI].QueryCMD(sqlStr, Debug)
	s.MysqlClose(mI)
	reqd <- qd
	reerr <- err
}

// ===============
//
//	根据 *Setting 从数据库集中调用单行SQL查询指令
//	sqlStr		string			SQL指令
//	Debug		*log.Logger		调试输出
//
//	返回值1		[]map[string]string	查询结果
//	返回值2		[]error			错误信息
//
// ===============
//
//	According to *Setting, call single row SQL query instruction from database set
//	sqlStr		string			SQL instruction
//	Debug		*log.Logger		Debug output
//
//	Return 1	[]map[string]string	Query result
//	Return 2	[]error			Error message
func (s *MysqlDB) QueryCMD(sqlStr string, Debug *log.Logger) ([]map[string]string, error) {
	if Debug != nil {
		Debug.Println("[Query]", sqlStr)
	}
	fmt.Println("[Query]", sqlStr)
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
//	sqlStr		string				SQL 语句
//	reqd		chan []map[string]string	查询结果
//	reerr		chan error			错误信息
//	Debug		*log.Logger			Debug 日志对象
//
// ===============
//
//	According to *MysqlDB query
//	sqlStr		string				SQL statement
//	reqd		chan []map[string]string	query result
//	reerr		chan error			error message
//	Debug		*log.Logger			Debug log object
func (s *Setting) go_exec(i int, sqlStr string, reLIid chan int64, reRA chan int64, reerr chan error, Debug *log.Logger) {
	mI, err := s.MysqlIsRun(i)
	if err != nil {
		s.MysqlClose(mI)
		reLIid <- 0
		reRA <- 0
		reerr <- err
		return
	}
	lastInsertId, rowsAffected, err := s.MySQLDB[mI].ExecCMD(sqlStr, Debug)
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
//	sqlStr		string			SQL指令
//	Debug		*log.Logger		调试输出
//
//	返回值1		[]map[string]string	查询结果
//	返回值2		[]error			错误信息
//
// ===============
//
//	According to *Setting, call single row SQL instruction from database set
//	sqlStr		string			SQL instruction
//	Debug		*log.Logger		Debug output
//
//	Return 1	[]map[string]string	Query result
//	Return 2	[]error			Error message
func (s *MysqlDB) ExecCMD(sqlStr string, Debug *log.Logger) (int64, int64, error) {
	if Debug != nil {
		Debug.Println("[ExecCMD]", sqlStr)
	}
	fmt.Println("[ExecCMD]", sqlStr)
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
