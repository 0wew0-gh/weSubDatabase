package weSubDatabase

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/0wew0-gh/simpleEncryption"
	_ "github.com/go-sql-driver/mysql"
)

type Setting struct {
	//	数据库配置
	//
	//	Database configuration
	SqlConfigs []SQLConfig
	//	数据库名
	//
	//	Database name
	DBName string
	//	下一个数据库ID号,起始为1
	//
	//	Next database ID number, starting at 1
	NextDBID int
	//	数据库最大数目
	//
	//	Maximum number of databases
	DBMaxNum int
	//	连接数目
	//
	//	Number of connections
	LinkNum int
	//	最大连接数目
	//
	//	Maximum number of connections
	MaxLink int
	//	数据库连接
	//
	//	Database connection
	MySQLDB []*MysqlDB
	//	加密对象
	//
	//	Encryption object
	SEKey *simpleEncryption.SecretKey
	//	重新连接数据库的时间间隔
	//
	//	Time interval for reconnecting to the database
	ConnectAgainTime int
	//	上次连接失败的时间, 用于判断是否需要重新连接
	//
	//	The last time the connection failed, used to determine whether to reconnect
	ConnectFailTime []*time.Time
}

type MysqlDB struct {
	//	数据库名
	//
	//	Database name
	Name string
	//	当前连接的数据库在配置中的位置
	//
	//	The location of the currently connected database in the configuration
	DBItem int
	//	数据库连接
	//
	//	Database connection
	DB *sql.DB
}

// 重试时间的可选配置
//
// Optional configuration of retry time
type RetryTimeO func(*Option)

// ===============
//
//	设置重试时间
//	RetryTime	int	"重试等待时间(ms)"
//
// ===============
//
//	Set retry time
//	RetryTime	int	"Retry wait time(ms)"
func OptionRetryTime(RetryTime int) RetryTimeO {
	return func(o *Option) {
		o.RetryTime = RetryTime
	}
}

// ===============
//
//	数据库配置
//	configString	string		"配置文件字符串"
//	options		[]RetryTimeO	"配置"
//		RetryTime	int		"重试时间(ms)"
//	return 1	*Setting	"数据库配置对象"
//	return 2	error		"错误信息"
//
// ===============
//
//	Database config
//	configString	string		"Configuration file string"
//	options		[]RetryTimeO	"Configuration"
//		RetryTime	int		"Retry time(ms)"
//	return 1	*Setting	"Database configuration object"
//	return 2	error		"Error message"
func New(configString string, options ...RetryTimeO) (*Setting, error) {
	option := &Option{
		RetryTime: 60000,
	}
	for _, o := range options {
		o(option)
	}
	var setting Setting
	config, err := GetConfig(configString)
	if err != nil {
		return nil, err
	}
	setting.SqlConfigs = config.Mysql
	setting.DBName = config.MysqlName
	setting.NextDBID = 1
	setting.DBMaxNum = len(setting.SqlConfigs)
	setting.LinkNum = 0
	setting.MaxLink = config.MaxLink.MySQL
	mySQLDBs := []*MysqlDB{}
	connectFailTime := []*time.Time{}
	for i := 0; i < setting.MaxLink; i++ {
		mySQLDBs = append(mySQLDBs, nil)
	}
	for i := 0; i < len(setting.SqlConfigs); i++ {
		connectFailTime = append(connectFailTime, nil)
	}
	setting.MySQLDB = mySQLDBs
	if config.Contrast.Key != nil {
		se, err := simpleEncryption.New(config.Contrast.ExtraItem, config.Contrast.Key[0], config.Contrast.Key[1])
		if err != nil {
			return nil, err
		}
		setting.SEKey = se
	}
	setting.ConnectAgainTime = option.RetryTime
	setting.ConnectFailTime = connectFailTime
	return &setting, nil
}

// ===============
//
//	连接数据库
//	item		int		"需要连接的数据库在配置中的位置"
//	return 1	*MysqlDB	"连接池中的位置"
//	return 2	error		"错误信息"
//
// ===============
//
//	Connect to MySQL database
//	item		int		"Location of the database to be
//						 		connected in the configuration"
//	return 1	*MysqlDB	"Location in the connection pool"
//	return 2	error		"Error message"
func (s *Setting) Link(item int) (*MysqlDB, error) {
	if s.SqlConfigs == nil || len(s.SqlConfigs) == 0 {
		return nil, fmt.Errorf("MySQL Open Error: %s", "配置为空")
	}
	var (
		sqlJson *SQLConfig
		err     error
	)
	sqlJson = &s.SqlConfigs[item]
	var sqlsetting string = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", sqlJson.User, sqlJson.Password, sqlJson.Address, sqlJson.Port, sqlJson.DB)
	sqldb, err := sql.Open("mysql", sqlsetting)
	if err != nil {
		return nil, err
	}
	if err := sqldb.Ping(); err != nil {
		return nil, err
	}
	return &MysqlDB{Name: sqlJson.DB, DBItem: item, DB: sqldb}, nil
}

// ===============
//
//	关闭数据库
//
// ===============
//
//	Close database
func (db *MysqlDB) Close() {
	if db.DB != nil {
		db.DB.Close()
		db.DB = nil
		db.Name = ""
		db.DBItem = -1
	}
}
