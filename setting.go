package weSubDatabase

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/0wew0-gh/simpleEncryption"
	"github.com/go-redis/redis/v8"
	_ "github.com/go-sql-driver/mysql"
)

type Setting struct {
	//	数据库配置
	//
	//	Database configuration
	SqlConfigs []SQLConfig
	// //	数据库名
	// //
	// //	Database name
	// DBName string
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

	//	Redis配置
	//
	//	Redis configuration
	RedisConfigs []RedisConfig
	//	下一个Redis数据库ID号,起始为1
	//
	//	Next Redis database ID number, starting at 1
	NextRedisDBID int
	//	Redis最大数目
	//
	//	Maximum number of Redis
	RedisMaxNum int
	//	连接数目
	//
	//	Number of connections
	RedisLinkNum int
	//	最大连接数目
	//
	//	Maximum number of connections
	RedisMaxLink int
	//	Redis连接
	//
	//	Redis connection
	RedisDB []*RedisDB
	//	上次连接失败的时间, 用于判断是否需要重新连接
	//
	//	The last time the connection failed, used to determine whether to reconnect
	RedisConnectFailTime []*time.Time
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

type RedisDB struct {
	//	数据库地址
	//
	//	Database Address
	Addr string
	//	当前连接的数据库在配置中的位置
	//
	//	The location of the currently connected database in the configuration
	DBItem int
	//	数据库连接
	//
	//	Database connection
	DB *redis.Client
}

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
	//	Redis专用：在查詢完成後刪除此條目
	//
	//	Redis special: delete this entry after the query is completed
	IsDelete bool
	//	Redis专用：資料條目的超時時間（秒）
	//
	//	Redis special: timeout time of data entry (seconds)
	AutoDeleteTime int
	//	Redis专用：在批次操作中是否遇到錯誤就停止
	//
	//	Redis special: whether to stop if an error is encountered in batch operation
	IsErrorStop bool
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
	// setting.DBName = config.MysqlName
	setting.NextDBID = 0
	setting.DBMaxNum = len(setting.SqlConfigs)
	setting.LinkNum = 0
	setting.MaxLink = config.MaxLink.MySQL

	setting.RedisConfigs = config.Redis
	setting.NextRedisDBID = 0
	setting.RedisMaxNum = len(setting.RedisConfigs)
	setting.RedisLinkNum = 0
	setting.RedisMaxLink = config.MaxLink.Redis

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

	redisDBs := []*RedisDB{}
	redisConnectFailTime := []*time.Time{}
	for i := 0; i < setting.RedisMaxLink; i++ {
		redisDBs = append(redisDBs, nil)
	}
	for i := 0; i < len(setting.RedisConfigs); i++ {
		redisConnectFailTime = append(redisConnectFailTime, nil)
	}
	setting.RedisDB = redisDBs
	setting.RedisConnectFailTime = redisConnectFailTime

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

// ===============
//
//	连接Redis数据库
//	item		int		"需要连接的数据库在配置中的位置"
//	dbID		int		"需要连接的数据库ID"
//	return 1	*RedisDB	"连接池中的位置"
//	return 2	error		"错误信息"
//
// ===============
//
//	Connect to Redis database
//	item		int		"Location of the database to be
//						 		connected in the configuration"
//	dbID		int		"Database ID to be connected"
//	return 1	*RedisDB	"Location in the connection pool"
//	return 2	error		"Error message"
func (s *Setting) RedisLink(item int, dbID int) (*RedisDB, error) {
	if s.RedisConfigs == nil || len(s.RedisConfigs) == 0 {
		return nil, fmt.Errorf("redis Open Error: %s", "配置为空")
	}
	var (
		redisJson *RedisConfig
		err       error
	)
	redisJson = &s.RedisConfigs[item]
	if !(0 <= dbID && dbID <= redisJson.MaxDB) {
		return nil, fmt.Errorf("redis Open Error: %s", "数据库ID超出范围")
	}
	redisdb := redis.NewClient(&redis.Options{
		Addr:     redisJson.Addr + ":" + redisJson.Port,
		Password: redisJson.Password,
		DB:       dbID,
	})
	_, err = redisdb.Ping(redisdb.Context()).Result()
	if err != nil {
		return nil, err
	}
	return &RedisDB{Addr: redisJson.Addr, DBItem: item, DB: redisdb}, nil
}

// ===============
//
//	关闭Redis数据库
//
// ===============
//
//	Close Redis database
func (db *RedisDB) Close() {
	if db.DB != nil {
		db.DB.Close()
		db.DB = nil
		db.Addr = ""
		db.DBItem = -1
	}
}
