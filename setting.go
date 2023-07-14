package weSubDatabase

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/0wew0-gh/simpleEncryption"
	_ "github.com/go-sql-driver/mysql"
)

type Setting struct {
	SqlConfigs       []SQLConfig                 //数据库配置
	DBName           string                      //数据库名
	NextAddDBID      int                         //下一个数据库ID号
	DBMaxNum         int                         //数据库最大数目
	LinkNum          int                         //连接数目
	MaxLink          int                         //最大连接数目
	MySQLDB          []*MysqlDB                  //数据库连接
	SEKey            *simpleEncryption.SecretKey //加密对象
	ConnectAgainTime int                         //重新连接数据库的时间间隔
	ConnectFailTime  []*time.Time                //上次连接失败的时间, 用于判断是否需要重新连接
}

type MysqlDB struct {
	Name   string  //数据库名
	DBItem int     //当前连接的数据库在配置中的位置
	DB     *sql.DB //数据库连接
}

type NewOptionConfig func(*Option)

func OptionRetryTime(RetryTime int) NewOptionConfig {
	return func(o *Option) {
		o.RetryTime = RetryTime
	}
}

// ===============
//
//	数据库配置
//	configString	string			配置文件字符串
//	options		[]NewOptionConfig	配置
//		RetryTime	int			重试时间(ms)
//
//	返回值1		*Setting		数据库配置对象
//	返回值2		error			错误信息
//
// ===============
//
//	Database config
//	configString	string			Configuration file string
//	options		[]NewOptionConfig	Configuration
//		RetryTime	int			Retry time(ms)
//
//	Return 1	*Setting		Database configuration object
//	Return 2	error			Error message
func New(configString string, options ...NewOptionConfig) (*Setting, error) {
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
	setting.NextAddDBID = 1
	setting.DBMaxNum = len(setting.SqlConfigs)
	setting.LinkNum = 0
	setting.MaxLink = config.MaxLink.MySQL
	mySQLDBs := []*MysqlDB{}
	connectFailTime := []*time.Time{}
	for i := 0; i < setting.MaxLink; i++ {
		mySQLDBs = append(mySQLDBs, nil)
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
//	item		int		需要连接的数据库在配置中的位置
//	返回值1		*MysqlDB	连接池中的位置
//	返回值2		error		错误信息
//
// ===============
//
//	Connect to MySQL database
//	item		int		Location of the database to be
//						 		connected in the configuration
//	Return 1	*MysqlDB	Location in the connection pool
//	Return 2	error		Error message
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
//	加密主键, *Setting.SEKey 为 nil 时不加密
//	queryDatas	[]map[string]string	查询结果
//	primaryKey	string			主键字段名
//											为""时不加密
//
//	返回值		[]map[string]string	查询结果
//
// ===============
//
//	Encrypt primary key, *Setting.SEKey is not encrypted when nil
//	queryDatas	[]map[string]string	query result
//	primaryKey	string			primary key field name
//											No encryption when ""
//
//	return		[]map[string]string	query result
func (s *Setting) EncryptPrimaryKey(queryDatas []map[string]string, primaryKey string) []map[string]string {
	if s.SEKey == nil {
		return queryDatas
	}
	if primaryKey == "" {
		return queryDatas
	}
	for i := 0; i < len(queryDatas); i++ {
		idStr := queryDatas[i][primaryKey]
		if idStr == "" {
			continue
		}
		dbI := queryDatas[i]["db"]
		if dbI == "" {
			continue
		}
		queryDatas[i][primaryKey] = s.SEKey.Encrypt(idStr, dbI)
		delete(queryDatas[i], "db")
	}
	return queryDatas
}

// ===============
//
//	根据主键和id数组解密出数据库ID和主键ID
//	primaryKey	string			主键字段名
//	ids		[]string		主键ID数组
//
//	返回值1		[]string		数据库ID数组
//	返回值2		[]string		主键ID数组
//	返回值3		[]int			主键ID原数组中的位置
//
// ===============
//
//	Decrypt the database ID and primary key ID based on the primary key and ID array
//	primaryKey	string			primary key field name
//	ids		[]string		primary key ID array
//
//	return 1	[]string		database ID array
//	return 2	[]string		primary key ID array
//	return 3	[]int			Position in the original array of primary key IDs
func (s *Setting) DecryptID(primaryKey string, ids []string) ([]bool, [][]string, [][]int) {
	var (
		dbIList []bool     = []bool{}
		idList  [][]string = [][]string{}
		pwList  [][]int    = [][]int{}
	)
	for i := 0; i < len(s.SqlConfigs); i++ {
		dbIList = append(dbIList, false)
		idList = append(idList, []string{})
		pwList = append(pwList, []int{})
	}
	for i, v := range ids {
		id, dbI, err := s.SEKey.Decrypt(v)
		if err != nil {
			continue
		}
		dbIint, err := strconv.Atoi(dbI)
		if err != nil {
			continue
		}
		dbIList[dbIint] = true
		idList[dbIint] = append(idList[dbIint], id)
		pwList[dbIint] = append(pwList[dbIint], i)
	}
	return dbIList, idList, pwList
}

// ===============
//
//	判断是否可以连接数据库
//	item		int		需要连接的数据库在配置中的位置
//
//	返回值1		bool		是否可以尝试连接
//
// ===============
//
//	Determine whether the database can be connected
//	item		int		Location of the database to be
//						 		connected in the configuration
//
//	return 1	bool		Whether to try to connect
func (s *Setting) IsRetryConnect(item int) bool {
	if s.ConnectFailTime[item] != nil {
		tn := time.Now()
		tend := s.ConnectFailTime[item].Add(time.Millisecond * time.Duration(s.ConnectAgainTime))
		if tn.After(tend) {
			s.ConnectFailTime[item] = nil
			return true
		} else {
			return false
		}
	}
	return true
}
