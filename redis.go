package weSubDatabase

import (
	"context"
	"fmt"
	"time"
)

// <類>
var ctx = context.Background()

// 连接MySQL时的可选配置
//
// Optional configuration when connecting to MySQL
type RedisO func(*Option)

// ===============
//
//	设置等待次数
//	WaitCount	int	"等待次数"
//
// ===============
//
//	Set the number of waits
//	WaitCount	int	"Number of waits"
func OLRedisWaitCount(WaitCount int) RedisO {
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
func OLRedisWaitTime(WaitTime int) RedisO {
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
func OLRedisIsShowPrint(IsShowPrint bool) LinkSQLO {
	return func(o *Option) {
		o.IsShowPrint = IsShowPrint
	}
}

// ===============
//
//	在查詢完成後刪除此條目
//	IsDelete	bool	"是否删除"
//
// ===============
//
//	Delete this entry after the query is complete
//	IsDelete	bool	"Whether to delete"
func OLRedisIsDelete(IsDelete bool) LinkSQLO {
	return func(o *Option) {
		o.IsDelete = IsDelete
	}
}

// ===============
//
//	自動刪除時間，單位秒
//	AutoDeleteTime	int	"自动删除时间，单位秒"
//
// ===============
//
//	Auto delete time, in seconds
//	AutoDeleteTime	int	"Auto delete time, in seconds"
func OLRedisAutoDeleteTime(AutoDeleteTime int) LinkSQLO {
	return func(o *Option) {
		o.AutoDeleteTime = AutoDeleteTime
	}
}

// ===============
//
//	是否在出錯時停止
//	IsErrorStop	bool	"是否在出錯時停止"
//
// ===============
//
//	Whether to stop when an error occurs
//	IsErrorStop	bool	"Whether to stop when an error occurs"
func OLRedisIsErrorStop(IsErrorStop bool) LinkSQLO {
	return func(o *Option) {
		o.IsErrorStop = IsErrorStop
	}
}

// ===============
//
//	连接Redis数据库并放入连接池
//	item	int	"数据库ID"
//	dbID	int	"数据库在配置中的位置"
//	options	...RedisO	"可选配置"
//		WaitCount	int	"等待次数"
//		WaitTime	int	"每次等待时间，单位毫秒"
//		IsShowPrint	bool	"是否输出到控制台"
//	return 1	int	"连接池中的位置"
//	return 2	error	"错误信息"
//
// ===============
//
//	Connect to the Redis database and put it in the connection pool
//	item	int	"Database ID"
//	dbID	int	"Location of the database in the configuration"
//	options	...RedisO	"Optional configuration"
//		WaitCount	int	"Number of waits"
//		WaitTime	int	"Waiting time per time, in milliseconds"
//		IsShowPrint	bool	"Whether to output to the console"
//	return 1	int	"Position in the connection pool"
//	return 2	error	"Error message"
func (s *Setting) RedisIsRun(item int, dbID int, options ...RedisO) (int, error) {
	option := &Option{
		WaitCount:   10,
		WaitTime:    500,
		IsShowPrint: false,
	}
	for _, o := range options {
		o(option)
	}
	if s.RedisLinkNum >= s.RedisMaxLink {
		WaitCount := 0
		for {
			if s.RedisLinkNum < s.RedisMaxLink {
				break
			}
			if WaitCount > option.WaitCount {
				return -1, fmt.Errorf("MySQL connections are full")
			}
			WaitCount += 1
			time.Sleep(time.Duration(option.WaitTime) * time.Millisecond)
		}
	}
	wRedisDB, err := s.RedisLink(item, dbID)
	if err != nil {
		tn := time.Now()
		s.RedisConnectFailTime[item] = &tn
		return -1, err
	}
	ii := 0
	for i := 0; i < len(s.RedisDB); i++ {
		if s.RedisDB[i] == nil {
			ii = i
			break
		}
	}
	s.RedisLinkNum += 1
	s.RedisDB[ii] = wRedisDB
	if option.IsShowPrint {
		println("Redis DB", item, "connection successful!")
	}
	return ii, nil
}

// ===============
//
//	关闭Redis连接
//	i	int	"连接池中的位置"
//
// ===============
//
//	Close Redis connection
//	i	int	"Position in the connection pool"
func (s *Setting) RedisClose(i int, options ...IsShowPrintO) {
	if i < 0 || i >= len(s.RedisDB) {
		return
	}
	if s.RedisDB[i] != nil {
		s.RedisDB[i].Close()
		s.RedisDB[i] = nil
		s.RedisLinkNum -= 1
		if s.RedisLinkNum < 0 {
			s.RedisLinkNum = 0
		}
		option := &Option{
			IsShowPrint: false,
		}
		for _, o := range options {
			o(option)
		}
		if option.IsShowPrint {
			println("MySQL Close Connection! Current number of connections:", s.RedisLinkNum)
		}
	}
}

// ===============
//
//	向Redis数据库中插入数据
//	key			string		"键"
//	val			string		"值"
//	options			...RedisO	"可选配置"
//		AutoDeleteTime	int		"自动删除时间，单位秒"
//	return			error		"错误信息"
//
// ===============
//
//	Insert data into Redis database
//	key			string		"Key"
//	val			string		"Value"
//	options			...RedisO	"Optional configuration"
//		AutoDeleteTime	int		"Auto delete time, in
//											seconds"
//	return			error		"Error message"
func (rDB *RedisDB) SetValue(key string, val string, options ...RedisO) error {
	option := &Option{AutoDeleteTime: 0}
	for _, o := range options {
		o(option)
	}
	var err error
	if option.AutoDeleteTime == 0 {
		err = rDB.DB.Set(ctx, key, val, 0).Err()
	} else {
		err = rDB.DB.Set(ctx, key, val, time.Duration(option.AutoDeleteTime)*time.Second).Err()
	}
	return err
}

// ===============
//
//	从Redis数据库中获取数据
//	key		string		"键"
//	options		...RedisO	"可选配置"
//		IsDelete	bool		"是否删除,默认值为false"
//	return 1	string		"值"
//	return 2	error		"错误信息"
//
// ===============
//
//	Get data from Redis database
//	key		string		"Key"
//	options		...RedisO	"Optional configuration"
//		IsDelete	bool		"Whether to delete, the default
//									value is false"
//	return 1	string		"Value"
//	return 2	error		"Error message"
func (rDB *RedisDB) GetString(key string, options ...RedisO) (string, error) {
	option := &Option{IsDelete: false}
	for _, o := range options {
		o(option)
	}
	val, err := rDB.DB.Get(ctx, key).Result()
	if err != nil {
		return "", err
	}
	if option.IsDelete {
		err = rDB.DB.Del(ctx, key).Err()
		if err != nil {
			return "", err
		}
	}
	return val, nil
}

// ===============
//
//	从Redis数据库中批次获取数据
//	keyPattern	string			"键(支持通配符,如:*)"
//	options		...RedisO		"可选配置"
//		IsDelete	bool			"是否删除,默认值为false"
//		IsErrorStop	bool			"是否在出錯時停止,
//											默认值为false"
//	return 1	map[string]string	"值"
//	return 2	error			"错误信息"
//
// ===============
//
//	Get data in batches from Redis database
//	keyPattern	string			"Key (supports
//											wildcards, such as: *)"
//	options		...RedisO		"Optional configuration"
//		IsDelete	bool			"Whether to delete,the
//											default value is false"
//		IsErrorStop	bool			"Whether to stop when an
//											error occurs,the default
//											value is false"
//	return 1	map[string]string	"Value"
//	return 2	error			"Error message"
func (rDB *RedisDB) GetStringAll(keyPattern string, options ...RedisO) (map[string]string, error) {
	option := &Option{
		IsDelete:    false,
		IsErrorStop: false,
	}
	for _, o := range options {
		o(option)
	}
	var (
		data map[string]string = make(map[string]string)
		err  error             = nil
	)
	iter := rDB.DB.Scan(ctx, 0, keyPattern, 0).Iterator()
	for iter.Next(ctx) {
		var (
			key string
			val string
		)
		key = iter.Val()
		val, err = rDB.GetString(key, options...)
		if err != nil {
			if option.IsErrorStop {
				return nil, err
			}
		} else {
			data[key] = val
		}
	}
	err = iter.Err()
	return data, err
}

// ===============
//
//	从Redis数据库中获取全部键值
//	keyPattern	string		"键(支持通配符,如:*)"
//	return 1	[]string	"键"
//	return 2	error		"错误信息"
//
// ===============
//
//	Get all key values from Redis database
//	keyPattern	string		"Key (supports wildcards,
//									such as: *)"
//	return 1	[]string	"Key"
//	return 2	error		"Error message"
func (rDB *RedisDB) Keys(keyPattern string) ([]string, error) {
	keys, err := rDB.DB.Keys(ctx, keyPattern).Result()
	if err == nil {
		//排序
		for i := 0; i < len(keys); i++ {
			for j := i + 1; j < len(keys); j++ {
				if keys[i] > keys[j] {
					keys[i], keys[j] = keys[j], keys[i]
				}
			}
		}
	}

	return keys, err
}

// ===============
//
//	从Redis数据库中删除数据
//	keys		[]string	"键"
//	options		...RedisO	"可选配置"
//		IsErrorStop	bool		"是否在出錯時停止,默认值为true"
//	return		error		"错误信息"
//
// ===============
//
//	Delete data from Redis database
//	keys		[]string	"Key"
//	options		...RedisO	"Optional configuration"
//		IsErrorStop	bool		"Whether to stop when an
//									error occurs, the default
//									value is true"
//	return		error		"Error message"
func (rDB *RedisDB) Del(keys []string, options ...RedisO) error {
	option := &Option{
		IsErrorStop: true,
	}
	for _, o := range options {
		o(option)
	}
	var err error
	for _, k := range keys {
		err = rDB.DB.Del(ctx, k).Err()
		if err != nil && option.IsErrorStop {
			return err
		}
	}
	return err
}

// ===============
//
//	从Redis数据库中批次删除数据
//	keyPattern	string		"键(支持通配符,如:*)"
//	options		...RedisO	"可选配置"
//		IsErrorStop	bool		"是否在出錯時停止,默认值为true"
//	return		error		"错误信息"
//
// ===============
//
//	Delete data in batches from Redis database
//	keyPattern	string		"Key (supports wildcards,
//									such as: *)"
//	options		...RedisO	"Optional configuration"
//		IsErrorStop	bool		"Whether to stop when an
//									error occurs, the default
//									value is true"
//	return		error		"Error message"
func (rDB *RedisDB) DelMulti(keyPattern string, options ...RedisO) error {
	option := &Option{
		IsErrorStop: true,
	}
	for _, o := range options {
		o(option)
	}
	var err error
	iter := rDB.DB.Scan(ctx, 0, keyPattern, 0).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		err = rDB.DB.Del(ctx, key).Err()
		if err != nil && option.IsErrorStop {
			return err
		}
	}
	err = iter.Err()
	return err
}
