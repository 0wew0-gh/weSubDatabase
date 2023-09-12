package weSubDatabase

import (
	"database/sql"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ===============
//
//	自动根据 *Setting 从数据库集中，根据加密后的主键查询
//	table		string			"表名"
//	from		string			"查询的字段"
//	primaryKey	string			"主键"
//	ids		[]string		"加密后的主键"
//	order		string			"排序"
//	Debug		*log.Logger		"调试输出"
//	options		[]IsShowPrintO		"配置"
//		IsShowPrint	bool			"是否输出到控制台"
//	return 1	[]map[string]string	"查询到的数据"
//	return 2	[]error			"错误信息"
//
// ===============
//
//	Automatically query from the database set according to *Setting, according to the encrypted primary key
//	table		string			"table name"
//	from		string			"query field"
//	primaryKey	string			"primary key"
//	ids		[]string		"encrypted primary key"
//	order		string			"sort"
//	Debug		*log.Logger		"debug output"
//	options		[]IsShowPrintO		"Configuration"
//		IsShowPrint	bool			"Whether to output to the
//											console"
//	return 1	[]map[string]string	"query data"
//	return 2	[]error			"error message"
func (s *Setting) QueryID(table string, from string, primaryKey string, ids []string, order string, Debug *log.Logger, options ...IsShowPrintO) ([]map[string]string, []error) {
	option := &Option{
		IsShowPrint: false,
	}
	for _, o := range options {
		o(option)
	}
	dbIList, idList, _ := s.DecryptID(primaryKey, ids)
	var (
		queryDatas []map[string]string
		errs       []error
	)
	for i := 0; i < len(s.SqlConfigs); i++ {
		errs = append(errs, nil)
	}
	var wg sync.WaitGroup
	for i := 0; i < len(s.SqlConfigs); i++ {
		if !dbIList[i] {
			continue
		}
		if !s.IsRetryConnect(i) {
			continue
		}
		wg.Add(1)
		sqlStr := "SELECT "
		if from == "" {
			sqlStr += "*"
		} else {
			sqlStr += from
		}
		sqlStr += " FROM `" + table + "` WHERE `" + primaryKey + "` IN ('"
		whereIN := strings.Join(idList[i], "','")
		if errStr := CheckString(whereIN); len(errStr) > 0 {
			errs[i] = fmt.Errorf("SQL injection: %s", whereIN)
			wg.Done()
			continue
		}
		sqlStr += whereIN + "')"
		if order != "" {
			sqlStr += " ORDER BY " + order
		}
		chanQD := make(chan []map[string]string)
		chanErr := make(chan error)
		go s.go_query(i, sqlStr, chanQD, chanErr, option.IsShowPrint, Debug)
		rI := false
		rE := false
		for {
			select {
			case reqd := <-chanQD:
				if !rI {
					for _, v := range reqd {
						v["db"] = strconv.Itoa(i)
						queryDatas = append(queryDatas, v)
					}
					close(chanQD)
					rI = true
				}
			case reErr := <-chanErr:
				if !rE {
					errs = append(errs, reErr)
					close(chanErr)
					rE = true
				}
			}
			if rI && rE {
				break
			}
			time.Sleep(time.Millisecond * 50)
		}
		wg.Done()
	}
	wg.Wait()
	orderKey := "id"
	orderSort := "ASC"
	if order != "" {
		os := strings.Split(order, " ")
		if len(os) != 2 {
			return nil, []error{fmt.Errorf("order error")}
		}
		orderKey = strings.ReplaceAll(os[0], "`", "")
		orderSort = os[1]
	}
	sort.Slice(queryDatas, func(i, j int) bool {
		switch orderKey {
		case "id":
			idA, err := strconv.Atoi(queryDatas[i]["id"])
			if err != nil {
				return false
			}
			idB, err := strconv.Atoi(queryDatas[j]["id"])
			if err != nil {
				return false
			}
			if orderSort == "ASC" {
				return idA < idB
			} else {
				return idA > idB
			}
		case "time":
			timeFormat := "2006-01-02 15:04:05"
			tmA, err := time.Parse(timeFormat, queryDatas[i]["time"])
			if err != nil {
				return false
			}
			tmB, err := time.Parse(timeFormat, queryDatas[j]["time"])
			if err != nil {
				return false
			}
			if orderSort == "ASC" {
				return tmA.Before(tmB)
			} else {
				return tmA.After(tmB)
			}
		default:
			return false
		}
	})
	queryDatas = s.EncryptPrimaryKey(queryDatas, primaryKey)
	for _, v := range errs {
		if v != nil {
			return queryDatas, errs
		}
	}
	return queryDatas, nil
}

// ===============
//
//	根据 *Setting 从数据库集中查询
//	table		string			"表名"
//	from		string			"查询字段
//											空字符串为默认值 *"
//	primaryKey	string			"主键
//											空字符串时不加密"
//	where		string			"查询条件"
//	order		string			"排序"
//	limit		string			"分页
//											空字符串为默认值:100"
//	Debug		*log.Logger		"调试日志对象"
//	options		[]IsShowPrintO		"配置"
//		IsShowPrint	bool			"是否输出到控制台"
//	return 1	[]map[string]string	"查询结果"
//	return 2	[]error			"错误信息"
//
// ===============
//
//	According to *Setting, query from the database set
//	table		string			"Table name"
//	from		string			"Query field
//											Empty string is the default
//											value *"
//	primaryKey	string			"Primary key
//											Empty string is not
//											encrypted"
//	where		string			"Query condition"
//	order		string			"Sorting"
//	limit		string			"Paging
//											Empty string is the default
//											value: 100"
//	Debug		*log.Logger		"Debug log object"
//	options		[]IsShowPrintO		"Configuration"
//		IsShowPrint	bool			"Whether to output to the
//											console"
//	return 1	[]map[string]string	"Query result"
//	return 2	[]error			"Error message"
func (s *Setting) Query(table string, from string, primaryKey string, where string, order string, limit string, Debug *log.Logger, options ...IsShowPrintO) ([]map[string]string, []error) {
	option := &Option{
		IsShowPrint: false,
	}
	for _, o := range options {
		o(option)
	}
	sqlStr := "SELECT "
	if from != "" {
		sqlStr += from + " FROM "
	} else {
		sqlStr += "* FROM "
	}
	sqlStr += "`" + table + "`"
	if where != "" {
		sqlStr += " WHERE " + where
	}
	orderKey := "id"
	orderSort := "ASC"
	if order != "" {
		sqlStr += " ORDER BY " + order
		os := strings.Split(order, " ")
		if len(os) != 2 {
			return nil, []error{fmt.Errorf("order error")}
		}
		orderKey = strings.ReplaceAll(os[0], "`", "")
		orderSort = os[1]
	}

	var isContinues []bool
	for i := 0; i < len(s.ConnectFailTime); i++ {
		isContinues = append(isContinues, s.IsRetryConnect(i))
	}

	if limit == "" {
		limit = "100"
	}

	limitList := []string{}
	limit = strings.ReplaceAll(limit, " ", "")
	ls := strings.Split(limit, ",")
	if len(ls) >= 2 {
		i, err := strconv.Atoi(ls[1])
		if err == nil {
			limitList = toLimit(s.NextDBID, ls[0], i, isContinues)
		}
	} else {
		i, err := strconv.Atoi(limit)
		if err == nil {
			limitList = toLimit(s.NextDBID, "", i, isContinues)
		}
	}

	var (
		queryDatas []map[string]string
		errs       []error
	)
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	for i := 0; i < len(s.SqlConfigs); i++ {
		if !s.IsRetryConnect(i) {
			continue
		}
		if !isContinues[i] {
			continue
		}
		wg.Add(1)
		chanQD := make(chan []map[string]string)
		chanErr := make(chan error)
		go s.go_query(i, sqlStr+" LIMIT "+limitList[i], chanQD, chanErr, option.IsShowPrint, Debug)
		rI := false
		rE := false
		for {
			select {
			case reqd := <-chanQD:
				if !rI {
					for _, v := range reqd {
						v["db"] = strconv.Itoa(i)
						queryDatas = append(queryDatas, v)
					}
					close(chanQD)
					rI = true
				}
			case reErr := <-chanErr:
				if !rE {
					errs = append(errs, reErr)
					close(chanErr)
					rE = true
				}
			}
			if rI && rE {
				break
			}
			time.Sleep(time.Millisecond * 50)
		}
		wg.Done()
	}
	wg.Wait()
	sort.Slice(queryDatas, func(i, j int) bool {
		switch orderKey {
		case "id":
			idA, err := strconv.Atoi(queryDatas[i]["id"])
			if err != nil {
				return false
			}
			idB, err := strconv.Atoi(queryDatas[j]["id"])
			if err != nil {
				return false
			}
			if orderSort == "ASC" {
				return idA < idB
			} else {
				return idA > idB
			}
		case "time":
			timeFormat := "2006-01-02 15:04:05"
			tmA, err := time.Parse(timeFormat, queryDatas[i]["time"])
			if err != nil {
				return false
			}
			tmB, err := time.Parse(timeFormat, queryDatas[j]["time"])
			if err != nil {
				return false
			}
			if orderSort == "ASC" {
				return tmA.Before(tmB)
			} else {
				return tmA.After(tmB)
			}
		default:
			return false
		}
	})
	queryDatas = s.EncryptPrimaryKey(queryDatas, primaryKey)
	for _, v := range errs {
		if v != nil {
			return queryDatas, errs
		}
	}
	return queryDatas, nil
}

// ===============
//
//	根据 *Setting 从数据库集中查询指定表的最后一条数据的 ID
//	table		string		"表名"
//	primaryKey	string		"主键名,主键类型必须为数字类型"
//	Debug		*log.Logger	"Debug 日志对象"
//	options		[]IsShowPrintO	"配置"
//		IsShowPrint	bool		"是否输出到控制台"
//	return 1	int		"下一条数据所在的数据库索引"
//	return 2	int		"下一条数据的 ID"
//	return 3	error		"错误信息"
//
// ===============
//
//	According to *Setting, query the ID of the last data of the specified table from the database set
//	table		string		"table name"
//	primaryKey	string		"primary key name,The primary key
//									type must be a numeric type"
//	Debug		*log.Logger	"Debug log object"
//	options		[]IsShowPrintO	"Configuration"
//		IsShowPrint	bool		"Whether to output to the
//									console"
//	return 1	int		"The database index where the next
//									data is located"
//	return 2	int		"ID of the next data"
//	return 3	error		"Error message"
func (s *Setting) SelectLastID(table string, primaryKey string, Debug *log.Logger, options ...IsShowPrintO) (int, int, error) {
	option := &Option{
		IsShowPrint: false,
	}
	for _, o := range options {
		o(option)
	}
	sqlStr := "SELECT MAX(`" + primaryKey + "`) FROM `" + table + "`"
	var (
		queryDatas []map[string]string
		errs       []error
	)
	for i := 0; i < len(s.SqlConfigs); i++ {
		errs = append(errs, nil)
	}
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	for i := 0; i < len(s.SqlConfigs); i++ {
		if !s.IsRetryConnect(i) {
			continue
		}
		wg.Add(1)
		chanQD := make(chan []map[string]string)
		chanErr := make(chan error)
		go s.go_query(i, sqlStr, chanQD, chanErr, option.IsShowPrint, Debug)
		rI := false
		rE := false
		for {
			select {
			case reqd := <-chanQD:
				if !rI {
					for _, v := range reqd {
						v["db"] = strconv.Itoa(i)
						queryDatas = append(queryDatas, v)
					}
					close(chanQD)
					rI = true
				}
			case reErr := <-chanErr:
				if !rE {
					errs = append(errs, reErr)
					close(chanErr)
					rE = true
				}
			}
			if rI && rE {
				break
			}
			time.Sleep(time.Millisecond * 50)
		}
		wg.Done()
	}
	wg.Wait()
	var maxID int
	var dbI int
	for _, v := range queryDatas {
		if v["MAX(`"+primaryKey+"`)"] == "" {
			continue
		}
		id, err := strconv.Atoi(v["MAX(`"+primaryKey+"`)"])
		if err != nil {
			return -1, 1, err
		}
		if id > maxID {
			maxID = id
			db, err := strconv.Atoi(v["db"])
			if err != nil {
				return -1, 1, err
			}
			dbI = db + 1
		}
	}
	dbI += 1
	if dbI > len(s.SqlConfigs) {
		dbI = 1
	}
	maxID += 1
	// 防止连接不上的数据库中存在最大值
	for i := 0; i < len(s.ConnectFailTime); i++ {
		if s.ConnectFailTime[i] != nil {
			maxID += 1
		}
	}
	for _, v := range errs {
		if v != nil {
			return dbI, maxID, v
		}
	}
	return dbI, maxID, nil
}

// ===============
//
//	处理查询结果
//	query		*sql.Rows		"查询结果"
//	Debug		*log.Logger		"Debug 日志对象"
//	return 1	[]map[string]string	"查询结果"
//	return 2	error			"错误信息"
//
// ===============
//
//	handle query data
//	query		*sql.Rows		"query result"
//	Debug		*log.Logger		"Debug log object"
//	return 1	[]map[string]string	"query result"
//	return 2	error			"error message"
func handleQD(query *sql.Rows, Debug *log.Logger) ([]map[string]string, error) {
	//读出查询出的列字段名
	cols, _ := query.Columns()
	//values是每个列的值，这里获取到byte里
	values := make([][]byte, len(cols))
	//query.Scan的参数，因为每次查询出来的列是不定长的，用len(cols)定住当次查询的长度
	scans := make([]interface{}, len(cols))
	//让每一行数据都填充到[][]byte里面
	for i := range values {
		scans[i] = &values[i]
	}

	//最后得到的map
	results := []map[string]string{}
	i := 0
	for query.Next() { //循环，让游标往下推
		if err := query.Scan(scans...); err != nil { //query.Scan查询出来的不定长值放到scans[i] = &values[i],也就是每行都放在values里
			if Debug != nil {
				Debug.Println(err)
			}
			return []map[string]string{}, err
		}
		row := map[string]string{} //每行数据
		for k, v := range values { //每行数据是放在values里面，现在把它挪到row里
			key := cols[k]
			row[key] = string(v)
		}
		i++
		results = append(results, row)
	}

	//关闭结果集（释放连接）
	query.Close()

	return results, nil
}
