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
//	table		string			表名
//	from		string			查询的字段
//	primaryKey	string			主键
//	ids		[]string		加密后的主键
//	order		string			排序
//	Debug		*log.Logger		调试输出
//
//	返回值1		[]map[string]string	查询到的数据
//	返回值2		[]error			错误信息
//
// ===============
//
//	Automatically query from the database set according to *Setting, according to the encrypted primary key
//	table		string			table name
//	from		string			query field
//	primaryKey	string			primary key
//	ids		[]string		encrypted primary key
//	order		string			sort
//	Debug		*log.Logger		debug output
//
//	return 1	[]map[string]string	query data
//	return 2	[]error			error message
func (s *Setting) QueryID(table string, from string, primaryKey string, ids []string, order string, Debug *log.Logger) ([]map[string]string, []error) {
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
		wg.Add(1)
		sqlStr := "SELECT "
		if from == "" {
			sqlStr += "*"
		} else {
			sqlStr += from
		}
		sqlStr += " FROM `" + table + "` WHERE `" + primaryKey + "` IN ('"
		sqlStr += strings.Join(idList[i], "','") + "')"
		if order != "" {
			sqlStr += " ORDER BY " + order
		}
		chanQD := make(chan []map[string]string)
		chanErr := make(chan error)
		go s.go_query(i, sqlStr, chanQD, chanErr, Debug)
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
	for _, v := range errs {
		if v != nil {
			return nil, errs
		}
	}
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
	return queryDatas, nil
}

// ===============
//
//	根据 *Setting 从数据库集中查询
//	table		string			表名
//	from		string			查询字段
//											""为默认值*
//	primaryKey	string			主键
//											为""时不加密
//	where		string			查询条件
//	order		string			排序
//	limit		string			分页
//											""为默认值100
//	Debug		*log.Logger		调试日志对象
//
//	返回值1		[]map[string]string	查询结果
//	返回值2		[]error			错误信息
//
// ===============
//
//	According to *Setting, query from the database set
//	table		string			Table name
//	from		string			Query field
//											"" is the default value *
//	primaryKey	string			Primary key
//											"" does not encrypt
//	where		string			Query condition
//	order		string			Sorting
//	limit		string			Paging
//											"" is the default value 100
//	Debug		*log.Logger		Debug log object
//
//	Return 1	[]map[string]string	Query result
//	Return 2	[]error			Error message
func (s *Setting) Query(table string, from string, primaryKey string, where string, order string, limit string, Debug *log.Logger) ([]map[string]string, []error) {
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
	if limit != "" {
		sqlStr += " LIMIT " + limit
	} else {
		sqlStr += " LIMIT 100"
	}
	var (
		queryDatas []map[string]string
		errs       []error
	)
	for i := 0; i < len(s.SqlConfigs); i++ {
		errs = append(errs, nil)
	}
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	for i := 0; i < len(s.SqlConfigs); i++ {
		wg.Add(1)
		chanQD := make(chan []map[string]string)
		chanErr := make(chan error)
		go s.go_query(i, sqlStr, chanQD, chanErr, Debug)
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
	for _, v := range errs {
		if v != nil {
			return nil, errs
		}
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
	return queryDatas, nil
}

// ===============
//
//	根据 *Setting 从数据库集中查询指定表的最后一条数据的 ID
//	table		string		表名
//	primaryKey	string		主键名,主键类型必须为数字类型
//	Debug		*log.Logger	Debug 日志对象
//
//	返回值1		int		下一条数据所在的数据库索引
//	返回值2		int		下一条数据的 ID
//	返回值3		error		错误信息
//
// ===============
//
//	According to *Setting, query the ID of the last data of the specified table from the database set
//	table		string		table name
//	primaryKey	string		primary key name,The primary key
//									type must be a numeric type
//	Debug		*log.Logger	Debug log object
//
//	return 1	int		The database index where the next data is located
//	return 2	int		ID of the next data
//	return 3	error		Error message
func (s *Setting) SelectLastID(table string, primaryKey string, Debug *log.Logger) (int, int, error) {
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
		wg.Add(1)
		chanQD := make(chan []map[string]string)
		chanErr := make(chan error)
		go s.go_query(i, sqlStr, chanQD, chanErr, Debug)
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
	for _, v := range errs {
		if v != nil {
			return -1, 1, v
		}
	}
	var maxID int
	var dbI int
	for i, v := range queryDatas {
		if v["MAX(`"+primaryKey+"`)"] == "" {
			continue
		}
		id, err := strconv.Atoi(v["MAX(`"+primaryKey+"`)"])
		if err != nil {
			return -1, 1, err
		}
		if id > maxID {
			maxID = id + 1
			dbI = i + 1
			if dbI >= len(s.SqlConfigs) {
				dbI = 0
			}
			dbI += 1
		}
	}
	if dbI == 0 {
		dbI = 1
	}
	return dbI, maxID, nil
}

// ===============
//
//	处理查询结果
//	query		*sql.Rows		查询结果
//	Debug		*log.Logger		Debug 日志对象
//
//	返回值1		[]map[string]string		查询结果
//	返回值2		error				错误信息
//
// ===============
//
//	handle query data
//	query		*sql.Rows		query result
//	Debug		*log.Logger	Debug log object
//
//	return 1	[]map[string]string		query result
//	return 2	error				error message
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
