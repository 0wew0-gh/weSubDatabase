package weSubDatabase

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"
)

// ===============
//
//	根据encryptedKey计算Value值的数据库下标，并根据下标向表中插入
//	table		string		"表名"
//	encryptedKey	[]string	"根据此项计算value应该插入哪个库中"
//	keys		[]string	"键名"
//	values		[][]string	"值"
//	Debug		*log.Logger	"调试输出"
//	options		[]IsShowPrintO	"配置"
//		IsShowPrint	bool		"是否输出到控制台"
//	return 1	[]int64		"插入的行数"
//	return 2	[]error		"错误信息"
//
// ===============
//
//	Calculate the database subscript of the Value value based on the encryptedKey, and insert it into the table based on the subscript.
//	table		string		"table name"
//	encryptedKey	[]string	"Calculate which library the value should be inserted into based on this item"
//	keys		[]string	"key name"
//	values		[][]string	"value"
//	Debug		*log.Logger	"debug output"
//	options		[]IsShowPrintO	"Configuration"
//		IsShowPrint	bool		"Whether to output to the console"
//	return 1	[]int64		"Number of rows inserted"
//	return 2	[]error		"Error message"
func (s *Setting) AddForPrimary(table string, encryptedKey []string, keys []string, value [][]string, Debug *log.Logger, options ...IsShowPrintO) ([]int64, []error) {
	option := &Option{
		IsShowPrint: false,
	}
	for _, o := range options {
		o(option)
	}
	if len(encryptedKey) != len(value) {
		return nil, []error{fmt.Errorf("the `encryptedKey` and `value` lengths are inconsistent")}
	}

	sortList := [][]int{}
	for i := 0; i < len(s.SqlConfigs); i++ {
		sortList = append(sortList, []int{})
	}

	for i, v := range encryptedKey {
		_, dbIstr, err := s.SEKey.Decrypt(v)
		if err != nil {
			return nil, []error{fmt.Errorf("decrype err: %v", err)}
		}
		dbI, err := strconv.Atoi(dbIstr)
		if err != nil {
			return nil, []error{fmt.Errorf("dbI string to int err: %v", err)}
		}
		sortList[dbI] = append(sortList[dbI], i)
	}

	sqlKeys := ""
	sqlValList := []string{}
	var isContinues []bool
	for i := 0; i < len(s.ConnectFailTime); i++ {
		isContinues = append(isContinues, s.IsRetryConnect(i))
	}
	var (
		inserts []int64
		errs    []error
	)
	for i := 0; i < len(isContinues); i++ {
		inserts = append(inserts, -1)
		errs = append(errs, nil)
	}
	for _, v := range keys {
		if sqlKeys != "" {
			sqlKeys += ","
		}
		sqlKeys += "`" + v + "`"
	}

	for _, v := range sortList {
		sqlval := ""
		for _, vv := range v {
			sqlv := ""
			for _, vvv := range value[vv] {
				if sqlv != "" {
					sqlv += ","
				}
				sqlv += "'" + vvv + "'"
			}
			if sqlval != "" {
				sqlval += ","
			}
			sqlval += "(" + sqlv + ")"
		}
		sqlValList = append(sqlValList, sqlval)
	}

	var wg *sync.WaitGroup = new(sync.WaitGroup)
	for i := 0; i < len(sqlValList); i++ {
		if sqlValList[i] == "(" {
			continue
		}
		if !isContinues[i] {
			continue
		}
		wg.Add(1)
		sqlStr := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES %s", table, sqlKeys, sqlValList[i])
		reInsert := make(chan int64)
		reErr := make(chan error)
		go s.go_add(i, sqlStr, reInsert, reErr, option.IsShowPrint, Debug)
		rI := false
		rE := false
		for {
			select {
			case insert := <-reInsert:
				if !rI {
					inserts[i] = insert
					rI = true
					close(reInsert)
				}
			case err := <-reErr:
				if !rE {
					errs[i] = err
					rE = true
					close(reErr)
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
			return inserts, errs
		}
	}
	return inserts, nil
}

// ===============
//
//	自动根据 *Setting 向下一个数据库中的指定表添加数据
//	table		string		"表名"
//	keys		[]string	"键名"
//	values		[][]string	"值"
//	Debug		*log.Logger	"调试输出"
//	options		[]IsShowPrintO	"配置"
//		IsShowPrint	bool		"是否输出到控制台"
//	return 1	[]int64		"插入的行数"
//	return 2	[]error		"错误信息"
//
// ===============
//
//	Automatically add data to the specified table in the next database according to *Setting
//	table		string		"table name"
//	keys		[]string	"key name"
//	values		[][]string	"value"
//	Debug		*log.Logger	"debug output"
//	options		[]IsShowPrintO	"Configuration"
//		IsShowPrint	bool		"Whether to output to the console"
//	return 1	[]int64		"Number of rows inserted"
//	return 2	[]error		"Error message"
func (s *Setting) Add(table string, keys []string, values [][]string, Debug *log.Logger, options ...IsShowPrintO) ([]int64, []error) {
	option := &Option{
		IsShowPrint: false,
	}
	for _, o := range options {
		o(option)
	}
	sqlKeys := ""
	sqlValList := []string{}
	var isContinues []bool
	for i := 0; i < len(s.ConnectFailTime); i++ {
		isContinues = append(isContinues, s.IsRetryConnect(i))
	}
	var (
		inserts []int64
		errs    []error
	)
	for i := 0; i < len(isContinues); i++ {
		inserts = append(inserts, -1)
		errs = append(errs, nil)
	}
	for i := 0; i < len(values); i++ {
		val := values[i]
		if len(keys) != len(val) {
			return nil, []error{fmt.Errorf("values[%d]与keys 对象数目不一致", i)}
		}
		sqlVal := ""
		for i := 0; i < len(val); i++ {
			if errStr := CheckString(val[i]); len(errStr) > 0 {
				errs = append(errs, fmt.Errorf("SQL injection: %s ==> %s", val[i], errStr))
				continue
			}
			if sqlVal != "" {
				sqlVal += ","
			}
			sqlVal += "'" + val[i] + "'"
		}
		sqlI := s.NextDBID
		s.NextDBID++
		if s.NextDBID >= s.DBMaxNum {
			s.NextDBID = 0
		}
		if !isContinues[sqlI] {
			i--
			continue
		}
		if sqlI >= len(sqlValList) {
			temp := sqlI - len(sqlValList) + 1
			for i := 0; i < temp; i++ {
				sqlValList = append(sqlValList, "(")
			}
		}
		if sqlI <= s.DBMaxNum && sqlValList[sqlI] != "(" {
			sqlValList[sqlI] += ",("
		}
		sqlValList[sqlI] += sqlVal + ")"
	}
	for i := 0; i < len(keys); i++ {
		if sqlKeys != "" {
			sqlKeys += ","
		}
		sqlKeys += "`" + keys[i] + "`"
	}

	var wg *sync.WaitGroup = new(sync.WaitGroup)
	for i := 0; i < len(sqlValList); i++ {
		if sqlValList[i] == "(" {
			continue
		}
		if !isContinues[i] {
			continue
		}
		wg.Add(1)
		sqlStr := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES %s", table, sqlKeys, sqlValList[i])
		reInsert := make(chan int64)
		reErr := make(chan error)
		go s.go_add(i, sqlStr, reInsert, reErr, option.IsShowPrint, Debug)
		rI := false
		rE := false
		for {
			select {
			case insert := <-reInsert:
				if !rI {
					inserts[i] = insert
					rI = true
					close(reInsert)
				}
			case err := <-reErr:
				if !rE {
					errs[i] = err
					rE = true
					close(reErr)
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
			return inserts, errs
		}
	}
	return inserts, nil
}

func (s *Setting) go_add(i int, sqlStr string, reInsert chan<- int64, reErr chan<- error, IsShowPrint bool, Debug *log.Logger) {
	mI, err := s.MysqlIsRun(i, OLIsShowPrint(IsShowPrint))
	if err != nil {
		s.MysqlClose(mI)
		reInsert <- -1
		reErr <- err
		return
	}
	lastInsertId, _, err := s.MySQLDB[mI].ExecCMD(sqlStr, Debug, OIsShowPrint(IsShowPrint))
	s.MysqlClose(mI, OIsShowPrint(IsShowPrint))
	if err != nil {
		reInsert <- lastInsertId
		reErr <- err
		return
	}
	reInsert <- lastInsertId
	reErr <- err
}
