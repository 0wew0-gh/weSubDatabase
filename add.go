package weSubDatabase

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// ===============
//
//	自动根据 *Setting 向下一个数据库中的指定表添加数据
//	table	string	表名
//	keys	[]string	键名
//	values	[][]string	值
//	Debug	*log.Logger	调试输出
//
//	返回值1	[]int64		插入的行数
//	返回值2	[]error		错误信息
//
// ===============
//
//	Automatically add data to the specified table in the next database according to *Setting
//	table		string		table name
//	keys		[]string	key name
//	values		[][]string	value
//	Debug		*log.Logger	debug output
//
//	return 1	[]int64		Number of rows inserted
//	return 2	[]error		Error message
func (s *Setting) Add(table string, keys []string, values [][]string, Debug *log.Logger) ([]int64, []error) {
	sqlKeys := ""
	sqlValList := []string{}
	for i := 0; i < len(values); i++ {
		val := values[i]
		if len(keys) != len(val) {
			return nil, []error{fmt.Errorf("values[%d]与keys 对象数目不一致", i)}
		}
		sqlVal := ""
		for i := 0; i < len(val); i++ {
			if sqlVal != "" {
				sqlVal += ","
			}
			sqlVal += "'" + val[i] + "'"
		}
		sqlI := s.NextAddDBID - 1
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
		s.NextAddDBID++
		if s.NextAddDBID > s.DBMaxNum {
			s.NextAddDBID = 1
		}
	}
	for i := 0; i < len(keys); i++ {
		if sqlKeys != "" {
			sqlKeys += ","
		}
		sqlKeys += "`" + keys[i] + "`"
	}

	var (
		inserts []int64
		errs    []error
	)
	for i := 0; i < len(sqlValList); i++ {
		inserts = append(inserts, -1)
		errs = append(errs, nil)
	}
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	for i := 0; i < len(sqlValList); i++ {
		wg.Add(1)
		reInsert := make(chan int64)
		reErr := make(chan error)
		sqlStr := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES %s", table, sqlKeys, sqlValList[i])
		go s.go_add(i, sqlStr, reInsert, reErr, Debug)
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

func (s *Setting) go_add(i int, sqlStr string, reInsert chan<- int64, reErr chan<- error, Debug *log.Logger) {
	mI, err := s.MysqlIsRun(i)
	if err != nil {
		s.MysqlClose(mI)
		reInsert <- -1
		reErr <- err
		return
	}
	lastInsertId, _, err := s.MySQLDB[mI].ExecCMD(sqlStr, Debug)
	s.MysqlClose(mI)
	if err != nil {
		reInsert <- lastInsertId
		reErr <- err
		return
	}
	reInsert <- lastInsertId
	reErr <- err
}

// ===============
//
//	添加数据
//	table	string		表名
//	keys	string		键名
//	values	string		值
//	Debug	*log.Logger	调试输出
//
//	返回值1	int64		最后插入的id
//	返回值2	error		错误信息
//
// ===============
//
//	Add data
//	table		string		table name
//	keys		string		key name
//	values		string		value
//	Debug		*log.Logger	debug output
//
//	return 1	int64		last insert id
//	return 2	error		Error message
func (s *MysqlDB) AddRecord(table string, keys string, values string, Debug *log.Logger) (int64, error) {
	sqlStr := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES %s", table, keys, values)

	id, _, err := s.ExecCMD(sqlStr, Debug)
	return id, err
}
