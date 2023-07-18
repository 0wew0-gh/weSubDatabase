package weSubDatabase

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// ===============
//
//	自动根据 *Setting 向下一个数据库中的指定表删除数据
//	table			string		表名
//	forKey			string		键名
//	ids			[]string	值
//	Debug			*log.Logger	调试输出
//	options			[]IsPrimaryKeyO	配置
//		IsPrimaryKey	bool		是否为主键
//		IsShowPrint		bool		是否输出到控制台
//
//	返回值1			[]int64		删除的行数
//	返回值2			[]error		错误信息
//
// ===============
//
//	Automatically delete data from the specified table in the next database according to *Setting
//	table			string		table name
//	forKey			string		key name
//	ids			[]string	value
//	Debug			*log.Logger	debug output
//	options			[]IsPrimaryKeyO	Configuration
//		IsPrimaryKey	bool		Whether it is a primary key
//		IsShowPrint		bool		Whether to output to the console
//
//	return 1		[]int64		Number of rows deleted
//	return 2		[]error		Error message
func (s *Setting) Delete(table string, forKey string, ids []string, Debug *log.Logger, options ...IsPrimaryKeyO) ([]int64, []error) {
	option := &Option{
		IsPrimaryKey: true,
		IsShowPrint:  false,
	}
	for _, o := range options {
		o(option)
	}

	var (
		dbIList []bool
		idList  [][]string
		reInt   []int64
		errs    []error
	)
	if option.IsPrimaryKey {
		dbIList, idList, _ = s.DecryptID(forKey, ids)
	} else {
		for i := 0; i < len(s.SqlConfigs); i++ {
			dbIList = append(dbIList, true)
			idList = append(idList, ids)
		}
	}
	for i := 0; i < len(s.SqlConfigs); i++ {
		reInt = append(reInt, -1)
		errs = append(errs, nil)
	}
	if option.IsShowPrint {
		fmt.Println("=================")
		fmt.Println(dbIList)
		fmt.Println(idList)
		fmt.Println("=================")
	}
	var wg sync.WaitGroup
	for sqlI := 0; sqlI < len(s.SqlConfigs); sqlI++ {
		if !s.IsRetryConnect(sqlI) {
			continue
		}
		if !dbIList[sqlI] {
			continue
		}
		wg.Add(1)
		sqlStr := "DELETE FROM `" + table + "` WHERE `" + forKey + "` IN ("
		where := ""
		for i := 0; i < len(idList[sqlI]); i++ {
			if where != "" {
				where += ","
			}
			where += "'" + idList[sqlI][i] + "'"
		}
		sqlStr += where + ");"
		if option.IsShowPrint {
			fmt.Println("[", s.SqlConfigs[sqlI].DB, "]:", sqlStr)
		}
		chanRA := make(chan int64)
		chanErr := make(chan error)
		go s.go_exec(sqlI, sqlStr, nil, chanRA, chanErr, option.IsShowPrint, Debug)
		rRA := false
		rE := false
		for {
			select {
			case reqd := <-chanRA:
				if !rRA {
					reInt[sqlI] = reqd
					close(chanRA)
					rRA = true
				}
			case reErr := <-chanErr:
				if !rE {
					errs = append(errs, reErr)
					close(chanErr)
					rE = true
				}
			}
			if rRA && rE {
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
	return reInt, nil
}
