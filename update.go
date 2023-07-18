package weSubDatabase

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// ===============
//
//	更新数据
//	table			string			表名
//	key			[]string		字段名
//	value			[][]string		值
//	forKey			string			加密用的key
//	ids			[]string		主键
//	Debug			*log.Logger		调试输出
//	options			[]UpdateOptionConfig	配置
//		IsPrimaryKey	bool			是否使用主键
//		IsShowPrint		bool			是否输出到控制台
//
//	返回值1			[]int64			更新的行数
//	返回值2			error			错误信息
//
// ===============
//
//	Update data
//	table			string			Table name
//	key			[]string		Field name
//	value			[][]string		Value
//	forKey			string			Encryption key
//	ids			[]string		Primary key
//	Debug			*log.Logger		Debug output
//	options			[]UpdateOptionConfig	Configuration
//		IsPrimaryKey	bool			Whether to use the
//													primary key
//		IsShowPrint		bool			Whether to output
//													to the console
//
//	return 1		[]int64			Number of rows
//													updated
//	return 2		error			Error message
func (s *Setting) Update(table string, key []string, value [][]string, forKey string, ids []string, Debug *log.Logger, options ...IsPrimaryKeyO) ([]int64, []error) {
	option := &Option{
		IsPrimaryKey: true,
		IsShowPrint:  false,
	}
	for _, o := range options {
		o(option)
	}

	valueLen := 0
	for i := 0; i < len(value); i++ {
		for j := 0; j < len(value[i]); j++ {
			if valueLen == 0 {
				valueLen = len(value[i])
				continue
			}
			if valueLen != len(value[i]) {
				return nil, []error{fmt.Errorf("inconsistent quantity of 'value'")}
			}
		}
	}
	if len(key) != len(value) {
		return nil, []error{fmt.Errorf("inconsistent quantity of 'key' and 'value'")}
	}
	if len(ids) != valueLen {
		return nil, []error{fmt.Errorf("inconsistent quantity of 'key' and 'ids'")}
	}

	var (
		dbIList  []bool
		idList   [][]string
		itemList [][]int
		reInt    []int64
		errs     []error
	)
	if option.IsPrimaryKey {
		dbIList, idList, itemList = s.DecryptID(forKey, ids)
	} else {
		for i := 0; i < len(s.SqlConfigs); i++ {
			dbIList = append(dbIList, true)
			idList = append(idList, ids)
			items := []int{}
			for j := 0; j < len(ids); j++ {
				items = append(items, j)
			}
			itemList = append(itemList, items)
		}
	}
	for i := 0; i < len(s.SqlConfigs); i++ {
		reInt = append(reInt, -1)
		errs = append(errs, nil)
	}
	var wg sync.WaitGroup
	for sqlI := 0; sqlI < len(s.SqlConfigs); sqlI++ {
		if !s.IsRetryConnect(sqlI) {
			continue
		}
		wg.Add(1)
		if !dbIList[sqlI] {
			wg.Done()
			continue
		}
		sqlStr := "UPDATE `" + table + "` SET"
		setStr := ""
		idStr := ""
		for i := 0; i < len(key); i++ {
			if setStr != "" {
				setStr += ","
			}

			setStr += " `" + key[i] + "`=CASE `" + forKey + "`"
			for j := 0; j < len(value[i]); j++ {
				if j >= len(idList[sqlI]) {
					continue
				}
				valI := itemList[sqlI][j]
				if valI >= len(value[i]) {
					continue
				}
				setStr += " WHEN '" + idList[sqlI][j]
				setStr += "' THEN '" + value[i][valI] + "'"
			}
			setStr += " END"
		}
		for i := 0; i < len(idList[sqlI]); i++ {
			if idStr != "" {
				idStr += ","
			}
			idStr += "'" + idList[sqlI][i] + "'"
		}
		sqlStr += setStr + " WHERE `" + forKey + "` IN (" + idStr + ")"
		if forKey == "" {
			wg.Done()
			continue
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
