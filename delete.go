package weSubDatabase

import (
	"fmt"
	"log"
	"sync"
	"time"
)

func (s *Setting) Delete(table string, forKey string, ids []string, Debug *log.Logger, options ...UpdateOptionConfig) ([]int64, []error) {
	option := &Option{
		IsPrimaryKey: true,
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
	fmt.Println("=================")
	fmt.Println(dbIList)
	fmt.Println(idList)
	fmt.Println("=================")
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
		fmt.Println("[", s.SqlConfigs[sqlI].DB, "]:", sqlStr)
		chanRA := make(chan int64)
		chanErr := make(chan error)
		go s.go_exec(sqlI, sqlStr, nil, chanRA, chanErr, Debug)
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
