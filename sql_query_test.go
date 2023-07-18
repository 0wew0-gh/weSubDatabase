package weSubDatabase

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestT(t *testing.T) {
	nextDBID := 3
	isContinues := []bool{true, false, true, true}

	limitList := []string{}
	limit := "1, 10"
	limit = strings.ReplaceAll(limit, " ", "")
	ls := strings.Split(limit, ",")
	if len(ls) >= 2 {
		i, err := strconv.Atoi(ls[1])
		if err == nil {
			limitList = toLimit(nextDBID, ls[0], i, isContinues)
		}
	} else {
		i, err := strconv.Atoi(limit)
		if err == nil {
			limitList = toLimit(nextDBID, "", i, isContinues)
		}
	}
	fmt.Println(limitList)
}

func TestSQLforID(t *testing.T) {
	t.Log("TestSQLforID")
	sqlSetting, err := New(testJsonStr)
	if err != nil {
		t.Error("initialization failed:", err)
		return
	}
	//	map[data:测试7 id:YV time:2023-07-12 17:12:32] DB big_data_test_3 ID 7
	//	map[data:测试8 id:XU time:2023-07-12 17:12:32] DB big_data_test_4 ID 8
	//	map[data:测试3 id:cV time:2023-07-12 09:29:31] DB big_data_test_3 ID 3
	//	map[data:测试4 id:bU time:2023-07-12 09:29:31] DB big_data_test_4 ID 4
	qd, errs := sqlSetting.QueryID("data", "", "id", []string{"YV", "XU", "cV", "bU"}, "`id` DESC", nil, OIsShowPrint(true))
	if errs != nil {
		t.Error("MySQL QueryCMD failed:", errs)
		return
	}
	for i := 0; i < len(qd); i++ {
		fmt.Println(qd[i])
	}
}

func TestArray(t *testing.T) {
	tfail, err := time.Parse("2006-01-02 15:04:05", "2023-07-14 15:50:00")
	if err != nil {
		fmt.Println(err)
		return
	}
	tn := time.Now().UTC()
	relyTime := 300000
	tend := tfail.Add(time.Millisecond * time.Duration(relyTime))
	fmt.Println(tn)
	fmt.Println(tfail)
	fmt.Println(tend)
	fmt.Println(tfail.Before(tend), tfail.After(tend))
}

func TestSQLQuery(t *testing.T) {
	t.Log("TestSQLQuery")
	sqlSetting, err := New(testJsonStr)
	if err != nil {
		t.Error("initialization failed:", err)
		return
	}

	nextDBI, maxID, err := sqlSetting.SelectLastID("data", "id", nil)
	if err != nil {
		t.Error("MySQL SelectLastID failed:", err)
	}
	println("NextAddDBID:", nextDBI, "maxID:", maxID)

	println("MySQL Link test")
	qd, errs := sqlSetting.Query("data", "*", "id", "`time` BETWEEN '2023-07-14 13:54:48' and '2023-07-14 13:54:55'", "`id` DESC", "10", nil, OIsShowPrint(true))
	if errs != nil {
		t.Error("MySQL QueryCMD failed:", errs)
	}
	for i := 0; i < len(qd); i++ {
		id := qd[i]["id"]
		reStr, reEi, _ := sqlSetting.SEKey.Decrypt(id)
		DBi, err := strconv.Atoi(reEi)
		dbName := ""
		if err == nil {
			dbName = sqlSetting.SqlConfigs[DBi].DB
		}
		t.Log("=====>", dbName, reStr, i, "<=====")
	}
}

func TestQueryLastID(t *testing.T) {
	t.Log("TestQueryLastID")
	sqlSetting, err := New(testJsonStr)
	if err != nil {
		t.Error("initialization failed:", err)
		return
	}

	nextDBI, maxID, err := sqlSetting.SelectLastID("data", "id", nil)
	if err != nil {
		t.Error("MySQL SelectLastID failed:", err)
	}
	println("NextAddDBID:", nextDBI, "maxID:", maxID)
}
