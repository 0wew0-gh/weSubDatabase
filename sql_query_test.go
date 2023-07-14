package weSubDatabase

import (
	"fmt"
	"strconv"
	"testing"
)

func TestSQLforID(t *testing.T) {
	t.Log("TestSQLforID")
	sqlSetting, err := New(testJsonStr, 1)
	if err != nil {
		t.Error("initialization failed:", err)
		return
	}
	//	map[data:测试7 id:YV time:2023-07-12 17:12:32] DB big_data_test_3 ID 7
	//	map[data:测试8 id:XU time:2023-07-12 17:12:32] DB big_data_test_4 ID 8
	//	map[data:测试3 id:cV time:2023-07-12 09:29:31] DB big_data_test_3 ID 3
	//	map[data:测试4 id:bU time:2023-07-12 09:29:31] DB big_data_test_4 ID 4
	qd, errs := sqlSetting.QueryID("data", "", "id", []string{"YV", "XU", "cV", "bU"}, "`id` DESC", nil)
	if errs != nil {
		t.Error("MySQL QueryCMD failed:", errs)
		return
	}
	for i := 0; i < len(qd); i++ {
		fmt.Println(qd[i])
	}
}

func TestSQLQuery(t *testing.T) {
	t.Log("TestSQLQuery")
	sqlSetting, err := New(testJsonStr, 1)
	if err != nil {
		t.Error("initialization failed:", err)
		return
	}

	println("MySQL Link test")
	qd, errs := sqlSetting.Query("data", "*", "id", "", "`id` DESC", "", nil)
	if errs != nil {
		t.Error("MySQL QueryCMD failed:", errs)
		return
	}
	for i := 0; i < len(qd); i++ {
		id := qd[i]["id"]
		reStr, reEi, _ := sqlSetting.SEKey.Decrypt(id)
		DBi, err := strconv.Atoi(reEi)
		dbName := ""
		if err == nil {
			dbName = sqlSetting.SqlConfigs[DBi].DB

		}
		fmt.Println(qd[i], "DB", dbName, "ID", reStr)
	}
}

func TestQueryLastID(t *testing.T) {
	t.Log("TestQueryLastID")
	sqlSetting, err := New(testJsonStr, 1)
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
