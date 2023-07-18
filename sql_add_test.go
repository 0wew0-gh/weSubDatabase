package weSubDatabase

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

func TestSQLAdd(t *testing.T) {
	sqlSetting, err := New(testJsonStr)
	if err != nil {
		t.Error("initialization failed:", err)
		return
	}

	println("MySQL Link test")
	mI, err := sqlSetting.MysqlIsRun(0)
	if err != nil {
		t.Error("MySQL Link failed:", err)
		return
	}
	sqlSetting.MysqlClose(mI)
	println("MySQL test link success")

	nextDBID, nextID, err := sqlSetting.SelectLastID("data", "id", nil)
	if err != nil {
		t.Error("MySQL Link failed:", err)

	}
	sqlSetting.NextDBID = nextDBID
	println("NextDBID:", sqlSetting.NextDBID, "maxID:", nextID)

	tn := time.Now()
	sqlSetting.ConnectFailTime[1] = &tn

	values := [][]string{}
	for i := nextID; i < nextID+8; i++ {
		values = append(values, []string{strconv.Itoa(i), fmt.Sprintf("测试%d", i)})
	}
	inserts, errs := sqlSetting.Add("data", []string{"id", "data"}, values, nil, OIsShowPrint(true))
	if errs != nil {
		t.Error("Add failed:", errs)
		return
	}
	fmt.Println("inserts:", inserts)

	fmt.Println(sqlSetting.MySQLDB)
}
