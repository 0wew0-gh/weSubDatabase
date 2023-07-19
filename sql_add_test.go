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
	j := nextID
	for i := nextID; i < nextID+8; i++ {
		msg := fmt.Sprintf("测试%d", i)
		if errStr := CheckString(msg); len(errStr) > 0 {
			fmt.Println(msg, "有非法字符")
			continue
		}
		values = append(values, []string{strconv.Itoa(j), msg})
		j += 1
	}
	inserts, errs := sqlSetting.Add("data", []string{"id", "data"}, values, nil, OIsShowPrint(true))
	if errs != nil {
		t.Error("Add failed:", errs)
		return
	}
	fmt.Println("inserts:", inserts)
	fmt.Println("MySQLDB array:", sqlSetting.MySQLDB)
}
