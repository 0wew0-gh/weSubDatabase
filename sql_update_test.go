package weSubDatabase

import (
	"fmt"
	"testing"
)

func TestUpdateForID(t *testing.T) {
	t.Log("TestUpdateForID")
	sqlSetting, err := New(testJsonStr)
	if err != nil {
		t.Error("initialization failed:", err)
		return
	}
	var (
		key   []string   = []string{"time", "data"}
		value [][]string = [][]string{{"2023-07-12 17:12:07", "2023-07-12 17:12:08", "2023-07-12 17:12:03"}, {"测试07", "测试08", "测试03"}}
		id    []string   = []string{"YV", "XU", "cV"} // []string{"2", "6"}
	)
	rowsAffected, errs := sqlSetting.Update("data", key, value, "id", id, nil)
	if errs != nil {
		t.Error(errs)
		return
	}
	fmt.Println("Update success! RowsAffected:", rowsAffected)
}

func TestUpdateForKey(t *testing.T) {
	t.Log("TestUpdateForKey")
	sqlSetting, err := New(testJsonStr)
	if err != nil {
		t.Error("initialization failed:", err)
		return
	}
	var (
		key   []string   = []string{"data", "time"}
		value [][]string = [][]string{{"测试7", "测试8", "测试3"}, {"2023-07-12 17:12:17", "2023-07-12 17:12:18", "2023-07-12 17:12:13"}}
		id    []string   = []string{"2023-07-12 17:12:07", "2023-07-12 17:12:08", "2023-07-12 17:12:03"} // []string{"2", "6"}
	)
	rowsAffected, errs := sqlSetting.Update("data", key, value, "time", id, nil, OIPKIsPrimaryKey(false))
	if errs != nil {
		t.Error(errs)
		return
	}
	fmt.Println("Update success! RowsAffected:", rowsAffected)
}
