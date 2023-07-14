package weSubDatabase

import (
	"fmt"
	"testing"
)

func TestDeleteForID(t *testing.T) {
	t.Log("TestDeleteForID")
	sqlSetting, err := New(testJsonStr, 1)
	if err != nil {
		t.Error("initialization failed:", err)
		return
	}
	var (
		id []string = []string{"YV", "XU", "cV"} // []string{"2", "6"}
	)
	rowsAffected, errs := sqlSetting.Delete("data", "id", id, nil)
	if errs != nil {
		t.Error(errs)
		return
	}
	fmt.Println("Delete success! RowsAffected:", rowsAffected)
}

func TestDeleteForKey(t *testing.T) {
	t.Log("TestDeleteForKey")
	sqlSetting, err := New(testJsonStr, 1)
	if err != nil {
		t.Error("initialization failed:", err)
		return
	}
	var (
		id []string = []string{"测试1", "测试4", "测试5"} // []string{"2", "6"}
	)
	rowsAffected, errs := sqlSetting.Delete("data", "data", id, nil, OptionIsPrimaryKey(false))
	if errs != nil {
		t.Error(errs)
		return
	}
	fmt.Println("Delete success! RowsAffected:", rowsAffected)
}
