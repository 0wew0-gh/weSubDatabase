package weSubDatabase

import (
	"fmt"
	"testing"
)

func TestEncryptDecrypt(t *testing.T) {
	sqlSetting, err := New(testJsonStr)
	if err != nil {
		t.Error("initialization failed:", err)
		return
	}
	strs := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}
	extraStrs := []string{"0", "1", "2", "3", "0", "1", "2", "3", "0", "1"}
	encryptStrs := []string{}
	for i := 0; i < len(strs); i++ {
		encryptStrs = append(encryptStrs, sqlSetting.SEKey.Encrypt(strs[i], extraStrs[i]))
	}
	fmt.Println("encryptStrs:", encryptStrs)
	dbIList, idList, itemList := sqlSetting.DecryptID("id", encryptStrs)
	fmt.Println("==============")
	fmt.Println(dbIList)
	fmt.Println(idList)
	fmt.Println(itemList)
	fmt.Println("==============")
}
