package weSubDatabase

import (
	"fmt"
	"testing"
)

func TestRedis(t *testing.T) {
	setting, err := New(testJsonStr)
	if err != nil {
		t.Error("initialization failed:", err)
		return
	}
	println("Redis Link test")

	setting.NextRedisDBID++
	if setting.NextRedisDBID >= setting.RedisMaxNum {
		setting.NextRedisDBID = 0
	}
	rI, err := setting.RedisIsRun(setting.NextRedisDBID, 1)
	if err != nil {
		t.Error("Redis Link failed:", err)
		return
	}
	err = setting.RedisDB[rI].SetValue("test", "test", RedisO(OLRedisAutoDeleteTime(100)))
	if err != nil {
		fmt.Println("Redis set err:", err)
	}
	err = setting.RedisDB[rI].SetValue("test1", "test1", RedisO(OLRedisAutoDeleteTime(100)))
	if err != nil {
		fmt.Println("Redis set err:", err)
	}
	err = setting.RedisDB[rI].SetValue("test2", "test2", RedisO(OLRedisAutoDeleteTime(100)))
	if err != nil {
		fmt.Println("Redis set err:", err)
	}
	err = setting.RedisDB[rI].SetValue("t2", "t2", RedisO(OLRedisAutoDeleteTime(100)))
	if err != nil {
		fmt.Println("Redis set err:", err)
	}
	err = setting.RedisDB[rI].SetValue("test3", "test3", RedisO(OLRedisAutoDeleteTime(100)))
	if err != nil {
		fmt.Println("Redis set err:", err)
	}
	err = setting.RedisDB[rI].SetValue("test4", "test4", RedisO(OLRedisAutoDeleteTime(100)))
	if err != nil {
		fmt.Println("Redis set err:", err)
	}

	keys, err := setting.RedisDB[rI].Keys("*")
	if err != nil {
		fmt.Println("Redis get keys err:", err)
	}
	fmt.Println("Redis keys:", keys)

	val, err := setting.RedisDB[rI].GetStringAll("*", RedisO(OLRedisIsDelete(false)))
	if err != nil {
		fmt.Println("Redis get val err:", err)
	}
	fmt.Println("Redis vals:", val)

	err = setting.RedisDB[rI].Del([]string{"test", "test1"})
	if err != nil {
		fmt.Println("Redis Del err:", err)
	}

	val, err = setting.RedisDB[rI].GetStringAll("*", RedisO(OLRedisIsDelete(false)))
	if err != nil {
		fmt.Println("Redis get val err:", err)
	}
	fmt.Println("Redis vals:", val)

	err = setting.RedisDB[rI].DelMulti("*2")
	if err != nil {
		fmt.Println("Redis DelMulti err:", err)
	}

	val, err = setting.RedisDB[rI].GetStringAll("*", RedisO(OLRedisIsDelete(false)))
	if err != nil {
		fmt.Println("Redis get val err:", err)
	}
	fmt.Println("Redis vals:", val)
	setting.RedisClose(rI)
}
