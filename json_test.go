package weSubDatabase

import (
	"fmt"
	"testing"
)

var testJsonStr = `{"mysql_name":"big_data_test","mysql":[{"mysql_user":"0wew0","mysql_pwd":"7%5cfM3!&pw9^#6j","mysql_addr":"127.0.0.1","mysql_port":"3306","mysql_db":"big_data_test_1"},{"mysql_user":"0wew0","mysql_pwd":"7%5cfM3!&pw9^#6j","mysql_addr":"127.0.0.1","mysql_port":"3306","mysql_db":"big_data_test_2"},{"mysql_user":"0wew0","mysql_pwd":"7%5cfM3!&pw9^#6j","mysql_addr":"127.0.0.1","mysql_port":"3306","mysql_db":"big_data_test_3"},{"mysql_user":"0wew0","mysql_pwd":"7%5cfM3!&pw9^#6j","mysql_addr":"127.0.0.1","mysql_port":"3306","mysql_db":"big_data_test_4"}],"redis":{"redis_addr":"127.0.0.1","redis_port":"6379","redis_pwd":"","redis_db":"1"},"redis_next_sql_db":0,"redis_key":"big_data_db_item","redis_max_db":255,"maxLinkNumber":{"mysql":10,"redis":10},"contrast":{"extraItem":6,"key":["jb10=m/zkvpds=1/","/*-+0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"]}}`
var configs = []string{"{\"mysql_user\":\"0wew0\",\"mysql_pwd\":\"7%5cfM3!&pw9^#6j\",\"mysql_addr\":\"127.0.0.1\",\"mysql_port\":\"3306\",\"mysql_db\":\"big_data_test_1\"}", "{\"mysql_user\":\"0wew0\",\"mysql_pwd\":\"7%5cfM3!&pw9^#6j\",\"mysql_addr\":\"127.0.0.1\",\"mysql_port\":\"3306\",\"mysql_db\":\"big_data_test_2\"}", "{\"mysql_user\":\"0wew0\",\"mysql_pwd\":\"7%5cfM3!&pw9^#6j\",\"mysql_addr\":\"127.0.0.1\",\"mysql_port\":\"3306\",\"mysql_db\":\"big_data_test_3\"}", "{\"mysql_user\":\"0wew0\",\"mysql_pwd\":\"7%5cfM3!&pw9^#6j\",\"mysql_addr\":\"127.0.0.1\",\"mysql_port\":\"3306\",\"mysql_db\":\"big_data_test_4\"}"}

func TestJson(t *testing.T) {
	var (
		err  error
		json *wJson
		temp interface{}
	)
	json, err = Parse(testJsonStr)
	if err != nil {
		t.Error(err)
		return
	}
	key := "mysql"
	temp = json.Get(key)
	if !json.Exists() {
		t.Error(key, "not found")
		return
	}
	switch temp.(type) {
	case string:
		println(key, "is string")
	case []interface{}:
		println(key, "is []interface{}")
	case map[string]interface{}:
		println(key, "is map[string]interface{}")
	case float32:
		println(key, "is float32")
	case float64:
		println(key, "is float64")
	case int:
		println(key, "is int")
	}
	fmt.Println(temp)
	println("===============")

	sqlConfig, err := GetConfig(testJsonStr)
	if err != nil {
		println(err.Error())
		return
	}
	fmt.Println(sqlConfig.MysqlName)
	fmt.Println(sqlConfig.Mysql)
	println("===============")

	for i := 0; i < len(configs); i++ {
		println("==== vvvvv ====")
		config, err := GetSQLConfig(configs[i])
		if err != nil {
			println(err.Error())
			continue
		}
		println(config.User)
		println(config.Password)
		println(config.Address)
		println(config.Port)
		println(config.DB)
	}
}
