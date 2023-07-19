package weSubDatabase

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
)

type wJson struct {
	Data  map[string]interface{}
	Key   string
	Value interface{}
}

// ===============
//
//	解析json字符串
//	jsonStr		string		"需要解析的json字符串"
//	return 1	*Json		"解析后的json对象"
//	return 2	error		"解析错误"
//
//	==============
//
//	Parsing JSON strings
//	jsonStr		string		"json string"
//	return1		*Json		"parsed json object"
//	return2		error		"parsing error"
func Parse(jsonStr string) (*wJson, error) {
	tempJson := make(map[string]interface{}, 0)
	err := json.Unmarshal([]byte(jsonStr), &tempJson)
	if err != nil {
		return nil, err
	}
	return &wJson{Data: tempJson}, nil
}

// ===============
//
//	获取json中的值
//	key		string		"需要获取的key"
//	return 1	interface{}	"获取到的值"
//
//	==============
//
//	Get the value in json
//	key		string		"key"
//	return 1	interface{}	"value"
func (w *wJson) Get(key string) interface{} {
	temp := w.Data[key]
	w.Key = key
	w.Value = temp
	return temp
}

// ===============
//
//	上次调用Get()方法后，是否存在值
//	return 1	bool	"是否存在"
//
//	==============
//
//	After the last call to the Get() method, is there a value
//	return 1	bool	"whether exists"
func (w *wJson) Exists() bool {
	return w.Value != nil
}

// ===============
//
//	返回上次调用Get()方法后，获取到的值的字符串形式
//	return 1	string	"获取到的值的字符串形式"
//
// ==============
//
//	return the string form of the value obtained after the last call to the Get() method
//	return 1	string	"string form of the value obtained"
func (w *wJson) String() string {
	switch tempType := w.Value.(type) {
	case string:
		return tempType
	case int:
		temp := strconv.Itoa(tempType)
		return temp
	case float32:
		temp := strconv.FormatFloat(float64(tempType), 'f', -1, 32)
		return temp
	case float64:
		temp := strconv.FormatFloat(tempType, 'f', -1, 64)
		return temp
	case bool:
		temp := strconv.FormatBool(tempType)
		return temp
	case []interface{}:
	case map[string]interface{}:
		temp, err := json.Marshal(tempType)
		if err != nil {
			return ""
		}
		return string(temp)
	}
	return ""
}

type Config struct {
	MysqlName string        `json:"mysql_name"`
	Mysql     []SQLConfig   `json:"mysql"`
	MaxLink   MaxLinkNumber `json:"MaxLinkNumber"`
	Contrast  Contrast      `json:"contrast"`
}

type SQLConfig struct {
	User     string `json:"mysql_user"`
	Password string `json:"mysql_pwd"`
	Address  string `json:"mysql_addr"`
	Port     string `json:"mysql_port"`
	DB       string `json:"mysql_db"`
}

type MaxLinkNumber struct {
	MySQL int `json:"mysql"`
	Redis int `json:"redis"`
}
type Contrast struct {
	ExtraItem int      `json:"extraItem"`
	Key       []string `json:"key"`
}

// ===============
//
//	从json字符串中解析配置
//	configStr	string	"需要解析的json字符串"
//	return 1	*Config	"解析后的配置对象"
//	return 2	error	"解析错误"
//
//	==============
//
//	Parse configuration from json string
//	configStr	string	"json string"
//	return 1	*Config	"parsed configuration object"
//	return 2	error	"parsing error"
func GetConfig(configStr string) (*Config, error) {
	var config Config
	err := json.Unmarshal([]byte(configStr), &config)
	if err != nil {
		return nil, fmt.Errorf("json string Error:%s", err.Error())
	}
	return &config, nil
}

// ===============
//
//	从json字符串中解析MySQL配置
//	config		string		"需要解析的json字符串"
//	return 1	*SQLConfig	"解析后的MySQL配置对象"
//	return 2	error		"解析错误"
//
//	==============
//
//	Parse MySQL configuration from json string
//	config		string		"json string"
//	return 1	*SQLConfig	"parsed MySQL configuration object"
//	return 2	error		"parsing error"
func GetSQLConfig(config string) (*SQLConfig, error) {
	var sqlConfig SQLConfig
	err := json.Unmarshal([]byte(config), &sqlConfig)
	if err != nil {
		return nil, fmt.Errorf("json string Error:%s", err.Error())
	}
	return &sqlConfig, nil
}

// ===============
//
//	从字典中解析MySQL配置
//	configMap	map[string]interface{}	"需要解析的字典"
//	return 1	*SQLConfig		"解析后的MySQL配置对象"
//
//	==============
//
//	Parse MySQL configuration from dictionary
//	configMap	map[string]interface{}	"dictionary"
//	return 1	*SQLConfig		"parsed MySQL configuration
//											object"
func GetSQLConfigMap(configMap map[string]interface{}) *SQLConfig {
	var sqlConfig SQLConfig
	temp := configMap["mysql_user"]
	if temp != nil && reflect.TypeOf(temp).String() == "string" {
		sqlConfig.User = temp.(string)
	} else {
		return nil
	}
	temp = configMap["mysql_pwd"]
	if temp != nil && reflect.TypeOf(temp).String() == "string" {
		sqlConfig.Password = temp.(string)
	} else {
		return nil
	}
	temp = configMap["mysql_addr"]
	if temp != nil && reflect.TypeOf(temp).String() == "string" {
		sqlConfig.Address = temp.(string)
	} else {
		return nil
	}
	temp = configMap["mysql_port"]
	if temp != nil && reflect.TypeOf(temp).String() == "string" {
		sqlConfig.Port = temp.(string)
	} else {
		return nil
	}
	temp = configMap["mysql_db"]
	if temp != nil && reflect.TypeOf(temp).String() == "string" {
		sqlConfig.DB = temp.(string)
	} else {
		return nil
	}
	return &sqlConfig
}
