package weSubDatabase

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"time"
)

// ===============
//
//	加密主键, *Setting.SEKey 为 nil 时不加密
//	queryDatas	[]map[string]string	"查询结果"
//	primaryKey	string			"主键字段名
//											为空字符串时不加密"
//	return		[]map[string]string	"查询结果"
//
// ===============
//
//	Encrypt primary key, *Setting.SEKey is not encrypted when nil
//	queryDatas	[]map[string]string	"query result"
//	primaryKey	string			"primary key field name
//											When it is an empty string,
//											it is not encrypted"
//	return		[]map[string]string	query result
func (s *Setting) EncryptPrimaryKey(queryDatas []map[string]string, primaryKey string) []map[string]string {
	if s.SEKey == nil {
		return queryDatas
	}
	if primaryKey == "" {
		return queryDatas
	}
	for i := 0; i < len(queryDatas); i++ {
		idStr := queryDatas[i][primaryKey]
		if idStr == "" {
			continue
		}
		dbI := queryDatas[i]["db"]
		if dbI == "" {
			continue
		}
		queryDatas[i][primaryKey] = s.SEKey.Encrypt(idStr, dbI)
		delete(queryDatas[i], "db")
	}
	return queryDatas
}

// ===============
//
//	根据主键和id数组解密出数据库ID和主键ID
//	primaryKey	string		"主键字段名"
//	ids		[]string	"主键ID数组"
//	return 1	[]bool		"解密出的ID是否有对应的数据库"
//	return 2	[][]string	"解密出的数据库ID"
//	return 3	[][]int		"解密出的主键ID在原数组中的位置"
//
// ===============
//
//	Decrypt the database ID and primary key ID based on the primary key and ID array
//	primaryKey	string		"primary key field name"
//	ids		[]string	"primary key ID array"
//	return 1	[]bool		"Whether the decrypted ID has a
//									corresponding database"
//	return 2	[][]string	"Decrypted database ID"
//	return 3	[][]int		"The position of the decrypted
//									primary key ID in the original
//									array"
func (s *Setting) DecryptID(primaryKey string, ids []string) ([]bool, [][]string, [][]int) {
	var (
		dbIList []bool     = []bool{}
		idList  [][]string = [][]string{}
		pwList  [][]int    = [][]int{}
	)
	for i := 0; i < len(s.SqlConfigs); i++ {
		dbIList = append(dbIList, false)
		idList = append(idList, []string{})
		pwList = append(pwList, []int{})
	}
	for i, v := range ids {
		id, dbI, err := s.SEKey.Decrypt(v)
		if err != nil {
			continue
		}
		dbIint, err := strconv.Atoi(dbI)
		if err != nil {
			continue
		}
		dbIList[dbIint] = true
		idList[dbIint] = append(idList[dbIint], id)
		pwList[dbIint] = append(pwList[dbIint], i)
	}
	return dbIList, idList, pwList
}

// ===============
//
//	判断是否可以连接数据库
//	item		int	"需要连接的数据库在配置中的位置"
//	return 1	bool	"是否可以尝试连接"
//
// ===============
//
//	Determine whether the database can be connected
//	item		int	"Location of the database to be
//					 	connected in the configuration"
//	return 1	bool	"Whether to try to connect"
func (s *Setting) IsRetryConnect(item int) bool {
	if s.ConnectFailTime[item] != nil {
		tn := time.Now()
		tend := s.ConnectFailTime[item].Add(time.Millisecond * time.Duration(s.ConnectAgainTime))
		if tn.After(tend) {
			s.ConnectFailTime[item] = nil
			return true
		} else {
			return false
		}
	}
	return true
}

// ===============
//
//	根据下一个数据库ID和限制数目生成限制字符串
//	nextDBID	int		"下一个数据库ID"
//	pageNumber	string		"页码"
//	limit		int		"限制数目"
//	isContinues	[]bool		"是否可以连接数据库"
//	return		[]string	"限制字符串数组"
//
// ===============
//
//	Generate limit string based on next database ID and limit number
//	nextDBID	int		"Next database ID"
//	pageNumber	string		"Page number"
//	limit		int		"Limit number"
//	isContinues	[]bool		"Whether the database can be
//									connected"
//	return		[]string	"Limit string array"
func toLimit(nextDBID int, pageNumber string, limit int, isContinues []bool) []string {
	if limit <= 0 {
		return nil
	}
	limitList := []string{}
	linkDBCount := 0
	for i := 0; i < len(isContinues); i++ {
		limitList = append(limitList, "")
		if isContinues[i] {
			linkDBCount += 1
		}
	}
	if nextDBID > linkDBCount {
		return nil
	}
	nextDBID -= 1
	maxDBNum := len(isContinues)
	f := math.Floor(float64(limit) / float64(linkDBCount))
	remainder := limit % linkDBCount
	ii := 0
	for i := nextDBID; i < maxDBNum; i++ {
		if !isContinues[i] {
			if i+1 >= maxDBNum {
				i = -1
			}
			ii++
			if ii >= maxDBNum {
				break
			}
			continue
		}
		limit := ""
		if pageNumber != "" {
			limit = pageNumber + ","
		}
		if remainder > 0 {
			limit += fmt.Sprintf("%d", int(f)+1)
			remainder -= 1
		} else {
			limit += fmt.Sprintf("%d", int(f))
		}
		limitList[i] = limit
		if i+1 >= maxDBNum {
			i = -1
		}
		ii++
		if ii >= maxDBNum {
			break
		}
	}
	return limitList
}

// 正则表达式
//
// Regular expression
var regexpString string = ``

// 默认正则表达式
//
// Default regular expression
const defaultRegexpString string = `([\x00-\x08\x0B-\x0C\x0E-\x1F\x7F]+)|(?:')|(?:\\)|(?:--)|(\b(\\x|�|select|update|and|or|delete|insert|trancate|char|chr|into|substr|ascii|declare|exec|count|master|into|drop|execute)\b)`

// ===============
//
//	设置正则表达式
//
//	regexpStr	string	"正则表达式
//							空字符串时修改为默认值"
//
// ===============
//
//	Set regular expression
//
//	regexpStr	string	"Regular expression
//							When the string is empty, it is default"
func SetRegexpString(regexpStr string) {
	regexpString = regexpStr
}

// ===============
//
//	检查字符串中是否有linux下的非法字符和SQL注入
//
// "
//
//	str	string	"需要检查的字符串"
//	return	bool	"是否有非法字符"
//
// ===============
//
//	Check whether there are illegal characters and SQL injection under Linux in the string
//
//	str	string	"String to be checked"
//	return	bool	"Whether there are illegal characters"
func CheckString(str string) []string {
	var re *regexp.Regexp
	if regexpString == "" {
		re = regexp.MustCompile(defaultRegexpString)
	} else {
		re = regexp.MustCompile(regexpString)
	}
	return re.FindAllString(str, -1)
}
