package common

import(
	"fmt"
	"crypto/aes"
	"crypto/cipher"
	"strconv"
	"database/sql"
	s "strings"

	"ap/src/dbpool"
	cf "ap/src/config"
)

//물음표 컬럼 개수만큼 조인
func GetQuestionMark(column []string) string {
	var placeholders []string
	numPlaceholders := len(column) // 원하는 물음표 수
	for i := 0; i < numPlaceholders; i++ {
	    placeholders = append(placeholders, "?")
	}
	return s.Join(placeholders, ",")
}

func GetReqAtColumn() []string {
	atReqColumn := []string{
		"msgid",
		"userid",
		"ad_flag",
		"button1",
		"button2",
		"button3",
		"button4",
		"button5",
		"image_link",
		"image_url",
		"message_type",
		"msg",
		"msg_sms",
		"only_sms",
		"phn",
		"profile",
		"p_com",
		"p_invoice",
		"reg_dt",
		"remark1",
		"remark2",
		"remark3",
		"remark4",
		"remark5",
		"reserve_dt",
		"sms_kind",
		"sms_lms_tit",
		"sms_sender",
		"s_code",
		"tmpl_id",
		"wide",
		"send_group",
		"supplement",
		"price",
		"currency_type",
		"title",
		"mms_image_id",
		// "header",
		// "carousel",
		"push_id",
		"app_key",
		"app_secret",
		"app_link",
		"app_launch",
		"atch_file_sn",
	}
	return atReqColumn
}

func GetReqAtColumn2() []string {
	atReqColumn := []string{
		"msgid",
		"userid",
		"ad_flag",
		"button1",
		"button2",
		"button3",
		"button4",
		"button5",
		"image_link",
		"image_url",
		"message_type",
		"msg",
		"msg_sms",
		"only_sms",
		"phn",
		"profile",
		"p_com",
		"p_invoice",
		"reg_dt",
		"remark1",
		"remark2",
		"remark3",
		"remark4",
		"remark5",
		"reserve_dt",
		"sms_kind",
		"sms_lms_tit",
		"sms_sender",
		"s_code",
		"tmpl_id",
		"wide",
		// "send_group",
		"supplement",
		"price",
		"currency_type",
		"title",
		"mms_image_id",
		// "header",
		// "carousel",
	}
	return atReqColumn
}

//테이블 insert 처리
func InsMsg(query string, insStrs []string, insValues []interface{}) ([]string, []interface{}){
	var errlog = cf.Stdlog
	stmt := fmt.Sprintf(query, s.Join(insStrs, ","))
	_, err := dbpool.DB.Exec(stmt, insValues...)

	if err != nil {
		errlog.Println("Result Table Insert 처리 중 오류 발생 ", err.Error())
		errlog.Println("table : ", query)
	}
	return nil, nil
}


//AES 복호화
func AES256GSMDecrypt(secretKey []byte, ciphertext_ string, nonce_ string) string {
	var errlog = cf.Stdlog
	ciphertext, _ := convertByte(ciphertext_)
	nonce, _ := convertByte(nonce_)

	if len(secretKey) != 32 {
		return ""
	}

	// prepare AES-256-GSM cipher
	block, err := aes.NewCipher(secretKey)
	if err != nil {
		errlog.Println("암호화 블록 초기화 실패 : ", err.Error())
		return ""
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		errlog.Println("GCM 암호화기를 초기화 실패 : ", err.Error())
		return ""
	}

	// decrypt ciphertext
	plaintext, err := aesgcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		errlog.Println("복호화 실패 : ", err.Error())
		return ""
	}

	return string(plaintext)
}

//바이트 생성
func convertByte(src string) ([]byte, error) {
	ba := make([]byte, len(src)/2)
	idx := 0
	for i := 0; i < len(src); i = i + 2 {
		b, err := strconv.ParseInt(src[i:i+2], 16, 10)
		if err != nil {
			return nil, err
		}
		ba[idx] = byte(b)
		idx++
	}

	return ba, nil
}

//데이터베이스 default 값 초기화
func InitDatabaseColumn(columnTypes []*sql.ColumnType, length int) []interface{} {
	scanArgs := make([]interface{}, length)

	for i, v := range columnTypes {

		switch v.DatabaseTypeName() {
		case "VARCHAR", "TEXT", "UUID", "TIMESTAMP":
			scanArgs[i] = new(sql.NullString)
			break
		case "BOOL":
			scanArgs[i] = new(sql.NullBool)
			break
		case "INT4":
			scanArgs[i] = new(sql.NullInt64)
			break
		default:
			scanArgs[i] = new(sql.NullString)
		}
	}

	return scanArgs
}