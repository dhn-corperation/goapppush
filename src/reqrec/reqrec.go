package reqrec

import(
	"fmt"
	"time"
	"strconv"
	s "strings"


	"ap/src/dbpool"
	"ap/src/tables"
	cf "ap/src/config"
	cm "ap/src/common"

	"github.com/valyala/fasthttp"
	"github.com/goccy/go-json"
)

var SecretKey = "9b4dabe9d4fed126a58f8639846143c7"

func Request(c *fasthttp.RequestCtx){
	errlog := cf.Stdlog

	atColumn := cm.GetReqAtColumn()
	atColumnStr := s.Join(atColumn, ",")

	userid := string(c.Request.Header.Peek("userid"))
	userip := c.RemoteIP().String()
	isValidation := false

	// 허가된 userid 인지 테이블에서 확인
	sqlstr := `
		select 
			count(1) as cnt 
		from
			DHN_CLIENT_LIST
		where
			user_id = ?
			and ip = ?
			and use_flag = 'Y'`

	var cnt int
	err := dbpool.DB.QueryRow(sqlstr, userid, userip).Scan(&cnt)
	if err != nil { errlog.Println(err) }

	if cnt > 0 { 
		isValidation = true 
	} else {
		errlog.Println("허용되지 않은 사용자 및 아이피에서 발송 요청!! (userid : ", userid, "/ ip : ", userip, ")")
	}

	var startNow = time.Now()
	var startTime = fmt.Sprintf("%02d:%02d:%02d", startNow.Hour(), startNow.Minute(), startNow.Second())

	if isValidation {
		var msg []tables.Reqtable

		if err1 := json.Unmarshal(c.PostBody(), &msg); err1 != nil {
			errlog.Println(err1)
			res, _ := json.Marshal(map[string]string{
				"code":    "01",
				"message": "데이터 맵핑 실패",
			})
			c.SetContentType("application/json")
			c.SetStatusCode(fasthttp.StatusBadRequest)
			c.SetBody(res)
			return
		}

		errlog.Println("앱 푸쉬 수신 시작 ( ", userid, ") : ", len(msg), startTime)

		atreqinsStrs := []string{}
		//알림톡 value interface 배열 생성
		atreqinsValues := []interface{}{}

		atreqinsQuery := `insert IGNORE into DHN_REQUEST_APP(`+atColumnStr+`) values %s`

		atQmarkStr := cm.GetQuestionMark(atColumn)

		//맵핑한 데이터 row 처리
		for i, _ := range msg {

			var nonce string
			if len(msg[i].Crypto) > 0 {
				nonce = s.Split(msg[i].Crypto, ",")[0]
			}

			// var processedMsg string
			// var err error
			var smsKind = msg[i].Smskind
			// if s.Contains(s.ToLower(msg[i].Crypto), "msg") && len(msg[i].Msgsms) > 0 {
			// 	processedMsg, err = cm.RemoveWs(cm.AES256GSMDecrypt([]byte(SecretKey), msg[i].Msgsms, nonce))
			// } else {
			// 	processedMsg, err = cm.RemoveWs(msg[i].Msgsms)
			// }
			// if err != nil {
			// 	errlog.Println("RemoveWs 에러 : ", err)
			// } else {
			// 	euckrLength, err := cm.LengthInEUCKR(processedMsg)
			// 	if err != nil {
			// 		errlog.Println("LengthInEUCKR 에러 : ", err)
			// 	}
			// 	if euckrLength <= 90 {
			// 		smsKind = "S"
			// 	} else if euckrLength > 90 && msg[i].Pinvoice == "" {
			// 		smsKind = "L"
			// 	} else {
			// 		smsKind = "M"
			// 	}
			// }

			//알림톡 insert values 만들기
			if s.HasPrefix(s.ToUpper(msg[i].Messagetype), "AP") {
				atreqinsStrs = append(atreqinsStrs, "("+atQmarkStr+")")
				atreqinsValues = append(atreqinsValues, msg[i].Msgid)
				atreqinsValues = append(atreqinsValues, userid)
				atreqinsValues = append(atreqinsValues, msg[i].Adflag)
				atreqinsValues = append(atreqinsValues, msg[i].Button1)
				atreqinsValues = append(atreqinsValues, msg[i].Button2)
				atreqinsValues = append(atreqinsValues, msg[i].Button3)
				atreqinsValues = append(atreqinsValues, msg[i].Button4)
				atreqinsValues = append(atreqinsValues, msg[i].Button5)
				atreqinsValues = append(atreqinsValues, msg[i].Imagelink)
				atreqinsValues = append(atreqinsValues, msg[i].Imageurl)
				atreqinsValues = append(atreqinsValues, msg[i].Messagetype)
				if s.Contains(s.ToLower(msg[i].Crypto), "msg") {
					atreqinsValues = append(atreqinsValues, cm.AES256GSMDecrypt([]byte(SecretKey), msg[i].Msg, nonce))
				} else {
					atreqinsValues = append(atreqinsValues, msg[i].Msg)
				}

				if s.Contains(s.ToLower(msg[i].Crypto), "msg") && len(msg[i].Msgsms) > 0 {
					atreqinsValues = append(atreqinsValues, cm.AES256GSMDecrypt([]byte(SecretKey), msg[i].Msgsms, nonce))
				} else {
					atreqinsValues = append(atreqinsValues, msg[i].Msgsms)
				}
				atreqinsValues = append(atreqinsValues, msg[i].Onlysms)
				if s.Contains(s.ToLower(msg[i].Crypto), "phn") && msg[i].Phn != "" {
					atreqinsValues = append(atreqinsValues, cm.AES256GSMDecrypt([]byte(SecretKey), msg[i].Phn, nonce))
				} else {
					atreqinsValues = append(atreqinsValues, msg[i].Phn)
				}
				if s.Contains(s.ToLower(msg[i].Crypto), "profile") && len(msg[i].Profile) > 0 {
					atreqinsValues = append(atreqinsValues, cm.AES256GSMDecrypt([]byte(SecretKey), msg[i].Profile, nonce))
				} else {
					atreqinsValues = append(atreqinsValues, msg[i].Profile)
				}
				atreqinsValues = append(atreqinsValues, msg[i].Pcom)
				atreqinsValues = append(atreqinsValues, msg[i].Pinvoice)
				atreqinsValues = append(atreqinsValues, msg[i].Regdt)
				atreqinsValues = append(atreqinsValues, msg[i].Remark1)
				atreqinsValues = append(atreqinsValues, msg[i].Remark2)
				atreqinsValues = append(atreqinsValues, msg[i].Remark3)
				atreqinsValues = append(atreqinsValues, msg[i].Remark4)
				atreqinsValues = append(atreqinsValues, msg[i].Remark5)
				atreqinsValues = append(atreqinsValues, msg[i].Reservedt)
				atreqinsValues = append(atreqinsValues, smsKind)
				if s.Contains(s.ToLower(msg[i].Crypto), "smslmstit") && len(msg[i].Smslmstit) > 0 {
					atreqinsValues = append(atreqinsValues, cm.AES256GSMDecrypt([]byte(SecretKey), msg[i].Smslmstit, nonce))
				} else {
					atreqinsValues = append(atreqinsValues, msg[i].Smslmstit)
				}

				if s.Contains(s.ToLower(msg[i].Crypto), "smssender") && len(msg[i].Smssender) > 0 {
					atreqinsValues = append(atreqinsValues, cm.AES256GSMDecrypt([]byte(SecretKey), msg[i].Smssender, nonce))
				} else {
					atreqinsValues = append(atreqinsValues, msg[i].Smssender)
				}
				atreqinsValues = append(atreqinsValues, msg[i].Scode)
				atreqinsValues = append(atreqinsValues, msg[i].Tmplid)
				atreqinsValues = append(atreqinsValues, msg[i].Wide)
				atreqinsValues = append(atreqinsValues, nil) //send_group
				atreqinsValues = append(atreqinsValues, msg[i].Supplement)

				if len(msg[i].Price) > 0 {
					price, _ := strconv.Atoi(msg[i].Price)
					atreqinsValues = append(atreqinsValues, price)
				} else {
					atreqinsValues = append(atreqinsValues, nil)
				}

				atreqinsValues = append(atreqinsValues, msg[i].Currencytype)
				atreqinsValues = append(atreqinsValues, msg[i].Title)
				atreqinsValues = append(atreqinsValues, msg[i].MmsImageId)
				// atreqinsValues = append(atreqinsValues, msg[i].Header)
				// atreqinsValues = append(atreqinsValues, msg[i].Carousel)
				atreqinsValues = append(atreqinsValues, msg[i].Pushid)
				atreqinsValues = append(atreqinsValues, msg[i].Appkey)
				atreqinsValues = append(atreqinsValues, msg[i].Appsecret)
				atreqinsValues = append(atreqinsValues, msg[i].Applink)
				atreqinsValues = append(atreqinsValues, msg[i].Applaunch)
				atreqinsValues = append(atreqinsValues, msg[i].Atchfilesn)
			}

			// 500건 단위로 처리한다(클라이언트에서 1000건씩 전송하더라도 지정한 단위의 건수로 insert한다.)
			saveCount := 500
			if len(atreqinsStrs) >= saveCount {
				atreqinsStrs, atreqinsValues = cm.InsMsg(atreqinsQuery, atreqinsStrs, atreqinsValues)
			}
		}
		
		// 나머지 건수를 저장하기 위해 다시한번 정의
		if len(atreqinsStrs) > 0 {
			atreqinsStrs, atreqinsValues = cm.InsMsg(atreqinsQuery, atreqinsStrs, atreqinsValues)
		}

		errlog.Println("발송 메세지 수신 끝 ( ", userid, ") : ", len(msg), startTime)

		res, _ := json.Marshal(map[string]string{
			"code": "00",
			"message": "발송 요청이 완료되었습니다.",
		})

		c.SetContentType("application/json")
		c.SetStatusCode(fasthttp.StatusOK)
		c.SetBody(res)

	} else {
		res, _ := json.Marshal(map[string]string{
			"code":    "01",
			"message": "허용되지 않은 사용자 입니다 / userid : " + userid + " / ip : " + userip,
		})
		c.SetContentType("application/json")
		c.SetStatusCode(fasthttp.StatusNotAcceptable)
		c.SetBody(res)
	}
}