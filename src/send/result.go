package send

import (
	"fmt"
	"context"
	"database/sql"

	"ap/src/dbpool"
	cf "ap/src/config"
	cm "ap/src/common"

	s "strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func AppResultProcess(ctx context.Context) {
	var wg sync.WaitGroup

	for {
		select {
			case <- ctx.Done():
		
		    cf.Stdlog.Println("AppPush result process가 10초 후에 종료 됨.")
		    time.Sleep(10 * time.Second)
		    cf.Stdlog.Println("AppPush result process 종료 완료")
		    return
		default:
			wg.Add(1)
			go resultProcess(&wg)
			wg.Wait()
		}
	}
}

func resultProcess(wg *sync.WaitGroup){
	defer wg.Done()
	defer func(){
		if r := recover(); r != nil {
			cf.Stdlog.Println("AppPush resultProcess panic 발생 원인 : ", r)
			if err, ok := r.(error); ok {
				if s.Contains(err.Error(), "connection refused") {
					for {
						cf.Stdlog.Println("AppPush resultProcess send ping to DB")
						err := dbpool.DB.Ping()
						if err == nil {
							break
						}
						time.Sleep(10 * time.Second)
					}
				}
			}
		}
	}()

	var db = dbpool.DB
	var errlog = cf.Stdlog

	var sucIds []string
	var failIds []string

	var isProc = true

	var groupQuery = "select queueidx, msgidx, step, error_code, resultdate, extra2 from " + cf.Conf.APP_RESPONSE_TABLE + " a where extra3 = '' and step in ('C', 'F')"

	groupRows, err := db.Query(groupQuery)
	if err != nil {
		errcode := err.Error()
		errlog.Println("AppPush ", cf.Conf.APP_RESPONSE_TABLE," 조회 중 오류 발생", groupQuery, errcode)

		time.Sleep(10 * time.Second)
		isProc = false
		return
	}
	defer groupRows.Close()

	if isProc {
		for groupRows.Next() {
			var queueidx, msgidx, step, error_code, resultdate, extra2 sql.NullString
			resCode := ""

			groupRows.Scan(&queueidx, &msgidx, &step, &error_code, &resultdate, &extra2)

			if s.EqualFold(step.String, "C") {
				sucIds = append(sucIds, extra2.String)
			} else if (s.EqualFold(step.String, "F")){
				failIds = append(failIds, extra2.String)
				resCode = error_code.String
			}
			db.Exec("update DHN_REQUEST_APP set message_type = 'at', remark4 = '" + resultdate.String + "', remark5 = '" + resCode + "' where msgid = '" + extra2.String + "'")
			db.Exec("update " + cf.Conf.APP_RESPONSE_TABLE + " set extra3 = 'Y' where queueidx = '" + queueidx.String + "'")

			if len(sucIds) >= 500 {
				insRes(sucIds)
				sucIds = []string{}
			}

			if len(failIds) >= 500 {
				insAt(failIds)
				failIds = []string{}
			}

		}

		if len(sucIds) > 0 {
			insRes(sucIds)
		}

		if len(failIds) > 0 {
			insAt(failIds)
		}
	}
}

func insAt(ids []string){
	msgids := "'" + s.Join(ids, "', '")	+ "'"
	atColumn := cm.GetReqAtColumn2()
	atColumnStr := s.Join(atColumn, ",")
	sql := `
		insert into DHN_REQUEST_AT(
			` + atColumnStr + `
		)
		select
			` + atColumnStr + `
		from DHN_REQUEST_APP
		where msgid in (` + msgids + `)
	`
	dbpool.DB.Exec(sql)
}

func insRes(ids []string){
	msgids := "'" + s.Join(ids, "', '")	+ "'"

	var resdt = time.Now()
	var resdtstr = fmt.Sprintf("%4d-%02d-%02d %02d:%02d:%02d", resdt.Year(), resdt.Month(), resdt.Day(), resdt.Hour(), resdt.Minute(), resdt.Second())

	sql := `
		insert into DHN_RESULT(
			msgid,
			userid,
			ad_flag,
			button1,
			button2,
			button3,
			button4,
			button5,
			code,
			image_link,
			image_url,
			message_type,
			msg,
			msg_sms,
			only_sms,
			p_com,
			p_invoice,
			phn,
			profile,
			reg_dt,
			remark1,
			remark2,
			remark3,
			remark4,
			remark5,
			res_dt,
			reserve_dt,
			s_code,
			result,
			sms_kind,
			sms_lms_tit,
			sms_sender,
			sync,
			tmpl_id,
			wide,
			send_group,
			supplement,
			price,
			currency_type,
			title,
			header,
			carousel,
			attachments
		)
		select 
			msgid,
			userid,
			ad_flag,
			button1,
			button2,
			button3,
			button4,
			button5,
			'0000',
			image_link,
			image_url,
			'ap',
			msg,
			msg_sms,
			only_sms,
			p_com,
			p_invoice,
			phn,
			profile,
			reg_dt,
			remark1,
			'` + resdtstr + `',
			remark3,
			remark4,
			remark5,
			now(),
			reserve_dt,
			s_code,
			'Y',
			sms_kind,
			sms_lms_tit,
			sms_sender,
			'N',
			tmpl_id,
			wide,
			null,
			supplement,
			price,
			currency_type,
			title,
			header,
			carousel,
			attachments
		from DHN_REQUEST_APP
		where msgid in (` + msgids + `)
	`
	dbpool.DB.Exec(sql)
}

















