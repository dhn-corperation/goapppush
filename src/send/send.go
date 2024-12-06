package send

import(
	"fmt"
	"time"
	"context"
	"strconv"
	"database/sql"
	s "strings"

	"ap/src/dbpool"
	cf "ap/src/config"
	cm "ap/src/common"
)

func AppProc(user_id string, ctx context.Context) {
	procCnt := 0
	cf.Stdlog.Println(user_id, " - AppPush Process 시작 됨.") 
	for {
		if procCnt < 5 {
			
			select {
			case <- ctx.Done():
			    cf.Stdlog.Println(user_id, " - AppPush process가 10초 후에 종료 됨.")
			    time.Sleep(10 * time.Second)
			    cf.Stdlog.Println(user_id, " - AppPush process 종료 완료")
			    return
			default:
				var count sql.NullInt64
				cnterr := dbpool.DB.QueryRowContext(ctx, "SELECT count(1) AS cnt FROM DHN_REQUEST_APP WHERE send_group IS NULL AND IFNULL(reserve_dt,'00000000000000') <= DATE_FORMAT(NOW(), '%Y%m%d%H%i%S') AND userid=? limit 1", user_id).Scan(&count)
				
				if cnterr != nil && cnterr != sql.ErrNoRows {
					cf.Stdlog.Println(user_id, " - AppPush DHN_REQUEST Table - select error : " + cnterr.Error())
					time.Sleep(10 * time.Second)
				} else {
					if count.Valid && count.Int64 > 0 {		
						var startNow = time.Now()
						var group_no = fmt.Sprintf("%02d%02d%02d%09d", startNow.Hour(), startNow.Minute(), startNow.Second(), startNow.Nanosecond())
						
						updateRows, err := dbpool.DB.ExecContext(ctx, "update DHN_REQUEST_APP set send_group = ? where send_group is null and ifnull(reserve_dt,'00000000000000') <= date_format(now(), '%Y%m%d%H%i%S') and userid = ?  limit ?", group_no, user_id, strconv.Itoa(cf.Conf.SENDLIMIT))
				
						if err != nil {
							cf.Stdlog.Println(user_id," - AppPush send_group Update error : ", err, " / group_no : ", group_no)
						}
				
						rowcnt, _ := updateRows.RowsAffected()
				
						if rowcnt > 0 {
							procCnt++
							cf.Stdlog.Println(user_id, " - AppPush 발송 처리 시작 ( ", group_no, " ) : ", rowcnt, " 건 ( Proc Cnt :", procCnt, ") - START")
							go func() {
								defer func() {
									procCnt--
								}()
								apsendProcess(group_no, user_id, procCnt)
							}()
						}
					}
				}
			}
		}
	}

}

func apsendProcess(group_no, user_id string, pc int) {
	defer func(){
		if r := recover(); r != nil {
			cf.Stdlog.Println(user_id, " - apsendProcess panic error : ", r, " / group_no : ", group_no, " / userid  : ", user_id)
			if err, ok := r.(error); ok {
				if s.Contains(err.Error(), "connection refused") {
					for {
						cf.Stdlog.Println(user_id, " - apsendProcess send ping to DB / group_no : ", group_no, " / userid  : ", user_id)
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
	var stdlog = cf.Stdlog
	var errlog = cf.Stdlog

	reqsql := "select * from DHN_REQUEST_APP where send_group = '" + group_no + "' and userid = '" + user_id + "'"

	reqrows, err := db.Query(reqsql)
	if err != nil {
		errlog.Println(user_id, " - apsendProcess select error : ", err, " / group_no : ", group_no, " / userid  : ", user_id," / query : ", reqsql)
		panic(err)
	}

	columnTypes, err := reqrows.ColumnTypes()
	if err != nil {
		errlog.Println(user_id, " - apsendProcess column init error : ", err, " / group_no : ", group_no, " / userid  : ", user_id)
		time.Sleep(5 * time.Second)
	}

	count := len(columnTypes)
	initScanArgs := cm.InitDatabaseColumn(columnTypes, count)

	var procCount int
	procCount = 0

	resinsStrs := []string{}
	resinsValues := []interface{}{}
	resinsquery := `insert IGNORE into ` + cf.Conf.APP_REQUEST_TABLE + `(appkey, appsecret, senddate, msgtitle, msgcontents, identify, link, custom_value_1, extra2) values %s`

	for reqrows.Next() {
		scanArgs := initScanArgs

		err := reqrows.Scan(scanArgs...)
		if err != nil {
			errlog.Println(user_id, " - apsendProcess column scan error : ", err, " / group_no : ", group_no)
			time.Sleep(5 * time.Second)
		}

		result := map[string]string{}

		for i, v := range columnTypes {
			if z, ok := (scanArgs[i]).(*sql.NullString); ok {
				result[s.ToLower(v.Name())] = z.String
			}

			if z, ok := (scanArgs[i]).(*sql.NullInt32); ok {
				result[s.ToLower(v.Name())] = string(z.Int32)
			}

			if z, ok := (scanArgs[i]).(*sql.NullInt64); ok {
				result[s.ToLower(v.Name())] = string(z.Int64)
			}
		}

		now := time.Now().Format("2006-01-02 15:04:05")
		resinsStrs = append(resinsStrs, "(?,?,?,?,?,?,?,?,?)")
		resinsValues = append(resinsValues, result["app_key"])
		resinsValues = append(resinsValues, result["app_secret"])
		resinsValues = append(resinsValues, now)
		if result["sms_lms_tit"] != "" {
			resinsValues = append(resinsValues, result["sms_lms_tit"])
		} else {
			resinsValues = append(resinsValues, "")
		}
		resinsValues = append(resinsValues, result["msg"])
		resinsValues = append(resinsValues, result["push_id"])
		resinsValues = append(resinsValues, result["app_link"])
		resinsValues = append(resinsValues, result["app_launch"])
		resinsValues = append(resinsValues, result["msgid"])

		if len(resinsStrs) >= 500 {
			resinsStrs, resinsValues = cm.InsMsg(resinsquery, resinsStrs, resinsValues)
		}

		procCount++
	}

	//Center에서도 사용하고 있는 함수이므로 공용 라이브러리 생성이 필요함
	if len(resinsStrs) > 0 {
		resinsStrs, resinsValues = cm.InsMsg(resinsquery, resinsStrs, resinsValues)
	}

	//알림톡 발송 후 DHN_REQUEST_AT 테이블의 데이터는 제거한다.
	
	stdlog.Println(user_id, " - AppPush 발송 처리 완료 ( ", group_no, " ) : ", procCount, " 건 ( Proc Cnt :", pc, ") - END")
	
}