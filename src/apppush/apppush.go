package main

import(
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"database/sql"
	"log"
	"time"
	"context"
	"crypto/tls"
	"runtime/debug"

	"ap/src/dbpool"
	"ap/src/reqrec"
	"ap/src/send"
	cf "ap/src/config"

	"github.com/takama/daemon"
	"github.com/fasthttp/router"
	"github.com/valyala/fasthttp"
	"github.com/caddyserver/certmagic"
)

const (
	name        = "DHNApppush"
	description = "대형네트웍스 앱푸시 에이전트"
	certEmail   = "dhn@dhncorp.co.kr"
)

var dependencies = []string{name+".service"}

type Service struct {
	daemon.Daemon
}

func (service *Service) Manage() (string, error) {

	usage := "Usage: "+name+" install | remove | start | stop | status"

	if len(os.Args) > 1 {
		command := os.Args[1]
		switch command {
		case "install":
			return service.Install()
		case "remove":
			return service.Remove()
		case "start":
			return service.Start()
		case "stop":
			return service.Stop()
		case "status":
			return service.Status()
		default:
			return usage, nil
		}
	}
	resultProc()
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, os.Kill, syscall.SIGTERM)

	for {
		select {
		case killSignal := <-interrupt:
			cf.Stdlog.Println("Got signal:", killSignal)
			cf.Stdlog.Println("Stoping DB Conntion : ", dbpool.DB.Stats())
			defer dbpool.DB.Close()
			if killSignal == os.Interrupt {
				return "Daemon was interrupted by system signal", nil
			}
			return "Daemon was killed", nil
		}
	}
}

func main(){
	cf.InitConfig()
	dbpool.InitDatabase()

	var rLimit syscall.Rlimit

	rLimit.Max = 50000
	rLimit.Cur = 50000

	err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)

	if err != nil {
		cf.Stdlog.Println("Error Setting Rlimit ", err)
	}

	err = syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)

	if err != nil {
		cf.Stdlog.Println("Error Getting Rlimit ", err)
	}

	cf.Stdlog.Println("Rlimit Final", rLimit)

	srv, err := daemon.New(name, description, daemon.SystemDaemon, dependencies...)
	if err != nil {
		cf.Stdlog.Println("Error: ", err)
		os.Exit(1)
	}

	service := &Service{srv}
	status, err := service.Manage()
	if err != nil {
		cf.Stdlog.Println(status, "\nError: ", err)
		os.Exit(1)
	}

	fmt.Println(status)
}

func resultProc(){
	cf.Stdlog.Println(name+" 시작")

	//모든 서비스
	allService := map[string]string{}
	allCtxC := map[string]interface{}{}

	ap_user_list, error := dbpool.DB.Query("select distinct user_id from DHN_CLIENT_LIST where use_flag = 'Y' and app_push='Y'")
	isAp := true
	if error != nil {
		cf.Stdlog.Println("앱 푸시 유저 select 오류 ")
		isAp = false
	}
	defer ap_user_list.Close()

	apUser := map[string]string{}
	apCtxC := map[string]interface{}{}

	if isAp {
		var user_id sql.NullString
		for ap_user_list.Next() {

			ap_user_list.Scan(&user_id)

			ctx, cancel := context.WithCancel(context.Background())
			go send.AppProc(user_id.String, ctx)

			apCtxC[user_id.String] = cancel
			apUser[user_id.String] = user_id.String

			allCtxC["AP"+user_id.String] = cancel
			allService["AP"+user_id.String] = user_id.String

		}
	}

	apctx, _ := context.WithCancel(context.Background())
	go send.AppResultProcess(apctx)

	r := router.New()

	r.GET("/", func(c *fasthttp.RequestCtx) {
		c.SetStatusCode(fasthttp.StatusOK)
		c.SetBodyString("test")
	})

	r.POST("/req", statusDatabaseMaddleware(reqrec.Request))

	topLevelHandler := recoveryMiddleware(r.Handler)

	if cf.Conf.SSL_FLAG == "Y" {
		//SSL 시작
		certmagic.DefaultACME.Agreed = true
		certmagic.DefaultACME.Email = certEmail
		certmagic.DefaultACME.CA = certmagic.LetsEncryptProductionCA

		err := certmagic.ManageSync(context.TODO(), []string{cf.Conf.DNS})

		if err != nil {
			cf.Stdlog.Println("certmagic.ManageSync 에러 : ", err)
			log.Fatal("certmagic.ManageSync 에러 : ", err)
		} else {
			cf.Stdlog.Println("certmagic.ManageSync 성공 인증서 자동갱신 시작")
		}

		certmagicCfg := certmagic.NewDefault()
		tlsConfig := certmagicCfg.TLSConfig()

		tlsConfig.MinVersion = tls.VersionTLS12

		tlsConfig.CipherSuites = []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		}

		tlsConfig.NextProtos = []string{"h2", "http/1.1"}


		server := &fasthttp.Server{
			Handler: topLevelHandler,
			MaxRequestBodySize: 10 * 1024 * 1024,
			TLSConfig: tlsConfig,
		}

		if err := server.ListenAndServeTLS(":" + cf.Conf.SSL_PORT, "", ""); err != nil {
			cf.Stdlog.Println("fasthttp 443포트 실행 실패")
			log.Fatal("fasthttp 443포트 실행 실패")
		}
		//SSL 끝
	} else {
		//HTTP 시작
		if err := fasthttp.ListenAndServe(":" + cf.Conf.PORT, topLevelHandler); err != nil {
			cf.Stdlog.Println("fasthttp 실행 실패")
		}
		//HTTP 끝
	}
}

func recoveryMiddleware(next fasthttp.RequestHandler) fasthttp.RequestHandler {
    return func(c *fasthttp.RequestCtx) {
        defer func() {
            if r := recover(); r != nil {
                // panic 로그 기록
                cf.Stdlog.Println("Recovered from panic : ", r)
                cf.Stdlog.Println("Stack trace: ", string(debug.Stack()))
            }
        }()
        next(c) // 다음 미들웨어 또는 핸들러로 넘김
    }
}

func statusDatabaseMaddleware(next fasthttp.RequestHandler) fasthttp.RequestHandler {
	return func(c *fasthttp.RequestCtx){
		if err := dbpool.DB.Ping(); err != nil {
			cf.Stdlog.Println("DB 핑 신호 없음 err : ", err)
			for {
				if err := dbpool.DB.Ping(); err != nil {
					cf.Stdlog.Println("DB 할당 중")
					time.Sleep(10 * time.Second) // 10초 후 재시도
					continue
				} else {
					cf.Stdlog.Println("DB 할당 완료")
					break
				}
			}
		}
		next(c)
	}
}