package cf

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	ini "github.com/BurntSushi/toml"
	rotatelogs "github.com/lestrrat/go-file-rotatelogs"
)

type Config struct {
	DNS                string
	SSL_FLAG           string
	SSL_PORT		   string
	DB                 string
	DBURL              string
	PORT      		   string
	APP_REQUEST_TABLE  string
	APP_RESPONSE_TABLE string
	SENDLIMIT          int
}

var Conf Config
var Stdlog *log.Logger
var BasePath string
var IsRunning bool = true
var ResultLimit int = 1000

func InitConfig() {
	realpath, _ := os.Executable()
	dir := filepath.Dir(realpath)
	logDir := filepath.Join(dir, "logs")
	err := createDir(logDir)
	if err != nil {
		log.Fatalf("Failed to ensure log directory: %s", err)
	}
	path := filepath.Join(logDir, "DHNApppush")
	loc, _ := time.LoadLocation("Asia/Seoul")
	writer, err := rotatelogs.New(
		fmt.Sprintf("%s-%s.log", path, "%Y-%m-%d"),
		rotatelogs.WithLocation(loc),
		rotatelogs.WithMaxAge(-1),
		rotatelogs.WithRotationCount(7),
	)

	if err != nil {
		log.Fatalf("Failed to Initialize Log File %s", err)
	}

	log.SetOutput(writer)
	stdlog := log.New(os.Stdout, "INFO -> ", log.Ldate|log.Ltime)
	stdlog.SetOutput(writer)
	Stdlog = stdlog

	Conf = readConfig()
	BasePath = dir + "/"

}

func readConfig() Config {
	realpath, _ := os.Executable()
	dir := filepath.Dir(realpath)
	var configfile = filepath.Join(dir, "config.ini")
	_, err := os.Stat(configfile)
	if err != nil {

		err := createConfig(configfile)
		if err != nil {
			Stdlog.Println("Config file create fail")
		}
		Stdlog.Println("config.ini 생성완료 작성을 해주세요.")

		system_exit("DHNApppush")
		fmt.Println("Config file is missing : ", configfile)
	}

	var result Config
	_, err1 := ini.DecodeFile(configfile, &result)

	if err1 != nil {
		fmt.Println("Config file read error : ", err1)
	}

	return result
}

func createDir(dirName string) error {
	err := os.MkdirAll(dirName, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}
	return nil
}

func createConfig(dirName string) error {
	fo, err := os.Create(dirName)
	if err != nil {
		return fmt.Errorf("Config file create fail: %w", err)
	}
	configData := []string{
		`# DB 관련`,
		`DB = "DB종류"`,
		`DBURL = "사용자:패스워드@tcp(000.000.000.000:포트번호)/데이터베이스"`,
	}

	for _, line := range configData {
		fmt.Fprintln(fo, line)
	}

	return nil
}

func system_exit(service_name string) {
	cmd := exec.Command("systemctl", "stop", service_name)
	if err := cmd.Run(); err != nil {
		Stdlog.Println(service_name+" 서비스 종료 실패:", err)
	} else {
		Stdlog.Println(service_name + " 서비스가 성공적으로 종료되었습니다.")
	}
}
