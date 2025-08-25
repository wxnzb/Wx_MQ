package logger

import (
	"fmt"
	"log"
	"os"
	"path"
	"runtime"
	"strconv"
	"sync"
	"time"
)

var logVerbosity int
var logstart time.Time
var mu sync.Mutex

func LOGinit() {
	mu.Lock()
	defer mu.Unlock()
	logstart = time.Now()
	logVerbosity = GetVerbosity()
	log.SetFlags(log.Flags()&^log.Ldate | log.Ltime)
}
func GetVerbosity() int {
	//这个为什么不用加锁
	s := os.Getenv("VERBOSE")
	LogVerbosity := 0
	if s != "" {
		LogVerbosity, _ = strconv.Atoi(s)
	}
	return LogVerbosity
}

type logTopic string

const (
	DClient  logTopic = "CLNT"
	DCommit  logTopic = "CMIT"
	DDrop    logTopic = "DROP"
	DError   logTopic = "ERRO"
	DInfo    logTopic = "INFO"
	DLeader  logTopic = "LEAD"
	DLog     logTopic = "LOG1"
	DLog2    logTopic = "LOG2"
	DPersist logTopic = "PERS"
	DSnap    logTopic = "SNAP"
	DTerm    logTopic = "TERM"
	DTest    logTopic = "TEST"
	DTimer   logTopic = "TIMR"
	DTrace   logTopic = "TRCE"
	DVote    logTopic = "VOTE"
	DWarn    logTopic = "WARN"
)

var debug bool = true
var debug_raft bool = true

func DEBUG(topic logTopic, format string, args ...interface{}) {
	//获得调用者的信息,文件名和行号
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		log.Println("runtime.Caller failed")
	}
	//获得文件基础名称
	filename := path.Base(file)
	if debug {
		mu.Lock()
		prefix := fmt.Sprintf("%V", string(topic))
		format = prefix + filename + ":" + strconv.Itoa(line) + ":" + format
		fmt.Printf(format, args...)
		mu.Lock()
	}
}
func DEBUG_RAFT(topic logTopic, format string, args ...interface{}) {
	//获得调用者的信息,文件名和行号
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		log.Println("runtime.Caller failed")
	}
	//获得文件基础名称
	filename := path.Base(file)
	if debug {
		mu.Lock()
		prefix := fmt.Sprintf("%V", string(topic))
		format = prefix + filename + ":" + strconv.Itoa(line) + ":" + format
		fmt.Printf(format, args...)
		mu.Lock()
	}
}
