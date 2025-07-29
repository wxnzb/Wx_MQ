package raft

import (
	"fmt"
	"log"
	"os"
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
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

func DEBUG(topic, format string, args ...interface{}) {
	mu.Lock()
	defer mu.Unlock()
	//这里的时间是怎样进行转换的
	// time := time.Since(logstart).Microseconds()
	// time = time / 100
	// prefix := fmt.Sprintf("%6d %v", time, topic)
	prefix := fmt.Sprintf("%v ", string(topic))
	format = prefix + format
	fmt.Printf(format, args...)

}
