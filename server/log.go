// package server

// import (
// 	"sync/atomic"
// )

// type Logger interface {
// 	//调试
// 	Debugf(format string, args ...interface{})
// 	//追踪
// 	Tracef(format string, args ...interface{})
// 	//正常信息
// 	Noticef(format string, args ...interface{})
// 	//警告
// 	Warnf(format string, args ...interface{})
// 	//错误
// 	Errorf(format string, args ...interface{})
// 	//致命错误
// 	Fatalf(format string, args ...interface{})
// }

// func (s *RPCServer) executeLogCall(f func(logger Logger, format string, args ...interface{}), format string, args ...interface{}) {
// 	s.logging.RLock()
// 	defer s.logging.RUnlock()
// 	if s.logging.logger == nil {
// 		return
// 	}
// 	f(s.logging.logger, format, args)
// }

// // 实现关于RPCServer的Logger接口
// // 调试
// func (s *RPCServer) Debugf(format string, args ...interface{}) {
// 	if atomic.LoadInt32(&s.logging.debug) == 0 {
// 		return
// 	}
// 	//为啥这个不能直接写成s.logging.Logger.Debugf(format,args)
// 	s.executeLogCall(func(logger Logger, format string, args ...interface{}) {
// 		logger.Debugf(format, args...)
// 	}, format, args...)
// }

// // 追踪
// func (s *RPCServer) Tracef(format string, args ...interface{}) {
// 	if atomic.LoadInt32(&s.logging.trace) == 0 {
// 		return
// 	}
// 	s.executeLogCall(func(logger Logger, format string, args ...interface{}) {
// 		logger.Tracef(format, args...)
// 	}, format, args...)
// }

// // 正常信息
// func (s *RPCServer) Noticef(format string, args ...interface{}) {
// 	s.executeLogCall(func(logger Logger, format string, args ...interface{}) {
// 		logger.Noticef(format, args...)
// 	}, format, args...)
// }

// // 警告
// func (s *RPCServer) Warnf(format string, args ...interface{}) {
// 	s.executeLogCall(func(logger Logger, format string, args ...interface{}) {
// 		logger.Warnf(format, args...)
// 	}, format, args...)
// }

// // 错误
// func (s *RPCServer) Errorf(format string, args ...interface{}) {
// 	s.executeLogCall(func(logger Logger, format string, args ...interface{}) {
// 		logger.Errorf(format, args...)
// 	}, format, args...)
// }

// // 致命错误
// func (s *RPCServer) Fatalf(format string, args ...interface{}) {
// 	s.executeLogCall(func(logger Logger, format string, args ...interface{}) {
// 		logger.Fatalf(format, args...)
// 	}, format, args...)
// }

// // 现在写这个感觉没用上
//
//	func (s *RPCServer) Logger() Logger {
//		s.logging.RLock()
//		defer s.logging.RUnlock()
//		return s.logging.logger
//	}
//
// 咱就是说，完全一个大变样～
package server

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

const (
	dERROR string = "ERROR"
	dLOG   string = "LOG"
)

func DEBUG(topic, format string, args ...interface{}) {
	mu.Lock()
	defer mu.Unlock()
	//这里的时间是怎样进行转换的
	time := time.Since(logstart).Microseconds()
	time = time / 100
	prefix := fmt.Sprintf("%6d %v", time, topic)
	format = prefix + format
	fmt.Printf(format, args...)

}
