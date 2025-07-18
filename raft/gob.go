package raft

import (
	"encoding/gob"
	"io"
	"reflect"
	"sync"
	"unicode"
	"unicode/utf8"
)

type LabEncoder struct {
	gob *gob.Encoder
}

func NewEncoder(w io.Writer) *LabEncoder {
	return &LabEncoder{gob: gob.NewEncoder(w)}
}
func (e *LabEncoder) Encode(v interface{}) error {
	checkValue(v)
	return e.gob.Encode(v)
}
func checkValue(v interface{}) {
	checkType(reflect.TypeOf(v))
}

var errCnt int
var rmu sync.RWMutex
var checked map[reflect.Type]bool

func checkType(t reflect.Type) {
	if checked == nil {
		checked = map[reflect.Type]bool{}
	}
	if checked[t] {
		return
	}
	rmu.Lock()
	checked[t] = true
	rmu.Unlock()
	k := t.Kind()
	switch k {
	//要是结构体类型，需要检查收字段是否大写，没有符合要求记下来
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			//获取这个元素首字母
			rune, _ := utf8.DecodeRuneInString(f.Name)
			if unicode.IsUpper(rune) == false {
				errCnt++
			}
			checkType(f.Type)
		}
		return
	case reflect.Ptr, reflect.Array, reflect.Slice:
		checkType(t.Elem())
		return
	case reflect.Map:
		checkType(t.Elem())
		checkType(t.Key())
		return
	default:
		return

	}
}
