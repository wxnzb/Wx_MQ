package raft

import (
	"encoding/gob"
	"io"
	"reflect"
	"sync"
	"unicode"
	"unicode/utf8"

	"sigs.k8s.io/structured-merge-diff/v4/value"
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

type LabDecoder struct {
	gob *gob.Decoder
}

func NewDecoder(r io.Reader) *LabDecoder {
	return &LabDecoder{gob: gob.NewDecoder(r)}
}
func (d *LabDecoder) Decode(v interface{}) error {
	checkValue(v)
	checkDefault(v)
	return d.gob.Decode(v)
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
func checkDefault(v interface{}) {
	if v == nil {
		return
	}
	return checkDefault1(reflect.ValueOf(v), 1, "")
}

// 递归检查value是不是默认值，要是不是，就加1
func checkDefault1(v reflect.Value, depth int, name string) {
	if depth > 3 {
		return
	}
	t := v.Type()
	k := t.Kind()
	switch k {
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			//这个返回的是什么
			name1 := t.Field(i).Name
			if name1 != "" {
				name1 = name + name1
			}
			checkDefault1(value.Field(i), depth+1, name1)
		}
		return
	case reflect.Ptr:
		if v.IsNil() {
			return
		}
		checkDefault1(v.Elem(), depth+1, name)
		return
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr, reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128, reflect.String:
		if reflect.DeepEqual(v.Interface(), reflect.Zero(v.Type()).Interface()) == false {
			rmu.Lock()
			errCnt++
			rmu.Unlock()
		}
		return
	}
}
func Register(v interface{}) {
	checkValue(v)
	return gob.Register(v)
}

func RegisterName(name string, v interface{}) {
	checkValue(v)
	gob.RegisterName(name, v)
}
