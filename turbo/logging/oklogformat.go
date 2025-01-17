package logging

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/ledgerwatch/log/v3"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	timeFormat  = "2006-01-02T15:04:05-0700"
	errorKey    = "LOG15_ERROR"
	floatFormat = 'f'
)

func OkLogV1Format(r *log.Record) []byte {

	props := make(map[string]interface{})
	content := make(map[string]string)

	callFrame := r.Call.Frame()

	props["time"] = r.Time.UnixMilli()
	props["level"] = formatLogLevel(r.Lvl)
	props["line_num"] = callFrame.Line
	props["class_name"] = filepath.Base(callFrame.File)
	props["ok_log_version"] = "1.0"
	props["method"] = r.Call.Frame().Function

	content["msg"] = r.Msg
	content["t"] = formatLogfmtValue(r.Time)

	for i := 0; i < len(r.Ctx); i += 2 {
		k, ok := r.Ctx[i].(string)
		if !ok {
			content[errorKey] = fmt.Sprintf("%+v is not a string key", r.Ctx[i])
		}
		content[k] = formatLogfmtValue(r.Ctx[i+1])
	}

	e := stringBufPool.Get().(*bytes.Buffer)
	e.WriteByte('[')
	i := len(content)
	for k, v := range content {
		e.WriteString(k)
		e.WriteByte('=')
		e.WriteString(v)
		i--
		if i > 0 {
			e.WriteString(", ")
		}
	}
	e.WriteByte(']')

	props["content"] = e.String()

	e.Reset()

	b, err := json.Marshal(props)
	if err != nil {
		b, _ = json.Marshal(map[string]string{
			errorKey: err.Error(),
		})
		return b
	}

	b = append(b, '\n')

	return b
}

func formatShared(value interface{}) (result interface{}) {
	defer func() {
		if err := recover(); err != nil {
			if v := reflect.ValueOf(value); v.Kind() == reflect.Ptr && v.IsNil() {
				result = "nil"
			} else {
				panic(err)
			}
		}
	}()

	switch v := value.(type) {
	case time.Time:
		return v.Format(timeFormat)

	case error:
		return v.Error()

	case fmt.Stringer:
		return v.String()

	default:
		return v
	}
}

var stringBufPool = sync.Pool{
	New: func() interface{} { return new(bytes.Buffer) },
}

func formatLogfmtValue(value interface{}) string {
	if value == nil {
		return "nil"
	}

	if t, ok := value.(time.Time); ok {
		return t.Format(timeFormat)
	}
	value = formatShared(value)
	switch v := value.(type) {
	case bool:
		return strconv.FormatBool(v)
	case float32:
		return strconv.FormatFloat(float64(v), floatFormat, 3, 64)
	case float64:
		return strconv.FormatFloat(v, floatFormat, 3, 64)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", value)
	case string:
		return v
	default:
		return fmt.Sprintf("%+v", value)
	}
}

func formatLogLevel(lvl log.Lvl) string {
	switch lvl {
	case log.LvlError:
		return "ERROR"
	default:
		return strings.ToUpper(lvl.String())
	}
}
