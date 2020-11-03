package async

import (
	"encoding/json"
	"log"
	"reflect"
	"time"

	"github.com/alecthomas/jsonschema"
)

type Time struct {
	time.Time
}

func (t Time) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Unix())

}

func (t *Time) UnmarshalJSON(data []byte) error {
	var ts int64
	err := json.Unmarshal(data, &ts)
	if err != nil {
		return err
	}
	*t = Time{time.Unix(ts, 0)}
	return nil
}

func (s *Process) HasThread(id string) bool {
	for _, sel := range s.Threads {
		if sel.Id == id {
			return true
		}
	}
	return false
}

func (s *Process) SetThread(new *Thread) (created bool) {
	for i, sel := range s.Threads {
		if sel.Id == new.Id {
			s.Threads[i] = new
			return false
		}
	}
	s.Threads = append(s.Threads, new)
	return true
}

func ReflectType(name string, t interface{}) *Type {
	schema, err := jsonschema.Reflect(t).MarshalJSON()
	if err != nil {
		panic(err) // not expected
	}
	log.Print(reflect.TypeOf(t))
	return &Type{
		Id:         name,
		JsonSchema: schema,
	}
}
