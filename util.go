package async

import (
	"encoding/json"
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

func (s *Workflow) HasThread(id string) bool {
	for _, sel := range s.Threads {
		if sel.Id == id {
			return true
		}
	}
	return false
}

func (s *Workflow) SetThread(new *Thread) (created bool) {
	for i, sel := range s.Threads {
		if sel.Id == new.Id {
			s.Threads[i] = new
			return false
		}
	}
	s.Threads = append(s.Threads, new)
	return true
}

func ReflectType(name string, t interface{}, version uint64, description string) *Type {
	schema, err := jsonschema.Reflect(t).MarshalJSON()
	if err != nil {
		panic(err) // not expected
	}
	return &Type{
		Id:          name,
		JsonSchema:  schema,
		Version:     version,
		Description: description,
	}
}
