package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// TableEntity describes a type that can be used as an input to the methods that
// accepts data for entities that can be sent to the Table Service.
//
// See types StructTableEntity, MapTableEntity and MarshaledTableEntity.
type TableEntity interface {
	jsonMarshal() ([]byte, error)
}

// MapTableEntity descibes a table entity input in a map that allows free form
// construction of the JSON object for the entity that's going to be sent in the
// request. The values for the keys must be JSON literal values; not objects or
// arrays.
//
// Example:
//
//     m := MapTableEntity(map[string]interface{}{
// 	       "PartitionKey":   "mypartitionkey",
// 	       "RowKey":         "myrowkey",
// 	       "Address":        "Mountain View",
// 	       "Age@odata.type": "Edm.Int64",
// 	       "Age":            "255",
//     })
type MapTableEntity map[string]interface{}

func (m MapTableEntity) jsonMarshal() ([]byte, error) {
	return json.MarshalIndent(m, "", "\t")
}

// StructTableEntity descibes a table entity input using an underlying struct
// instance. The struct gets serialized into JSON using the default JSON
// encoder, then the "odata.type" definitions are parsed from the `odatatype`
// tag and appended to the JSON object which gets sent to the API.
//
// Example:
//
//     type Student struct {
//         PartitionKey string
//         RowKey       string
//         Name         string `json:"name"`
//         Id           string `json:"id" odata.type:"Edm.Guid"`
//     }
//     s := StructTableEntity{Student{"pk", "rk", "name","c9da6455-213d-42c9-9a79-3e9149a57833"}}
type StructTableEntity struct {
	Val interface{}
}

func (s StructTableEntity) jsonMarshal() ([]byte, error) {
	// Step 0: Initial checks to see if value is a struct
	if s.Val == nil {
		return nil, errors.New("storage: struct value for given StructTableEntity is nil")
	}
	t := reflect.TypeOf(s.Val)
	if t.Kind() != reflect.Struct {
		return nil, errors.New("storage: value given to StructTableEntity is not a struct")
	}

	// Step 1: Serialize into JSON
	b, err := json.Marshal(s.Val)
	if err != nil {
		return nil, err
	}

	// Step 2: Serialize back to map
	var m map[string]interface{}
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}

	// Step 3: Scan val for odatatype tag
	extras := make(map[string]string)
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if tagVal := f.Tag.Get("odata.type"); tagVal != "" {
			extras[fmt.Sprintf("%s@odata.type", s.jsonName(f))] = tagVal
		}
	}

	// Step 4: Add tags back to map
	for k, v := range extras {
		m[k] = v
	}

	// Step 5: Serialize back into JSON
	return json.MarshalIndent(m, "", "\t")
}

func (s StructTableEntity) jsonName(f reflect.StructField) string {
	if jTag := f.Tag.Get("json"); jTag != "" {
		// return the part before the first ,
		return strings.Split(jTag, ",")[0]
	}

	return f.Name // default json tag unless specified
}

// MarshaledTableEntity describes a table entity which can generate its own
// JSON representation for sending it as is in the request to the table service.
type MarshaledTableEntity struct{ Val json.Marshaler }

func (c MarshaledTableEntity) jsonMarshal() ([]byte, error) {
	return c.Val.MarshalJSON()
}
