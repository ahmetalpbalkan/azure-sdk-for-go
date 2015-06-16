package storage

import (
	chk "gopkg.in/check.v1"
)

func (s *StorageTableSuite) Test_TableEntity_Map(c *chk.C) {
	m := MapTableEntity(map[string]interface{}{
		"PartitionKey":   "mypartitionkey",
		"RowKey":         "myrowkey",
		"Address":        "Mountain View",
		"Age@odata.type": "Edm.Int64",
		"Age":            "255",
	})

	expected := `{
	"Address": "Mountain View",
	"Age": "255",
	"Age@odata.type": "Edm.Int64",
	"PartitionKey": "mypartitionkey",
	"RowKey": "myrowkey"
}`

	b, err := m.jsonMarshal()
	c.Assert(err, chk.IsNil)
	c.Assert(string(b), chk.Equals, expected)
}

func (s *StorageTableSuite) Test_TableEntity_Struct(c *chk.C) {
	type MyStruct struct {
		PartitionKey          string
		RowKey                string
		Name                  string `json:"name"`
		FieldWithDefaultName  int
		GUIDVal               string `json:"guidVal" odata.type:"Edm.Guid"`
		FieldWithOnlyODataTag int64  `odata.type:"Edm.Int64"`
	}
	entity := StructTableEntity{MyStruct{
		PartitionKey:          "pk",
		RowKey:                "rk",
		Name:                  "foo",
		FieldWithDefaultName:  1,
		GUIDVal:               "000-000",
		FieldWithOnlyODataTag: 2,
	}}

	expected := `{
	"FieldWithDefaultName": 1,
	"FieldWithOnlyODataTag": 2,
	"FieldWithOnlyODataTag@odata.type": "Edm.Int64",
	"PartitionKey": "pk",
	"RowKey": "rk",
	"guidVal": "000-000",
	"guidVal@odata.type": "Edm.Guid",
	"name": "foo"
}`
	b, err := entity.jsonMarshal()
	c.Assert(err, chk.IsNil)
	c.Assert(string(b), chk.Equals, expected)
}

type MyMarshaler struct{ mockOutput string }

func (m MyMarshaler) MarshalJSON() ([]byte, error) { return []byte(m.mockOutput), nil }

func (s *StorageTableSuite) Test_TableEntity_MarshaledTableEntity(c *chk.C) {
	expected := `{"Name":"foo", "PartitionKey": "pk", "RowKey": "rk"}`
	custom := MarshaledTableEntity{MyMarshaler{expected}}

	b, err := custom.jsonMarshal()
	c.Assert(err, chk.IsNil)
	c.Assert(string(b), chk.Equals, expected)
}
