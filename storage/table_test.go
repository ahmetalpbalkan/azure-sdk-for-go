package storage

import (
	"fmt"

	chk "gopkg.in/check.v1"
)

type StorageTableSuite struct{}

var _ = chk.Suite(&StorageTableSuite{})

const tableTestPrefix = "zzzzztest"

func randTableName() string { return tableTestPrefix + randString(10) }

func getTableClient(c *chk.C) TableServiceClient {
	return getBasicClient(c).GetTableService()
}

func (s *StorageTableSuite) SetUpSuite(c *chk.C) {
	// delete ALL tables (clean start)
	cli := getTableClient(c)
	r, err := cli.QueryTables()
	c.Assert(err, chk.IsNil)
	for _, v := range r.Value {
		c.Assert(cli.DeleteTable(v.TableName), chk.IsNil)
	}
}

func (s *StorageTableSuite) TestQueryTables(c *chk.C) {
	cli := getTableClient(c)

	_, err := cli.QueryTables()
	c.Assert(err, chk.IsNil)
}

func (s *StorageTableSuite) TestQueryTables_withResults(c *chk.C) {
	cli := getTableClient(c)
	// create some tables
	const n = 5
	for i := 0; i < n; i++ {
		c.Assert(cli.CreateTable(CreateTableParameters{randTableName()}), chk.IsNil)
	}

	r, err := cli.QueryTables()
	c.Assert(err, chk.IsNil)
	c.Assert(len(r.Value), chk.Equals, 5)
}

func (s *StorageTableSuite) TestCreateTable(c *chk.C) {
	cli := getTableClient(c)
	name := randTableName()

	c.Assert(cli.CreateTable(CreateTableParameters{name}), chk.IsNil)
	defer cli.DeleteTable(name)
}

func (s *StorageTableSuite) TestDeleteTable(c *chk.C) {
	cli := getTableClient(c)
	name := randTableName()

	c.Assert(cli.CreateTable(CreateTableParameters{name}), chk.IsNil)
	c.Assert(cli.DeleteTable(name), chk.IsNil)
}

func (s *StorageTableSuite) TestDeleteEntity_nonExistingEntity(c *chk.C) {
	cli := getTableClient(c)
	name := randTableName()
	c.Assert(cli.CreateTable(CreateTableParameters{name}), chk.IsNil)
	defer cli.DeleteTable(name)

	pk, rk := randString(10), randString(10)
	err := cli.DeleteEntity(name, pk, rk)
	c.Assert(err, chk.NotNil)

	e, ok := err.(AzureStorageTableServiceError)
	c.Assert(ok, chk.Equals, true)
	c.Log(e.Message.Value)
	c.Assert(e.StatusCode, chk.Equals, 404)
}

func (s *StorageTableSuite) TestInsertEntity_map_QueryEntity(c *chk.C) {
	cli := getTableClient(c)
	tbl := randTableName()
	c.Assert(cli.CreateTable(CreateTableParameters{tbl}), chk.IsNil)
	defer cli.DeleteTable(tbl)

	m := map[string]interface{}{
		"PartitionKey":        randString(5) + "-" + randString(5),
		"RowKey":              randString(10),
		"GuidVal":             "c9da6455-213d-42c9-9a79-3e9149a57833",
		"GuidVal@odata.type":  "Edm.Guid",
		"BoolVal":             true,
		"Int32Val":            42,
		"Int64Val":            "9223372036854775807",
		"Int64Val@odata.type": "Edm.Int64",
		"TimeVal":             "2013-08-22T01:12:06.2608595Z",
		"TimeVal@odata.type":  "Edm.DateTime",
	}
	entity := MapTableEntity(m)

	c.Assert(cli.InsertEntity(tbl, entity), chk.IsNil)
	c.Assert(cli.QueryEntity(tbl, fmt.Sprintf("%s", m["PartitionKey"]), fmt.Sprintf("%s", m["RowKey"])), chk.IsNil)
}

func (s *StorageTableSuite) TestInsertEntity_struct_QueryEntity(c *chk.C) {
	cli := getTableClient(c)
	tbl := randTableName()
	c.Assert(cli.CreateTable(CreateTableParameters{tbl}), chk.IsNil)
	defer cli.DeleteTable(tbl)

	type S struct {
		PartitionKey string
		RowKey       string
		GUIDVal      string `odata.type:"Edm.Guid"`
		BoolVal      bool   `odata.type:"Edm.Boolean"`
		Int32Val     int
		Int64Val     string `odata.type:"Edm.Int64"`
		TimeVal      string `odata.type:"Edm.DateTime"`
	}

	v := S{
		PartitionKey: randString(5) + "-" + randString(5),
		RowKey:       randString(10),
		GUIDVal:      "c9da6455-213d-42c9-9a79-3e9149a57833",
		BoolVal:      true,
		Int32Val:     42,
		Int64Val:     "9223372036854775807",
		TimeVal:      "2013-08-22T01:12:06.2608595Z",
	}
	entity := StructTableEntity{v}

	c.Assert(cli.InsertEntity(tbl, entity), chk.IsNil)
	c.Assert(cli.QueryEntity(tbl, v.PartitionKey, v.RowKey), chk.IsNil)
}
