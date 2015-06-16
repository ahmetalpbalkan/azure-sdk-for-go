package storage

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

// TableServiceClient contains operations for Microsoft Azure Table Storage
// Service.
type TableServiceClient struct {
	client Client
}

func (c TableServiceClient) exec(verb string, url *url.URL, headers map[string]string, body io.Reader) (*storageResponse, error) {
	signer := tableSigner{baseSigner{accountName: c.client.accountName}}
	return c.client.exec(verb, url, headers, body, signer, tableErrFromJSON)
}

func (c TableServiceClient) baseHeaders() map[string]string {
	h := c.client.getStandardHeaders()
	h[acceptKey] = noMetadataHeader
	return h
}

const (
	acceptKey        = "Accept"
	noMetadataHeader = "application/json;odata=nometadata"
	jsonContentType  = "application/json"
)

// QueryTablesResponse is the response object returned from QueryTables call
// returning no OData metadata.
type QueryTablesResponse struct {
	Value []struct {
		TableName string `json:"TableName"`
	} `"json:value"`
}

// CreateTableParameters are the set of parameters that can be provided to
// CreateTable call.
type CreateTableParameters struct {
	TableName string `json:"TableName"`
}

// QueryTables operation returns a list of tables under the specified account.
//
// This implementation of the operation returns no OData metadata about the
// response contents.
//
// See https://msdn.microsoft.com/en-us/library/azure/dd179405.aspxs
func (c TableServiceClient) QueryTables() (QueryTablesResponse, error) {
	uri := c.client.getEndpoint(tableServiceName, "/Tables", url.Values{})

	var out QueryTablesResponse
	resp, err := c.exec("GET", uri, c.baseHeaders(), nil)
	if err != nil {
		return out, err
	}
	defer resp.body.Close()

	err = jsonUnmarshal(resp.body, &out)
	return out, err
}

// CreateTable operation creates a new table in the storage account.
//
// See https://msdn.microsoft.com/en-us/library/azure/dd135729.aspx
func (c TableServiceClient) CreateTable(params CreateTableParameters) error {
	uri := c.client.getEndpoint(tableServiceName, "Tables", url.Values{})
	headers := c.baseHeaders()
	headers["Content-Type"] = jsonContentType

	body, _, err := jsonMarshal(params)
	if err != nil {
		return err
	}

	resp, err := c.exec("POST", uri, headers, body)
	if err != nil {
		return err
	}

	defer resp.body.Close()
	return checkRespCode(resp.statusCode, []int{http.StatusCreated, http.StatusNoContent})
}

// DeleteTable operation deletes the specified table and any data it contains.
//
// See https://msdn.microsoft.com/en-us/library/azure/dd179387.aspx
func (c TableServiceClient) DeleteTable(tableName string) error {
	path := fmt.Sprintf("Tables('%s')", tableName)
	uri := c.client.getEndpoint(tableServiceName, path, url.Values{})

	resp, err := c.exec("DELETE", uri, c.baseHeaders(), nil)
	if err != nil {
		return err
	}

	defer resp.body.Close()
	return checkRespCode(resp.statusCode, []int{http.StatusNoContent})
}

// InsertEntity operation inserts a new entity into a table.
//
// https://msdn.microsoft.com/en-us/library/azure/dd179433.aspx
func (c TableServiceClient) InsertEntity(tableName string, entity TableEntity) error {
	if tableName == "" {
		return azureParameterError("tableName")
	}
	if entity == nil {
		return azureParameterError("entity")
	}
	body, err := entity.jsonMarshal()
	if err != nil {
		return err
	}

	uri := c.client.getEndpoint(tableServiceName, tableName, url.Values{})
	headers := c.baseHeaders()
	headers["Content-Type"] = jsonContentType

	resp, err := c.exec("POST", uri, headers, bytes.NewReader(body))
	if err != nil {
		return err
	}

	defer resp.body.Close()
	return checkRespCode(resp.statusCode, []int{http.StatusCreated, http.StatusNoContent})
}

// QueryEntity operation queries a single entity in a table.
//
// https://msdn.microsoft.com/en-us/library/azure/dd179433.aspx
func (c TableServiceClient) QueryEntity(tableName, partitionKey, rowKey string) error {
	// TODO(ahmetb) implement query options (e.g. $select, json metadata level)
	if tableName == "" {
		return azureParameterError("tableName")
	}
	if partitionKey == "" {
		return azureParameterError("partitionKey")
	}
	if rowKey == "" {
		return azureParameterError("rowKey")
	}

	path := fmt.Sprintf("%s(PartitionKey='%s',RowKey='%s')", tableName, partitionKey, rowKey)
	uri := c.client.getEndpoint(tableServiceName, path, url.Values{})
	headers := c.baseHeaders()
	headers["Content-Type"] = jsonContentType
	headers[acceptKey] = noMetadataHeader

	resp, err := c.exec("GET", uri, headers, nil)
	if err != nil {
		return err
	}

	defer resp.body.Close()
	return nil
}

// DeleteEntity operation deletes an existing entity in a table.
//
// https://msdn.microsoft.com/en-us/library/azure/dd135727.aspx
func (c TableServiceClient) DeleteEntity(tableName, partitionKey, rowKey string) error {
	if tableName == "" {
		return azureParameterError("tableName")
	}
	if partitionKey == "" {
		return azureParameterError("partitionKey")
	}
	if rowKey == "" {
		return azureParameterError("rowKey")
	}

	path := fmt.Sprintf("%s(PartitionKey='%s',RowKey='%s')", tableName, partitionKey, rowKey)
	uri := c.client.getEndpoint(tableServiceName, path, url.Values{})
	resp, err := c.exec("DELETE", uri, c.baseHeaders(), nil)
	if err != nil {
		return err
	}

	defer resp.body.Close()
	return checkRespCode(resp.statusCode, []int{http.StatusNoContent})
}
