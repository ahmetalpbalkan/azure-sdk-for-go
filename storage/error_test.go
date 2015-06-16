package storage

import (
	chk "gopkg.in/check.v1"
)

func (s *StorageClientSuite) Test_serviceErrFromXML(c *chk.C) {
	resp := `<?xml version="1.0" encoding="utf-8"?>
<Error>
  <Code>code-value</Code>
  <Message>message-value</Message>
</Error>`

	err := serviceErrFromXML([]byte(resp), 400, "foo")
	c.Assert(err, chk.NotNil)

	var e AzureStorageServiceError
	c.Assert(err, chk.FitsTypeOf, e)
	e = err.(AzureStorageServiceError)

	c.Assert(e.StatusCode, chk.Equals, 400)
	c.Assert(e.Message, chk.Equals, "message-value")
	c.Assert(e.Code, chk.Equals, "code-value")
	c.Assert(e.RequestID, chk.Equals, "foo")
}

func (s *StorageClientSuite) Test_tableErrFromJSON(c *chk.C) {
	resp := `{"odata.error":{
		"code":"ResourceNotFound",
		"message":{
			"lang":"en-US",
			"value":"The specified resource does not exist.\nRequestId:102a2b55-eb35-4254-9daf-854db78a47bd\nTime:2014-06-04T16:18:20.4307735Z"}}}`

	err := tableErrFromJSON([]byte(resp), 404, "foo")
	c.Assert(err, chk.NotNil)

	var e AzureStorageTableServiceError
	c.Assert(err, chk.FitsTypeOf, e)
	e = err.(AzureStorageTableServiceError)

	c.Assert(e.StatusCode, chk.Equals, 404)
	c.Assert(e.RequestID, chk.Equals, "foo")
	c.Assert(e.Code, chk.Equals, "ResourceNotFound")
	c.Assert(e.Message.Value, chk.Equals, "The specified resource does not exist.\nRequestId:102a2b55-eb35-4254-9daf-854db78a47bd\nTime:2014-06-04T16:18:20.4307735Z")
	c.Assert(e.Message.Lang, chk.Equals, "en-US")
}
