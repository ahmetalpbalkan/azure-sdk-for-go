package storage

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
)

// serviceErrorFunc creates an error based on the HTTP response format of the
// respective storage service.
type serviceErrorFunc func(body []byte, statusCode int, xMsRequestID string) error

// AzureStorageServiceError contains fields of the error response from
// Azure Storage Blob Service and Queue Storage REST APIs.
//
// See https://msdn.microsoft.com/en-us/library/azure/dd179382.aspx
// Some fields might be specific to certain API calls.
type AzureStorageServiceError struct {
	Code                      string `xml:"Code"`
	Message                   string `xml:"Message"`
	AuthenticationErrorDetail string `xml:"AuthenticationErrorDetail"`
	QueryParameterName        string `xml:"QueryParameterName"`
	QueryParameterValue       string `xml:"QueryParameterValue"`
	Reason                    string `xml:"Reason"`
	StatusCode                int
	RequestID                 string
}

func (e AzureStorageServiceError) Error() string {
	return fmt.Sprintf("storage: service returned error: StatusCode=%d, ErrorCode=%s, ErrorMessage=%s, RequestId=%s", e.StatusCode, e.Code, e.Message, e.RequestID)
}

// serviceErrFromXML deserializes given XML error response to error.
func serviceErrFromXML(body []byte, statusCode int, requestID string) error {
	var e AzureStorageServiceError
	if err := xml.Unmarshal(body, &e); err != nil {
		return fmt.Errorf("storage: error deserializing error: %v\nbody=%q",
			err, string(body))
	}
	e.StatusCode = statusCode
	e.RequestID = requestID
	return e
}

// AzureStorageTableServiceError contains fields of the error response from
// Azure Storage Table Service REST API.
//
// See https://msdn.microsoft.com/en-us/library/azure/dd179382.aspx
type AzureStorageTableServiceError struct {
	Code    string `json:"code"`
	Message struct {
		Lang  string `json:"lang"`
		Value string `json:"value"`
	} `"json:message"`

	// extra fields added for more info
	StatusCode int    `json:"-"`
	RequestID  string `json:"-"`
}

func (e AzureStorageTableServiceError) Error() string {
	return fmt.Sprintf("storage: table service returned error: StatusCode=%d ErrorCode=%s ErrorMessage=%q", e.StatusCode, e.Code, e.Message.Value)
}

// tableErrFromJSON deserializes table storage OData error response in JSON to
// error.
func tableErrFromJSON(body []byte, statusCode int, requestID string) error {
	// intermediate struct to grab only the relevant part of the error message
	type odataErr struct {
		Err struct {
			AzureStorageTableServiceError
		} `json:"odata.error"`
	}

	var o odataErr
	if err := json.Unmarshal(body, &o); err != nil {
		return fmt.Errorf("storage: error deserializing error: %v\nbody=%q",
			err, string(body))
	}
	e := o.Err.AzureStorageTableServiceError
	e.StatusCode = statusCode
	e.RequestID = requestID
	return e
}

type azureParameterError string

func (e azureParameterError) Error() string {
	return fmt.Sprintf("storage: parameter is empty: %s", e)
}
