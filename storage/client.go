// Package storage provides clients for Microsoft Azure Storage Services.
package storage

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

const (
	// DefaultBaseURL is the domain name used for storage requests when a
	// default client is created.
	DefaultBaseURL = "core.windows.net"

	// DefaultAPIVersion is the  Azure Storage API version string used when a
	// basic client is created.
	DefaultAPIVersion = "2014-02-14"

	defaultUseHTTPS = true

	blobServiceName  = "blob"
	tableServiceName = "table"
	queueServiceName = "queue"
)

// Client is the object that needs to be constructed to perform
// operations on the storage account.
type Client struct {
	accountName string
	accountKey  []byte
	useHTTPS    bool
	baseURL     string
	apiVersion  string
}

type storageResponse struct {
	statusCode int
	headers    http.Header
	body       io.ReadCloser
}

// UnexpectedStatusCodeError is returned when a storage service responds with neither an error
// nor with an HTTP status code indicating success.
type UnexpectedStatusCodeError struct {
	allowed []int
	got     int
}

func (e UnexpectedStatusCodeError) Error() string {
	s := func(i int) string { return fmt.Sprintf("%d %s", i, http.StatusText(i)) }

	got := s(e.got)
	expected := []string{}
	for _, v := range e.allowed {
		expected = append(expected, s(v))
	}
	return fmt.Sprintf("storage: status code from service response is %s; was expecting %s", got, strings.Join(expected, " or "))
}

// NewBasicClient constructs a Client with given storage service name and
// key.
func NewBasicClient(accountName, accountKey string) (Client, error) {
	return NewClient(accountName, accountKey, DefaultBaseURL, DefaultAPIVersion, defaultUseHTTPS)
}

// NewClient constructs a Client. This should be used if the caller wants
// to specify whether to use HTTPS, a specific REST API version or a custom
// storage endpoint than Azure Public Cloud.
func NewClient(accountName, accountKey, blobServiceBaseURL, apiVersion string, useHTTPS bool) (Client, error) {
	var c Client
	if accountName == "" {
		return c, fmt.Errorf("azure: account name required")
	} else if accountKey == "" {
		return c, fmt.Errorf("azure: account key required")
	} else if blobServiceBaseURL == "" {
		return c, fmt.Errorf("azure: base storage service url required")
	}

	key, err := base64.StdEncoding.DecodeString(accountKey)
	if err != nil {
		return c, err
	}

	return Client{
		accountName: accountName,
		accountKey:  key,
		useHTTPS:    useHTTPS,
		baseURL:     blobServiceBaseURL,
		apiVersion:  apiVersion,
	}, nil
}

func (c Client) getEndpoint(service, path string, params url.Values) *url.URL {
	scheme := "http"
	if c.useHTTPS {
		scheme = "https"
	}

	host := fmt.Sprintf("%s.%s.%s", c.accountName, service, c.baseURL)

	// Add leading slash to path if not exists
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	return &url.URL{
		Scheme:   scheme,
		Host:     host,
		Path:     path,
		RawQuery: params.Encode(),
	}
}

// GetBlobService returns a BlobStorageClient which can operate on the blob
// service of the storage account.
func (c Client) GetBlobService() BlobStorageClient {
	return BlobStorageClient{c}
}

// GetQueueService returns a QueueServiceClient which can operate on the queue
// service of the storage account.
func (c Client) GetQueueService() QueueServiceClient {
	return QueueServiceClient{c}
}

// GetTableService returns a TableServiceClient which can operate on the table
// service of the storage account.
func (c Client) GetTableService() TableServiceClient {
	return TableServiceClient{c}
}

func (c Client) getStandardHeaders() map[string]string {
	d := currentTimeRfc1123Formatted()
	return map[string]string{
		"x-ms-version": c.apiVersion,
		"x-ms-date":    d,
	}
}

func (c Client) getAuthorizationHeader(signer requestSigner, verb string, url *url.URL, headers map[string]string) (string, error) {
	canonicalizedString, err := signer.canonicalizedString(verb, headers, url)
	if err != nil {
		return "", fmt.Errorf("storage: error parsing the request for signing: %v", err)
	}
	signed := c.computeHmac256(canonicalizedString) // sign with key
	return fmt.Sprintf("%s %s:%s", signer.authScheme(), c.accountName, signed), nil
}

func (c Client) exec(verb string, url *url.URL, headers map[string]string, body io.Reader, signer requestSigner, errFunc serviceErrorFunc) (*storageResponse, error) {
	authHeader, err := c.getAuthorizationHeader(signer, verb, url, headers)
	if err != nil {
		return nil, err
	}
	headers["Authorization"] = authHeader

	req, err := http.NewRequest(verb, url.String(), body)
	for k, v := range headers {
		req.Header.Add(k, v)
	}
	httpClient := http.DefaultClient

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	statusCode := resp.StatusCode
	if statusCode >= 400 && statusCode <= 505 {
		var respBody []byte
		respBody, err = readResponseBody(resp)
		if err != nil {
			return nil, err
		}

		if len(respBody) == 0 {
			// no error in response body
			err = fmt.Errorf("storage: service returned without a response body (%s)",
				resp.Status)
		} else {
			// response contains storage service error object, unmarshal
			err = errFunc(respBody, resp.StatusCode, resp.Header.Get("x-ms-request-id"))
		}
		return &storageResponse{
			statusCode: resp.StatusCode,
			headers:    resp.Header,
			body:       ioutil.NopCloser(bytes.NewReader(respBody)), /* restore the body */
		}, err
	}

	return &storageResponse{
		statusCode: resp.StatusCode,
		headers:    resp.Header,
		body:       resp.Body}, nil
}

func readResponseBody(resp *http.Response) ([]byte, error) {
	defer resp.Body.Close()
	out, err := ioutil.ReadAll(resp.Body)
	if err == io.EOF {
		err = nil
	}
	return out, err
}

// checkRespCode returns UnexpectedStatusError if the given response code is not
// one of the allowed status codes; otherwise nil.
func checkRespCode(respCode int, allowed []int) error {
	for _, v := range allowed {
		if respCode == v {
			return nil
		}
	}
	return UnexpectedStatusCodeError{allowed, respCode}
}
