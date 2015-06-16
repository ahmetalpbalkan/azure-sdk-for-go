package storage

import (
	"net/url"
	"os"
	"strings"
	"testing"

	chk "gopkg.in/check.v1"
)

// Hook up gocheck to testing
func Test(t *testing.T) { chk.TestingT(t) }

type StorageClientSuite struct{}

var _ = chk.Suite(&StorageClientSuite{})

// getBasicClient returns a test client from storage credentials in the env
func getBasicClient(c *chk.C) Client {
	name := os.Getenv("ACCOUNT_NAME")
	if name == "" {
		c.Fatal("ACCOUNT_NAME not set, need an empty storage account to test")
	}
	key := os.Getenv("ACCOUNT_KEY")
	if key == "" {
		c.Fatal("ACCOUNT_KEY not set")
	}
	cli, err := NewBasicClient(name, key)
	c.Assert(err, chk.IsNil)
	return cli
}

func (s *StorageClientSuite) TestGetEndpoint(c *chk.C) {
	cli, err := NewBasicClient("foo", "YmFy")
	c.Assert(err, chk.IsNil)

	cases := []struct {
		service  string
		path     string
		params   url.Values
		expected string
	}{
		{"blob", "", url.Values{}, "https://foo.blob.core.windows.net/"},
		{"blob", "path", url.Values{}, "https://foo.blob.core.windows.net/path"},
		{"blob", "/path", url.Values{}, "https://foo.blob.core.windows.net/path"},
		{"blob", "/top(*')", url.Values{}, "https://foo.blob.core.windows.net/top%28%2A%27%29"},
		{"blob", "", url.Values{"a": {"b"}, "c": {"d"}}, "https://foo.blob.core.windows.net/?a=b&c=d"},
		{"blob", "/^$path", url.Values{"a": {"b"}, "c": {"d"}}, "https://foo.blob.core.windows.net/%5E$path?a=b&c=d"},
	}

	for _, t := range cases {
		out := cli.getEndpoint(t.service, t.path, t.params)
		c.Assert(out.String(), chk.Equals, t.expected)

		expectedPath := t.path
		if !strings.HasPrefix(expectedPath, "/") {
			expectedPath = "/" + expectedPath
		}

		c.Assert(out.Path, chk.Equals, expectedPath)
	}
}

func (s *StorageClientSuite) Test_getStandardHeaders(c *chk.C) {
	cli, err := NewBasicClient("foo", "YmFy")
	c.Assert(err, chk.IsNil)

	headers := cli.getStandardHeaders()
	c.Assert(len(headers), chk.Equals, 2)
	c.Assert(headers["x-ms-version"], chk.Equals, cli.apiVersion)
	if _, ok := headers["x-ms-date"]; !ok {
		c.Fatal("Missing x-ms-date header")
	}
}

func (s *StorageClientSuite) TestReturnsStorageServiceError(c *chk.C) {
	// attempt to delete a nonexisting container
	_, err := getBlobClient(c).deleteContainer(randContainer())
	c.Assert(err, chk.NotNil)

	v, ok := err.(AzureStorageServiceError)
	c.Check(ok, chk.Equals, true)
	c.Assert(v.StatusCode, chk.Equals, 404)
	c.Assert(v.Code, chk.Equals, "ContainerNotFound")
	c.Assert(v.Code, chk.Not(chk.Equals), "")
}
