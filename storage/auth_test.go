package storage

import (
	"net/url"

	chk "gopkg.in/check.v1"
)

func (s *StorageClientSuite) Test_auth_canonicalResource(c *chk.C) {
	type test struct{ url, expected string }
	tests := []test{
		{"https://foo.blob.core.windows.net/path?a=b&c=d&comp=ok", "/foo/path?comp=ok"},
		{"https://foo.blob.core.windows.net/?comp=list", "/foo/?comp=list"},
		{"https://foo.blob.core.windows.net/cnt/blob", "/foo/cnt/blob"},
		{"https://foo.blob.core.windows.net/Table('bar')", "/foo/Table%28%27bar%27%29"},
	}

	ss := blobQueueSigner{baseSigner{accountName: "foo"}}
	for _, i := range tests {
		u, err := url.Parse(i.url)
		c.Assert(err, chk.IsNil)

		out, err := ss.canonicalResource(u)
		c.Assert(err, chk.IsNil)
		c.Assert(out, chk.Equals, i.expected)
	}
}

func (s *StorageClientSuite) Test_auth_base_canonicalHeader(c *chk.C) {
	type test struct {
		headers  map[string]string
		expected string
	}
	tests := []test{
		{map[string]string{}, ""},
		{map[string]string{"x-ms-foo": "bar"}, "x-ms-foo:bar"},
		{map[string]string{"foo:": "bar"}, ""},
		{map[string]string{"foo:": "bar", "x-ms-foo": "bar"}, "x-ms-foo:bar"},
		{map[string]string{
			"x-ms-version":   "9999-99-99",
			"x-ms-blob-type": "BlockBlob"}, "x-ms-blob-type:BlockBlob\nx-ms-version:9999-99-99"}}

	ss := baseSigner{accountName: "foo"}
	for _, i := range tests {
		c.Assert(ss.canonicalHeader(i.headers), chk.Equals, i.expected)
	}
}

// TODO(ahmetb) implement tests for other methods
