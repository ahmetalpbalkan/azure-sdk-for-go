package storage

import (
	"fmt"
	"net/url"
	"sort"
	"strings"
)

type requestSigner interface {
	authScheme() string
	canonicalizedString(verb string, headers map[string]string, resourceURL *url.URL) (string, error)
}

type baseSigner struct {
	accountName string
}

func (b baseSigner) canonicalHeader(headers map[string]string) string {
	cm := make(map[string]string)

	for k, v := range headers {
		headerName := strings.TrimSpace(strings.ToLower(k))

		if strings.HasPrefix(headerName, "x-ms-") {
			cm[headerName] = v
		}
	}

	if len(cm) == 0 {
		return ""
	}

	keys := make([]string, 0, len(cm))
	for key := range cm {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	ch := ""

	for i, key := range keys {
		if i == len(keys)-1 {
			ch += fmt.Sprintf("%s:%s", key, cm[key])
		} else {
			ch += fmt.Sprintf("%s:%s\n", key, cm[key])
		}
	}
	return ch
}

func (b baseSigner) canonicalResource(resourceURL *url.URL) (string, error) {
	cr := "/" + b.accountName + b.encodeComponents(resourceURL.Path)

	params, err := url.ParseQuery(resourceURL.RawQuery)
	if err != nil {
		return "", fmt.Errorf("canonicalResource URL parsing error: %v", err)
	}

	// keep ?comp= parameter
	if params.Get("comp") != "" {
		v := url.Values{}
		v.Set("comp", params.Get("comp"))
		cr += "?" + v.Encode()
	}
	return cr, nil
}

func (b baseSigner) encodeComponents(path string) string {
	// func encode characters outside:
	// - ASCII letters
	// - numbers
	// - and the following characters: /,$=
	out := ""
	for _, c := range path {
		switch {
		case c >= 'a' && c <= 'z':
			fallthrough
		case c >= 'A' && c <= 'Z':
			fallthrough
		case c >= '0' && c <= '9':
			fallthrough
		case c == '/' || c == ',' || c == '$' || c == '=':
			out += string(c)
		default:
			out += url.QueryEscape(string(c))
		}
	}
	return out
}

// blobQueueSigner can sign blob service and queue service requests.
type blobQueueSigner struct{ baseSigner }

func (s blobQueueSigner) authScheme() string { return "SharedKeyLite" }

func (s blobQueueSigner) canonicalizedString(verb string, headers map[string]string, resourceURL *url.URL) (string, error) {
	cHeader := s.canonicalHeader(headers)
	cRes, err := s.canonicalResource(resourceURL)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s",
		verb,
		headers["Content-MD5"],
		headers["Content-Type"],
		headers["Date"],
		cHeader,
		cRes), nil
}

// tableSigner can sign table service requests.
type tableSigner struct{ baseSigner }

func (s tableSigner) authScheme() string { return "SharedKeyLite" }

func (s tableSigner) canonicalizedString(verb string, headers map[string]string, resourceURL *url.URL) (string, error) {
	cRes, err := s.canonicalResource(resourceURL)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s\n%s",
		headers["x-ms-date"],
		cRes), nil
}
