package utils

import (
	"github.com/emicklei/go-restful"
	"strings"
)
const(
MIME_OCTET = "application/octet-stream" // If Content-Type is not present in request, use the default
)

type MyRoute struct {
	restful.Route
}

func (r MyRoute) String() string {
	return "in subclass"
}


// Return whether this Route can consume content with a type specified by mimeTypes (can be empty).
func (r MyRoute) matchesContentType(mimeTypes string) bool {

	if len(r.Consumes) == 0 {
		// did not specify what it can consume ;  any media type (“*/*”) is assumed
		return true
	}

	if len(mimeTypes) == 0 {
		// idempotent methods with (most-likely or guaranteed) empty content match missing Content-Type
		m := r.Method
		if m == "GET" || m == "HEAD" || m == "OPTIONS" || m == "DELETE" || m == "TRACE" {
			return true
		}
		// proceed with default
		mimeTypes = MIME_OCTET
	}

	parts := strings.Split(mimeTypes, ",")
	for _, each := range parts {
		var contentType string
		if strings.Contains(each, ";") {
			contentType = strings.Split(each, ";")[0]
		} else {
			contentType = each
		}
		// trim before compare
		contentType = strings.Trim(contentType, " ")
		for _, consumeableType := range r.Consumes {
			if consumeableType == "*/*" || consumeableType == contentType {
				return true
			}
		}
	}
	return false
}