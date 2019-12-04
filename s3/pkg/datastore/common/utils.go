package common

import (
	"context"
)

func GetMd5FromCtx(ctx context.Context) (md5val string) {
	// md5 provided by user for uploading object.
	if val := ctx.Value(CONTEXT_KEY_MD5); val != nil {
		md5val = val.(string)
	}

	return
}

func TrimQuot(in string) string {
	s := in
	l := len(s)
	if l <= 0 {
		return ""
	}
	if s[l-1] == '"' {
		s = s[:l-1]
	}
	if s[0] == '"' {
		s = s[1:]
	}

	return s
}
