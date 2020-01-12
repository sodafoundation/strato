package main

import (
	"os"
	"strings"
	"testing"
)

func Test_Main(t *testing.T) {
	var (
		args []string
		//run  bool
		run = true
	)

	for _, arg := range os.Args {
		switch {
		case arg == "__DEVEL--i-heard-you-like-tests":
			run = true
		case strings.HasPrefix(arg, "-test"):
		case strings.HasPrefix(arg, "__DEVEL"):
		default:
			args = append(args, arg)
		}
	}
	os.Args = args

	if run {
		main()
	}
}
