package log

import (
	"os"

	golog "log"
)

type logLogger struct {
	log *golog.Logger
}

func (t *logLogger) Log(v ...interface{}) {
	t.log.Print(v...)
}

func (t *logLogger) Logf(format string, v ...interface{}) {
	t.log.Printf(format, v...)
}

func New() *logLogger {
	return &logLogger{
		log: golog.New(os.Stderr, "", golog.LstdFlags|golog.Lshortfile),
	}
}
