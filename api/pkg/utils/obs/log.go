package obs

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/go-ini/ini"
	"github.com/lestrrat-go/file-rotatelogs"
	"github.com/rifflock/lfshook"
	"github.com/sirupsen/logrus"
)

type LogFormatter struct {
	TimestampFormat string
	LogFormat       string
}

const (
	debugLevel             = "debug"
	infoLevel              = "info"
	warnLevel              = "warn"
	errorLevel             = "error"
	path                   = "path"
	level                  = "level"
	format                 = "format"
	defaultLogPath         = "/var/log/multi-cloud"
	defaultLogLevel        = "info"
	unknownHost            = "unknownhost"
	unknownUser            = "unknownuser"
	configFileName         = "/etc/log.conf"
	defaultLogFormat       = "[%time%] [%level%] [%filename%] [%funcName%():%lineNo%] [PID:%process%] %message%"
	defaultTimestampFormat = time.RFC3339
	callStackDeep          = 12
)

func InitLogs() {
	path, level, format := readConfigurationFile()
	configureLogModule(path, level, format)
}

func configureLogModule(path, level, format string) {
	configureWriter(path, format)
	configureLevel(level)
}

func configureWriter(path, format string) error {
	debugWriter, debugWriterErr := createWriter(path, debugLevel)
	infoWriter, infoWriterErr := createWriter(path, infoLevel)
	warnWriter, warnWriterErr := createWriter(path, warnLevel)
	errorWriter, errorWriterErr := createWriter(path, errorLevel)
	if debugWriterErr != nil || infoWriterErr != nil || warnWriterErr != nil || errorWriterErr != nil {
		return errors.New("Failed to create writer!\n")
	}
	lfsHook := lfshook.NewHook(lfshook.WriterMap{
		logrus.DebugLevel: debugWriter,
		logrus.InfoLevel:  infoWriter,
		logrus.WarnLevel:  warnWriter,
		logrus.ErrorLevel: errorWriter}, &LogFormatter{
		TimestampFormat: defaultTimestampFormat,
		LogFormat:       format + "\n",
	})
	logrus.AddHook(lfsHook)
	return nil
}

func createWriter(path, level string) (*rotatelogs.RotateLogs, error) {
	writer, err := rotatelogs.New(
		filepath.Join(path, logNameForRotateLogs(level)),
		rotatelogs.WithLinkName(filepath.Join(path, shortLogNameForRotateLogs(level))),
		rotatelogs.WithRotationTime(time.Hour))
	if err != nil {
		log.Println(err)
	}
	return writer, err
}

func configureLevel(level string) {
	switch level {
	case debugLevel:
		logrus.SetLevel(logrus.DebugLevel)
	case infoLevel:
		logrus.SetLevel(logrus.InfoLevel)
	case warnLevel:
		logrus.SetLevel(logrus.WarnLevel)
	case errorLevel:
		logrus.SetLevel(logrus.ErrorLevel)
	}
	logrus.SetReportCaller(true)
}

func logNameForRotateLogs(level string) (name string) {
	name = fmt.Sprintf("%s.%s.%s.log.%s.%%Y%%m%%d%%H.%d",
		filepath.Base(os.Args[0]),
		hostName(),
		userName(),
		strings.ToUpper(level),
		os.Getpid())
	return name
}

func shortLogNameForRotateLogs(level string) (name string) {
	name = fmt.Sprintf("%s.%s",
		filepath.Base(os.Args[0]),
		strings.ToUpper(level))
	return name
}

func shortHostname(hostname string) string {
	if i := strings.Index(hostname, "."); i >= 0 {
		return hostname[:i]
	}
	return hostname
}

func hostName() string {
	host := unknownHost
	h, err := os.Hostname()
	if err == nil {
		host = shortHostname(h)
	}
	return host
}

func userName() string {
	userName := unknownUser
	current, err := user.Current()
	if err == nil {
		userName = current.Username
	}
	// Sanitize userName since it may contain filepath separators on Windows.
	userName = strings.Replace(userName, `\`, "_", -1)
	return userName
}

func readConfigurationFile() (cfgPath, cfgLevel, cfgFormat string) {
	cfgPath = defaultLogPath
	cfgLevel = defaultLogLevel
	cfgFormat = defaultLogFormat
	cfg, err := ini.Load(configFileName)
	if err != nil {
		log.Println("Failed to open config file")
		return cfgPath, cfgLevel, cfgFormat
	}
	if cfg.Section("").HasKey(path) {
		cfgPath = cfg.Section("").Key(path).String()
	}
	if cfg.Section("").HasKey(level) {
		cfgLevel = strings.ToLower(cfg.Section("").Key(level).String())
	}
	if cfg.Section("").HasKey(format) {
		cfgFormat = cfg.Section("").Key(format).String()
	}

	return cfgPath, cfgLevel, cfgFormat
}

func (f *LogFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	output := f.LogFormat
	if output == "" {
		output = defaultLogFormat
	}

	timestampFormat := f.TimestampFormat
	if timestampFormat == "" {
		timestampFormat = defaultTimestampFormat
	}

	output = strings.Replace(output, "%time%", entry.Time.Format(timestampFormat), 1)

	output = strings.Replace(output, "%message%", entry.Message, 1)

	level := strings.ToUpper(entry.Level.String())
	output = strings.Replace(output, "%level%", level, 1)

	output = strings.Replace(output, "%process%", strconv.Itoa(os.Getpid()), 1)

	pc, filename, line, _ := runtime.Caller(callStackDeep)
	output = strings.Replace(output, "%filename%", filename, 1)
	output = strings.Replace(output, "%lineNo%", strconv.Itoa(line), 1)
	output = strings.Replace(output, "%funcName%", runtime.FuncForPC(pc).Name(), 1)

	return []byte(output), nil
}
