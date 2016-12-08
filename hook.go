package elogrus

import (
	"fmt"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"

	"golang.org/x/net/context"
	"gopkg.in/olivere/elastic.v5"
	"runtime"
	"path"
	"os"
)

var (
	// Fired if the
	// index is not created
	ErrCannotCreateIndex = fmt.Errorf("Cannot create index")
)

// ElasticHook is a logrus
// hook for ElasticSearch
type ElasticHook struct {
	client    *elastic.Client
	host      string
	service   string
	version   string
	index     string
	levels    []logrus.Level
	ctx       context.Context
	ctxCancel context.CancelFunc
}

type Log struct {
	Service   string
	Version	  string
	Host      string
	File   	  string
	FuncName  string
	Line	  int
	Timestamp string
	Message   string
	Level     logrus.Level
	Data      logrus.Fields
}

// NewElasticHook creates new hook
// client - ElasticSearch client using gopkg.in/olivere/elastic.v5
// host - host of system
// level - log level
// index - name of the index in ElasticSearch
func NewElasticHook(client *elastic.Client, service string, version string, level logrus.Level, index string) (*ElasticHook, error) {
	levels := []logrus.Level{}
	for _, l := range []logrus.Level{
		logrus.PanicLevel,
		logrus.FatalLevel,
		logrus.ErrorLevel,
		logrus.WarnLevel,
		logrus.InfoLevel,
		logrus.DebugLevel,
	} {
		if l <= level {
			levels = append(levels, l)
		}
	}

	ctx, cancel := context.WithCancel(context.TODO())

	// Use the IndexExists service to check if a specified index exists.
	exists, err := client.IndexExists(index).Do(ctx)
	if err != nil {
		// Handle error
		return nil, err
	}
	if !exists {
		createIndex, err := client.CreateIndex(index).Do(ctx)
		if err != nil {
			return nil, err
		}
		if !createIndex.Acknowledged {
			return nil, ErrCannotCreateIndex
		}
	}
	hostname, _  := os.Hostname()

	return &ElasticHook{
		client:    client,
		host:      hostname,
		service:   service,
		version:   version,
		index:     index,
		levels:    levels,
		ctx:       ctx,
		ctxCancel: cancel,
	}, nil
}

func getCallee(level int) (string, string, int) {
	for i := 0; i < 10; i++ {
		if pc, file, line, ok := runtime.Caller(level); ok {
			funcName := path.Base(runtime.FuncForPC(pc).Name())
			//fmt.Println(funcName, file, line)
			if !strings.HasPrefix(funcName, "logrus") && !strings.HasPrefix(funcName, "PocketLogger") {
				return file, funcName, line
			}
		} else {
			break
		}
		level++
	}

	return "", "", 0
}

// Fire is required to implement
// Logrus hook
func (hook *ElasticHook) Fire(entry *logrus.Entry) error {
	file, funcName, line := getCallee(7)

	entry.Data["file"] = path.Base(file)
	entry.Data["func"] = path.Base(funcName)
	entry.Data["line"] = line

	msg := Log{
		Service: hook.service,
		Version: hook.version,
		Host: hook.host,
		File: path.Base(file),
		FuncName: path.Base(funcName),
		Line: line,
		Timestamp: entry.Time.UTC().Format(time.RFC3339Nano),
		Level: entry.Level,
		Message: entry.Message,
		Data: entry.Data,
	}

	_, err := hook.client.
		Index().
		Index(hook.index).
		Type("log").
		BodyJson(msg).
		Do(hook.ctx)
	return err
}

// Required for logrus
// hook implementation
func (hook *ElasticHook) Levels() []logrus.Level {
	return hook.levels
}

// Cancels all calls to
// elastic
func (hook *ElasticHook) Cancel() {
	hook.ctxCancel()
}
