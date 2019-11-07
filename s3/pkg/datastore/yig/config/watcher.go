package config

import (
	"errors"
	"strings"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
)

const (
	CFG_SUFFIX = "toml"
)

type ConfigWatcher struct {
	FuncConfigParse FuncConfigParse
	watcher         *fsnotify.Watcher
	stopSignal      chan bool
	wg              sync.WaitGroup
	err             error
}

func (cw *ConfigWatcher) Error() error {
	return cw.err
}

func (cw *ConfigWatcher) Stop() {
	cw.stopSignal <- true
	close(cw.stopSignal)
	cw.wg.Wait()
	cw.watcher.Close()
}

func (cw *ConfigWatcher) Watch(dir string) {
	mask := fsnotify.Write | fsnotify.Create
	cw.wg.Add(1)
	go func() {
		defer cw.wg.Done()
		for {
			select {
			case event, ok := <-cw.watcher.Events:
				if !ok {
					cw.err = errors.New("failed to read watcher events.")
					return
				}
				if event.Op&mask != 0 {
					// got we need.
					if strings.HasSuffix(event.Name, CFG_SUFFIX) {
						viper.SetConfigFile(event.Name)
						err := viper.ReadInConfig()
						if err != nil {
							cw.err = err
							return
						}
						cfg := &Config{}
						err = cfg.Parse()
						if err != nil {
							cw.err = err
							return
						}
						err = cw.FuncConfigParse(cfg)
						if err != nil {
							cw.err = err
							return
						}
					}
				}
			case err := <-cw.watcher.Errors:
				if err != nil {
					cw.err = err
					return
				}
			case stopped := <-cw.stopSignal:
				if stopped {
					cw.err = nil
					return
				}
			}
		}
	}()
}

func NewConfigWatcher(funcConfigParse FuncConfigParse) (*ConfigWatcher, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	cw := &ConfigWatcher{
		FuncConfigParse: funcConfigParse,
		watcher:         watcher,
		stopSignal:      make(chan bool),
		err:             nil,
	}
	return cw, nil
}
