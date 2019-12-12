package config

import (
	"os"
	"path/filepath"

	"github.com/spf13/viper"
)

type FuncConfigParse func(config *Config) error

func ReadCommonConfig(dir string) (*CommonConfig, error) {
	viper.AddConfigPath(dir)
	viper.SetConfigName("common")
	err := viper.ReadInConfig()
	if err != nil {
		return nil, err
	}
	cc := &CommonConfig{}
	err = cc.Parse()
	if err != nil {
		return nil, err
	}
	return cc, nil
}

func ReadConfigs(dir string, funcConfigParse FuncConfigParse) error {
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		if info.Name() == "common.toml" {
			return nil
		}

		viper.SetConfigFile(path)
		err = viper.ReadInConfig()
		if err != nil {
			// skip the config file which is failed to parse and continue the next one.
			return nil
		}
		config := &Config{}
		err = config.Parse()
		if err != nil {
			return err
		}
		err = funcConfigParse(config)
		if err != nil {
			return err
		}
		return nil
	})

	return err
}
