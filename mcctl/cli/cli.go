// Copyright 2019 The OpenSDS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cli

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/opensds/multi-cloud/api/pkg/filters/context"
	"github.com/opensds/multi-cloud/api/pkg/utils"
	c "github.com/opensds/multi-cloud/client"
	"github.com/spf13/cobra"
	yaml "gopkg.in/yaml.v2"
)

var (
	client      *c.Client
	rootCommand = &cobra.Command{
		Use:   "mcctl",
		Short: "Administer the OpenSDS multi-cloud",
		Long:  `Admin utility for the OpenSDS multi-cloud.`,
		Run: func(cmd *cobra.Command, args []string) {
			cmd.Usage()
			os.Exit(1)
		},
	}

	// Debug Is it Debug?
	Debug bool
	// DockerComposePath The name of the environment variable used to set the
	// path to docker-compose.yml
	DockerComposePath = "DOCKER_COMPOSE_PATH"
	// DefaultDockerComposePath The default value of the environment variable DOCKER_COMPOSE_PATH
	DefaultDockerComposePath = "/root/gopath/src/github.com/opensds/multi-cloud/docker-compose.yml"
	// MultiCloudIP The name of the environment variable used to set ip
	MultiCloudIP = "MULTI_CLOUD_IP"
)

// DockerCompose implementation
type DockerCompose struct {
	Version  string `yaml:"version"`
	Services struct {
		Zookeeper struct {
			Image string   `yaml:"image"`
			Ports []string `yaml:"ports"`
		} `yaml:"zookeeper"`
		Kafka struct {
			Image       string   `yaml:"image"`
			Ports       []string `yaml:"ports"`
			Environment []string `yaml:"environment"`
			Volumes     []string `yaml:"volumes"`
			DependsOn   []string `yaml:"depends_on"`
		} `yaml:"kafka"`
		API struct {
			Image       string   `yaml:"image"`
			Volumes     []string `yaml:"volumes"`
			Ports       []string `yaml:"ports"`
			Environment []string `yaml:"environment"`
		} `yaml:"api"`
		Backend struct {
			Image       string   `yaml:"image"`
			Environment []string `yaml:"environment"`
		} `yaml:"backend"`
		S3 struct {
			Image       string   `yaml:"image"`
			Environment []string `yaml:"environment"`
		} `yaml:"s3"`
		Dataflow struct {
			Image       string   `yaml:"image"`
			Environment []string `yaml:"environment"`
			DependsOn   []string `yaml:"depends_on"`
		} `yaml:"dataflow"`
		Datamover struct {
			Image       string   `yaml:"image"`
			Volumes     []string `yaml:"volumes"`
			Environment []string `yaml:"environment"`
			DependsOn   []string `yaml:"depends_on"`
		} `yaml:"datamover"`
		Datastore struct {
			Image string   `yaml:"image"`
			Ports []string `yaml:"ports"`
		} `yaml:"datastore"`
	} `yaml:"services"`
}

func init() {
	rootCommand.AddCommand(backendCommand)
	rootCommand.AddCommand(bucketCommand)
	rootCommand.AddCommand(objectCommand)
	rootCommand.AddCommand(planCommand)
	rootCommand.AddCommand(policyCommand)
	rootCommand.AddCommand(jobCommand)
	rootCommand.AddCommand(typeCommand)
	rootCommand.AddCommand(storageClassesCommand)

	flags := rootCommand.PersistentFlags()
	flags.BoolVar(&Debug, "debug", false, "shows debugging output.")
}

// DummyWriter implementation
type DummyWriter struct{}

// do nothing
func (writer DummyWriter) Write(data []byte) (n int, err error) {
	return len(data), nil
}

// DebugWriter implementation
type DebugWriter struct{}

// do nothing
func (writer DebugWriter) Write(data []byte) (n int, err error) {
	Debugf("%s", string(data))
	return len(data), nil
}

// GetAPIEnvs Get api.environment in docker-compose.yml
func GetAPIEnvs() []string {
	path, ok := os.LookupEnv(DockerComposePath)
	if !ok {
		path = DefaultDockerComposePath
	}

	ymlFile, err := ioutil.ReadFile(path)
	if err != nil {
		log.Printf("Read config yaml file (%s) failed, reason:(%v)", path, err)
		return nil
	}

	DockerComposeConf := &DockerCompose{}
	if err = yaml.Unmarshal(ymlFile, DockerComposeConf); err != nil {
		log.Printf("Parse error: %v", err)
		return nil
	}

	return DockerComposeConf.Services.API.Environment
}

// Run method indicates how to start a cli tool through cobra.
func Run() error {
	if !utils.Contained("--debug", os.Args) {
		log.SetOutput(DummyWriter{})
	} else {
		log.SetOutput(DebugWriter{})
	}

	ip, ok := os.LookupEnv(MultiCloudIP)
	if !ok {
		return fmt.Errorf("ERROR: You must provide the ip by setting " +
			"the environment variable MULTI_CLOUD_IP")
	}

	cfg := &c.Config{
		Endpoint: "http://" + ip + os.Getenv(c.MicroServerAddress),
	}
	authStrategy := os.Getenv(c.OsAuthAuthstrategy)

	switch authStrategy {
	case c.Keystone:
		cfg.AuthOptions = c.LoadKeystoneAuthOptions()
	case c.Noauth:
		cfg.AuthOptions = c.NewNoauthOptions(context.NoAuthAdminTenantId)
	default:
		cfg.AuthOptions = c.NewNoauthOptions(context.DefaultTenantId)
	}

	client = c.NewClient(cfg)

	return rootCommand.Execute()
}
