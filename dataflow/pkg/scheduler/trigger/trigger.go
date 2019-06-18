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

package trigger

import (
	"fmt"
)

type Executer interface {
	Run()
}

type Trigger interface {
	Add(planId, properties string, executer Executer) error
	Remove(planId string) error
	Update(planId, properties string, executer Executer) error
}

const (
	TriggerTypeCron = "cron"
)

func GetTrigger(triggerType string) Trigger {
	if trigger, exist := triggers[triggerType]; exist {
		return trigger
	}
	return nil
}

var triggers = map[string]Trigger{}

func RegisterTrigger(triggerType string, trigger Trigger) error {
	if _, exist := triggers[triggerType]; exist {
		return fmt.Errorf("Connector %s already exist.", triggerType)
	}

	triggers[triggerType] = trigger
	return nil
}

func UnregisterTrigger(triggerType string) {
	if _, exist := triggers[triggerType]; !exist {
		return
	}

	delete(triggers, triggerType)
	return
}
