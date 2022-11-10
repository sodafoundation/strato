// Copyright 2019 The soda Authors.
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

package utils

import (
	"errors"
	"net"
	"sync"
	"time"
)

const (
	START_EPOCH     int64 = 1571385784722
	MACHINE_LEN     uint8 = 10
	SEQ_LEN         uint8 = 12
	TIMESTAMP_SHIFS uint8 = 22
	MAX_MACHINE_NUM int64 = 1023
	MAX_SEQ_NUM     int64 = 4095
)

type GlobalIdGen struct {
	MachineId int64
	startTime int64
	seq       int64
	mux       sync.Mutex
}

/*
* machineId: a number which specify the id of the current node.
* if machineId <=0, we will use lower bits of ip address for the node.
*
 */

func NewGlobalIdGen(machineId int64) (*GlobalIdGen, error) {
	gi := &GlobalIdGen{
		seq: 0,
	}
	if machineId <= 0 {
		id, err := gi.getMachineId()
		if err != nil {
			return nil, err
		}
		gi.MachineId = id
	} else {
		gi.MachineId = machineId
	}
	return gi, nil
}

/*
* the global id is generated according to snowflake algo.
* Please refer to https://github.com/twitter-archive/snowflake/tree/snowflake-2010.
 */

func (gi *GlobalIdGen) GetId() int64 {
	gi.mux.Lock()
	defer gi.mux.Unlock()
	current := time.Now().UnixNano() / 1000000
	if gi.startTime == current {
		gi.seq++
		if gi.seq > MAX_SEQ_NUM {
			// process the overflow
			for current <= gi.startTime {
				current = time.Now().UnixNano() / 1000000
			}
			gi.seq = 0
		}
	}
	gi.startTime = current
	id := int64((current-START_EPOCH)<<TIMESTAMP_SHIFS | (gi.MachineId << MACHINE_LEN) | (gi.seq))
	return id
}

func (gi *GlobalIdGen) getMachineId() (int64, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return 0, err
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipNet, ok := address.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			ipAddr := ipNet.IP.To4()
			if ipAddr != nil {
				return (int64(ipAddr[2])<<8 + int64(ipAddr[3])) & 0x0fff, nil
			}
		}
	}

	return 0, errors.New("failed to find the private ip addr")
}
