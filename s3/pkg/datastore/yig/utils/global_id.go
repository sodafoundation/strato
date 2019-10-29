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
	// lower bits of ip address.
	MachineId int64
	startTime int64
	seq       int64
	mux       sync.Mutex
}

func NewGlobalIdGen() (*GlobalIdGen, error) {
	gi := &GlobalIdGen{
		seq: 0,
	}
	id, err := gi.getMachineId()
	if err != nil {
		return nil, err
	}
	gi.MachineId = id
	return gi, nil
}

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
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			ip := ipnet.IP.To4()
			if ip != nil && (ip[0] == 10 || ip[0] == 172 && (ip[1] >= 16 && ip[1] < 32) || ip[0] == 192 && ip[1] == 168) {
				return (int64(ip[2])<<8 + int64(ip[3])) & 0x0fff, nil
			}
		}
	}

	return 0, errors.New("failed to find the private ip addr")
}
