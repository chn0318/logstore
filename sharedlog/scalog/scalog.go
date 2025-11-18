package scalog

import (
	"encoding/json"
	"sync"

	"github.com/chn0318/logstore/sharedlog"
	"github.com/chn0318/scalog/client"
	"github.com/chn0318/scalog/pkg/address"
	"github.com/spf13/viper"
)

type ScalogSystem struct {
	clients []*client.Client

	mu   sync.Mutex
	next int
}

func NewScalogSystem() (*ScalogSystem, error) {
	numReplica := int32(viper.GetInt("data-replication-factor"))
	discPort := uint16(viper.GetInt("disc-port"))
	discIp := viper.GetString("disc-ip")
	discAddr := address.NewGeneralDiscAddr(discIp, discPort)
	dataPort := uint16(viper.GetInt("data-port"))
	dataAddr := address.NewGeneralDataAddr("data-%v-%v-ip", numReplica, dataPort)
	//ToDo: config numClient
	numClients := 100
	if numClients <= 0 {
		numClients = 4
	}

	clients := make([]*client.Client, 0, numClients)
	for i := 0; i < numClients; i++ {
		c, err := client.NewClient(dataAddr, discAddr, numReplica)
		if err != nil {
			// 如果你愿意，也可以在这里把已经创建的 client 依次 Close 掉
			return nil, err
		}
		clients = append(clients, c)
	}

	return &ScalogSystem{
		clients: clients,
	}, nil
}

func (s *ScalogSystem) pickClient() *client.Client {
	s.mu.Lock()
	defer s.mu.Unlock()

	c := s.clients[s.next]
	s.next = (s.next + 1) % len(s.clients)
	return c
}

func (s *ScalogSystem) AppendData(rec sharedlog.DataRecord) (sharedlog.RecordRef, error) {
	data, err := json.Marshal(rec)
	if err != nil {
		return sharedlog.RecordRef{}, err
	}

	c := s.pickClient()

	gsn, sid, err := c.AppendOne(string(data))
	if err != nil {
		return sharedlog.RecordRef{}, err
	}

	return sharedlog.RecordRef{
		GSN:     uint64(gsn),
		ShardID: uint32(sid),
	}, nil
}

func (s *ScalogSystem) AppendCommit(rec sharedlog.CommitRecord) (uint64, error) {
	data, err := json.Marshal(rec)
	if err != nil {
		return 0, err
	}

	c := s.pickClient()

	gsn, _, err := c.AppendOne(string(data))
	if err != nil {
		return 0, err
	}
	return uint64(gsn), nil
}

func (s *ScalogSystem) ReadData(ref sharedlog.RecordRef) (sharedlog.DataRecord, error) {
	rid := int32(0)
	c := s.pickClient()

	data, err := c.Read(int64(ref.GSN), int32(ref.ShardID), rid)
	if err != nil {
		return sharedlog.DataRecord{}, err
	}

	var rec sharedlog.DataRecord
	if err := json.Unmarshal([]byte(data), &rec); err != nil {
		return sharedlog.DataRecord{}, err
	}
	return rec, nil
}
