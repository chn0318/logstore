package scalog

import (
	"encoding/json"

	"github.com/chn0318/logstore/sharedlog"
	"github.com/chn0318/scalog/client"
	"github.com/chn0318/scalog/pkg/address"
	"github.com/spf13/viper"
)

type scalogSystem struct {
	client *client.Client
}

func NewScalogSystem() (*scalogSystem, error) {
	numReplica := int32(viper.GetInt("data-replication-factor"))
	discPort := uint16(viper.GetInt("disc-port"))
	discIp := viper.GetString("disc-ip")
	discAddr := address.NewGeneralDiscAddr(discIp, discPort)
	dataPort := uint16(viper.GetInt("data-port"))
	dataAddr := address.NewGeneralDataAddr("data-%v-%v-ip", numReplica, dataPort)
	client, err := client.NewClient(dataAddr, discAddr, numReplica)
	if err != nil {
		return nil, err
	}
	return &scalogSystem{
		client: client,
	}, err
}

func (s *scalogSystem) AppendData(rec sharedlog.DataRecord) (sharedlog.RecordRef, error) {
	data, err := json.Marshal(rec)
	if err != nil {
		return sharedlog.RecordRef{}, err
	}
	gsn, sid, err := s.client.AppendOne(string(data))
	if err != nil {
		return sharedlog.RecordRef{}, err
	}
	return sharedlog.RecordRef{
		GSN:     uint64(gsn),
		ShardID: uint32(sid),
	}, nil
}

func (s *scalogSystem) AppendCommit(rec sharedlog.CommitRecord) (uint64, error) {
	data, err := json.Marshal(rec)
	if err != nil {
		return 0, err
	}
	gsn, _, err := s.client.AppendOne(string(data))
	if err != nil {
		return 0, nil
	}
	return uint64(gsn), nil
}

func (s *scalogSystem) ReadData(ref sharedlog.RecordRef) (sharedlog.DataRecord, error) {
	rid := int32(0)
	data, err := s.client.Read(int64(ref.GSN), int32(ref.ShardID), rid)
	if err != nil {
		return sharedlog.DataRecord{}, err
	}
	var rec sharedlog.DataRecord
	err = json.Unmarshal([]byte(data), &rec)
	if err != nil {
		return sharedlog.DataRecord{}, err
	}
	return rec, nil
}
