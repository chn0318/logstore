package storageserver

import (
	"context"

	"github.com/chn0318/logstore/mapservice"
	"github.com/chn0318/logstore/proto/storagepb"
	"github.com/chn0318/logstore/sharedlog"
)

type StorageServer struct {
	storagepb.UnimplementedStorageServer
	sharedLog  sharedlog.SharedLog
	mapService *mapservice.MapService
}

func NewStorageServer(sharedLog sharedlog.SharedLog, mapService *mapservice.MapService) *StorageServer {
	return &StorageServer{
		sharedLog:  sharedLog,
		mapService: mapService,
	}
}

func (s *StorageServer) MultiPut(ctx context.Context, req *storagepb.MultiPutRequest) (*storagepb.MultiPutResponse, error) {
	commitEntries := make([]sharedlog.CommitEntry, 0, len(req.Kvs))

	for _, kv := range req.Kvs {
		dataRecord := sharedlog.DataRecord{
			Key:   kv.Key,
			Value: kv.Value,
		}

		dataGSN, err := s.sharedLog.AppendData(dataRecord)
		if err != nil {
			return nil, err
		}

		commitEntries = append(commitEntries, sharedlog.CommitEntry{
			Key:     kv.Key,
			DataGSN: dataGSN,
		})
	}

	commitGSN, err := s.sharedLog.AppendCommit(sharedlog.CommitRecord{
		Entries: commitEntries,
	})
	if err != nil {
		return nil, err
	}

	msEntries := make([]mapservice.CommitEntry, 0, len(commitEntries))
	for _, e := range commitEntries {
		msEntries = append(msEntries, mapservice.CommitEntry{
			Key:     e.Key,
			DataGSN: e.DataGSN,
		})
	}
	s.mapService.ApplyCommit(commitGSN, msEntries)

	return &storagepb.MultiPutResponse{
		Ok: true,
	}, nil
}

func (s *StorageServer) MultiGet(ctx context.Context, req *storagepb.MultiGetRequest) (*storagepb.MultiGetResponse, error) {
	offsets := s.mapService.GetOffsets(req.Keys)

	res := &storagepb.MultiGetResponse{
		Values: make(map[string][]byte, len(offsets)),
	}

	for _, key := range req.Keys {
		dataGSN, ok := offsets[key]
		if !ok {
			continue
		}

		dataRec, err := s.sharedLog.ReadData(dataGSN)
		if err != nil {
			return nil, err
		}

		res.Values[key] = dataRec.Value
	}

	return res, nil
}
