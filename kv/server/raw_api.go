package server

import (
	"bytes"
	"context"
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	var err error
	reader, err := server.storage.Reader(nil)
	defer reader.Close()

	if err != nil {
		return nil, err
	}
	value, err := reader.GetCF(req.Cf, req.Key)

	resp := &kvrpcpb.RawGetResponse{
		Value:    value,
		NotFound: value == nil,
	}
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	var err error
	err = server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Put{
				Cf:    req.Cf,
				Key:   req.Key,
				Value: req.Value,
			},
		},
	})
	resp := new(kvrpcpb.RawPutResponse)
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	var err error
	err = server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Delete{
				Cf:  req.Cf,
				Key: req.Key,
			},
		},
	})
	resp := new(kvrpcpb.RawDeleteResponse)
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	var err error
	reader, err := server.storage.Reader(nil)
	defer reader.Close()
	if err != nil {
		return nil, err
	}
	// response data
	var kvs []*kvrpcpb.KvPair
	var errorString string
	iter := reader.IterCF(req.Cf)
	iter.Seek([]byte{})
	defer iter.Close()
	// find the position of start key
	for iter.Valid() {
		if bytes.Equal(iter.Item().Key(), req.StartKey) {
			break
		}
		iter.Next()
	}
	if !iter.Valid() {
		errorString = fmt.Sprintf("cannot find the start key ('%s') in cf '%s'", req.StartKey, req.Cf)
	} else {
		var cnt uint32 = 0
		for {
			// current iter is always valid
			value, _ := iter.Item().Value()
			kvs = append(kvs, &kvrpcpb.KvPair{
				Key:   iter.Item().Key(),
				Value: value,
			})
			cnt++
			if cnt == req.Limit {
				break
			}
			iter.Next()
			if !iter.Valid() {
				//errorString = "limit exceed the actual quantity of keys."
				break
			}
		}
	}

	return &kvrpcpb.RawScanResponse{
		Error: errorString,
		Kvs:   kvs,
	}, nil
}
