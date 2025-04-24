package main

import (
	api "Wx_MQ/kitex_gen/api"
	"context"
)

// Client_OperationsImpl implements the last service interface defined in the IDL.
type Client_OperationsImpl struct{}

// Pingpong implements the Client_OperationsImpl interface.
func (s *Client_OperationsImpl) Pingpong(ctx context.Context, req *api.PingpongRequest) (resp *api.PingpongResponse, err error) {
	// TODO: Your code here...
	return
}
