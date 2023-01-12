package server

import (
	"context"
	"encoding/json"
	"github.com/valerylobachev/cluster-sharding-proto/sharding"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/rest/httpc"
	"github.com/zeromicro/go-zero/rest/httpx"
	"log"
	"net/http"
)

type ZeroServer struct {
	sm     *sharding.ShardManager
	engine *rest.Server
}

func StartZeroServer(port int, sm *sharding.ShardManager) (*ZeroServer, error) {
	engine := rest.MustNewServer(rest.RestConf{
		ServiceConf: service.ServiceConf{
			Log: logx.LogConf{
				Mode:  "console",
				Level: "error",
			},
		},
		Port:     port,
		MaxConns: 500,
	})
	server := &ZeroServer{
		sm:     sm,
		engine: engine,
	}
	engine.AddRoute(rest.Route{
		Method:  http.MethodPost,
		Path:    "/process",
		Handler: server.handler,
	})
	log.Println("Start server")
	go func() {
		engine.Start()
	}()
	return server, nil
}

func (s *ZeroServer) handler(w http.ResponseWriter, r *http.Request) {
	var req = new(Request)
	err := httpx.Parse(r, req)
	if err != nil {
		log.Printf("failed to decode request")
		res := Response{
			Key:    "",
			Server: "",
			Res:    "",
			Err:    err.Error(),
		}
		httpx.OkJson(w, res)
		return
	}

	node, local := s.sm.GetNode(req.Key)
	if local {
		res := requestLocal(s.sm, node, req)
		httpx.OkJson(w, res)
	} else {
		res := s.requestSibling(node, req)
		httpx.OkJson(w, res)
	}
}

func (s *ZeroServer) requestSibling(node string, req *Request) *Response {
	addr := getSiblingAddr(node)
	resp, err := httpc.Do(context.Background(), http.MethodPost, addr, req)
	if err != nil {
		return &Response{
			Err: err.Error(),
		}
	}
	res := new(Response)
	json.NewDecoder(resp.Body).Decode(res)
	return res
}

func (s *ZeroServer) Stop() error {
	s.engine.Stop()
	return nil
}
