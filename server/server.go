package server

import (
	"encoding/json"
	"fmt"
	"github.com/valerylobachev/cluster-sharding-proto/sharding"
	"log"
	"net"
	"net/http"
)

type Server struct {
	sm       *sharding.ShardManager
	listener net.Listener
}

func StartServer(port int, sm *sharding.ShardManager) (*Server, error) {
	server := &Server{
		sm: sm,
	}
	http.HandleFunc("/process", server.handler)

	log.Println("Start server")
	addr := fmt.Sprintf("0.0.0.0:%d", port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	server.listener = l
	go func() {
		log.Fatal(http.Serve(l, nil))
	}()
	return server, nil
}

func (s *Server) handler(w http.ResponseWriter, r *http.Request) {
	var req Request

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		log.Printf("failed to decode request")
		res := Response{
			Key:    "",
			Server: "",
			Res:    "",
			Err:    err.Error(),
		}
		writeResponse(w, &res)
		return
	}
	//log.Printf("get Request %v", req)

	node, local := s.sm.GetNode(req.Key)
	if local {
		resp := requestLocal(s.sm, node, &req)
		writeResponse(w, resp)
		return
	} else {
		resp := requestSibling(node, &req)
		writeResponse(w, resp)
		return
	}

}

func (s *Server) Stop() error {
	return s.listener.Close()
}
