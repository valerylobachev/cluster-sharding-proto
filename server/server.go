package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/valerylobachev/cluster-sharding-proto/sharding"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
)

type Request struct {
	Key string `json:"key"`
	Msg string `json:"msg"`
}

type Response struct {
	Key    string `json:"key"`
	Server string `json:"server"`
	Res    string `json:"res"`
	Err    string `json:"err,omitempty"`
}

type Server struct {
	sm       *sharding.ShardManager
	listener net.Listener
}

func StartServer(port int, sm *sharding.ShardManager) (*Server, error) {
	server := &Server{
		sm: sm,
	}
	http.HandleFunc("/process", server.Handler)

	log.Println("Starting server...")
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

func (s *Server) Handler(w http.ResponseWriter, r *http.Request) {
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
		writeResponse(w, res)
		return
	}
	log.Printf("get Request %v", req)

	node, local := s.sm.GetNode(req.Key)
	if local {
		ans, err := s.sm.Ask(req.Key, req.Msg)
		res := Response{
			Key:    req.Key,
			Server: node,
			Res:    ans,
			Err:    "",
		}
		if err != nil {
			res.Err = err.Error()
		}
		writeResponse(w, res)
		return
	}

	requestSibling(node, req, w)
	return

	res := Response{
		Key:    req.Key,
		Server: node,
		Res:    "",
		Err:    fmt.Sprintf("actor located on node %s", node),
	}
	writeResponse(w, res)
}

func requestSibling(node string, req Request, w http.ResponseWriter) {
	addr := getSiblingAddr(node)
	buf := bytes.Buffer{}
	json.NewEncoder(&buf).Encode(req)

	resp, err := http.Post(addr, "application/json", &buf)
	if err != nil {
		res := Response{
			Key:    req.Key,
			Server: node,
			Res:    "",
			Err:    err.Error(),
		}
		writeResponse(w, res)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	io.Copy(w, resp.Body)
}

func getSiblingAddr(node string) string {
	split := strings.Split(node, ":")
	host := split[0]
	port, _ := strconv.Atoi(split[1])
	return fmt.Sprintf("http://%s:%d/process", host, port+100)
}

func writeResponse(w http.ResponseWriter, res Response) {
	w.Header().Set("Content-Type", "application/json")
	if len(res.Err) == 0 {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
	json.NewEncoder(w).Encode(res)
}

func (s *Server) Stop() error {
	return s.listener.Close()
}
