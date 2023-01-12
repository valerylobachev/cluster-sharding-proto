package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/valerylobachev/cluster-sharding-proto/sharding"
	"net/http"
	"strconv"
	"strings"
)

func requestLocal(sm *sharding.ShardManager, node string, req *Request) *Response {
	ans, err := sm.Ask(req.Key, req.Msg)
	res := Response{
		Key:    req.Key,
		Server: node,
		Res:    ans,
		Err:    "",
	}
	if err != nil {
		res.Err = err.Error()
	}
	return &res
}

func requestSibling(node string, req *Request) *Response {
	addr := getSiblingAddr(node)
	buf := bytes.Buffer{}
	json.NewEncoder(&buf).Encode(req)

	res := new(Response)
	resp, err := http.Post(addr, "application/json", &buf)
	if err != nil {
		res.Key = req.Key
		res.Server = node
		res.Err = err.Error()
		return res
	}
	json.NewDecoder(resp.Body).Decode(res)
	return res
}

func getSiblingAddr(node string) string {
	split := strings.Split(node, ":")
	host := split[0]
	port, _ := strconv.Atoi(split[1])
	return fmt.Sprintf("http://%s:%d/process", host, port+100)
}

func writeResponse(w http.ResponseWriter, res *Response) {
	w.Header().Set("Content-Type", "application/json")
	if len(res.Err) == 0 {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
	json.NewEncoder(w).Encode(res)
}
