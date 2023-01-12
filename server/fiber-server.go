package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	fiber "github.com/gofiber/fiber/v2"
	"github.com/valerylobachev/cluster-sharding-proto/sharding"
	"log"
	"net/http"
)

type FiberServer struct {
	sm  *sharding.ShardManager
	app *fiber.App
}

func StartFiberServer(port int, sm *sharding.ShardManager) (*FiberServer, error) {
	app := fiber.New()
	server := &FiberServer{
		sm:  sm,
		app: app,
	}
	app.Post("/process", server.handler)
	log.Println("Start server")
	addr := fmt.Sprintf("0.0.0.0:%d", port)
	go func() {
		log.Fatal(app.Listen(addr))
	}()
	return server, nil
}

func (s *FiberServer) handler(c *fiber.Ctx) error {
	var req = new(Request)

	if err := c.BodyParser(req); err != nil {
		log.Printf("failed to decode request")
		res := Response{
			Key:    "",
			Server: "",
			Res:    "",
			Err:    err.Error(),
		}
		c.JSON(res)
		return nil
	}

	node, local := s.sm.GetNode(req.Key)
	if local {
		res := requestLocal(s.sm, node, req)
		c.JSON(res)
		return nil
	} else {
		res := requestSibling(node, req)
		c.JSON(res)
		return nil
	}
}

func (s *FiberServer) requestSibling(node string, req *Request) *Response {
	addr := getSiblingAddr(node)
	buf := bytes.Buffer{}
	json.NewEncoder(&buf).Encode(req)

	r, _ := http.NewRequest("POST", addr, &buf)

	client := &http.Client{}
	resp, _ := client.Do(r)
	res := new(Response)
	json.NewDecoder(resp.Body).Decode(res)
	return res
}

func (s *FiberServer) Stop() error {
	return s.app.Shutdown()
}
