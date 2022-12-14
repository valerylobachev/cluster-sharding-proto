package main

import (
	"context"
	"fmt"
	"github.com/hashicorp/memberlist"
	"github.com/urfave/cli"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

func action(c *cli.Context) {
	port := c.Int("port")
	seed := c.String("join")
	conf := memberlist.DefaultLocalConfig()
	conf.Name = "node" + strconv.Itoa(port%100)
	conf.BindPort = port // avoid port confliction
	conf.AdvertisePort = conf.BindPort
	conf.Events = new(ClusterEventDelegate)

	list, err := memberlist.Create(conf)
	if err != nil {
		log.Fatal(err)
	}

	local := list.LocalNode()
	list.Join([]string{
		fmt.Sprintf("%s:%d", local.Addr.To4().String(), local.Port),
	})

	log.Printf("cluster join to %s", seed)

	if len(seed) != 0 {
		if _, err := list.Join([]string{seed}); err != nil {
			log.Fatal(err)
		}
	}

	stopCtx, cancel := context.WithCancel(context.TODO())
	go waitSignal(cancel)

	tick := time.NewTicker(10 * time.Second)
	run := true
	for run {
		select {
		case <-tick.C:
			fmt.Println("Tick")
			//devt := conf.Events.(*ClusterEventDelegate)
			//if devt == nil {
			//	log.Printf("consistent isnt initialized")
			//	continue
			//}
			//log.Printf("current node size: %d", devt.consistent.Size())
			//
			//keys := []string{"a1", "a2", "a3", "a4", "a5"}
			//for _, key := range keys {
			//	node, ok := devt.consistent.GetNode(key)
			//	if ok == true {
			//		log.Printf("node1 key %s => %s", key, node)
			//	} else {
			//		log.Printf("no node available")
			//	}
			//}
		case <-stopCtx.Done():
			log.Printf("stop called")
			run = false
		}
	}
	tick.Stop()
	log.Printf("bye.")

}

func waitSignal(cancel context.CancelFunc) {
	signal_chan := make(chan os.Signal, 1)
	signal.Notify(signal_chan, syscall.SIGINT)
	for {
		select {
		case s := <-signal_chan:
			log.Printf("signal %s happen", s.String())
			cancel()
		}
	}
}

func main() {

	app := &cli.App{
		Name:  "clushard",
		Usage: "cluster sharding prototype",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:   "join, j",
				Usage:  "cluster join address",
				Value:  "",
				EnvVar: "JOIN_ADDR",
			},
			&cli.IntFlag{
				Name:   "port, p",
				Usage:  "port to listen",
				Value:  8000,
				EnvVar: "PORT",
			},
		},
		Action: action,
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}

}
