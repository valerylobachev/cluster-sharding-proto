package main

import (
	"context"
	"fmt"
	"github.com/urfave/cli"
	"github.com/valerylobachev/cluster-sharding-proto/counter"
	"github.com/valerylobachev/cluster-sharding-proto/server"
	"github.com/valerylobachev/cluster-sharding-proto/sharding"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func action(c *cli.Context) {
	dsn := "postgresql://postgres:postgres@localhost:5432/counter?sslmode=disable"
	factory, err := counter.NewCounterFactory(dsn)
	if err != nil {
		log.Fatal(err)
	}
	port := c.Int("port")
	seed := c.String("join")
	shardMan, err := sharding.NewShardManager(port, seed, factory)
	if err != nil {
		log.Fatal(err)
	}

	srv, err := server.StartZeroServer(port+100, shardMan)

	stopCtx, cancel := context.WithCancel(context.TODO())
	go waitSignal(cancel)

	tick := time.NewTicker(10 * time.Second)
	run := true
	for run {
		select {
		case <-tick.C:
			fmt.Println("Tick")
			//members := shardMan.List.Members()
			//for _, m := range members {
			//	fmt.Println(m.Name, m.Addr, m.Port)
			//}
			shardMan.LogKeys()
		case <-stopCtx.Done():
			log.Printf("stop called")
			run = false
		}
	}
	tick.Stop()
	srv.Stop()
	shardMan.Leave(100 * time.Millisecond)
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
