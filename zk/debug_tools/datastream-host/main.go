package main

import (
	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	"flag"
	log2 "github.com/0xPolygonHermez/zkevm-data-streamer/log"
	"fmt"
	"os"
	"os/signal"
)

var file = ""

func main() {
	flag.StringVar(&file, "file", "", "datastream file")
	flag.Parse()

	logConfig := &log2.Config{
		Environment: "production",
		Level:       "info",
		Outputs:     []string{"stdout"},
	}

	stream, err := datastreamer.NewServer(uint16(6900), uint8(3), 1, datastreamer.StreamType(1), file, logConfig)
	if err != nil {
		fmt.Println("Error creating datastream server:", err)
		return
	}

	go func() {
		err := stream.Start()
		if err != nil {
			fmt.Println("Error starting datastream server:", err)
			return
		}
	}()
	fmt.Println("Datastream server started")

	// listen for sigint to exit
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	<-signals

	fmt.Println("Shutting down datastream server")
}
