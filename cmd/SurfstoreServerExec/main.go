package main

import (
	"cse224/proj5/pkg/surfstore"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"google.golang.org/grpc"
)

// Usage String
const USAGE_STRING = "./run-server.sh -s <service_type> -p <port> -l -d (blockStoreAddr*)"

// Set of valid services
var SERVICE_TYPES = map[string]bool{"meta": true, "block": true, "both": true}

// Exit codes
const EX_USAGE int = 64

func main() {
	// Custom flag Usage message
	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintf(w, "Usage of %s:\n", USAGE_STRING)
		flag.VisitAll(func(f *flag.Flag) {
			fmt.Fprintf(w, "  -%s: %v\n", f.Name, f.Usage)
		})
		fmt.Fprintf(w, "  (blockStoreAddr*): BlockStore Address (include self if service type is both)\n")
	}

	// Parse command-line argument flags
	service := flag.String("s", "", "(required) Service Type of the Server: meta, block, both")
	port := flag.Int("p", 8080, "(default = 8080) Port to accept connections")
	localOnly := flag.Bool("l", false, "Only listen on localhost")
	debug := flag.Bool("d", false, "Output log statements")
	flag.Parse()

	// Use tail arguments to hold BlockStore address
	args := flag.Args()
	blockStoreAddrs := make([]string, 0)
	// if len(args) == 1 {
	// 	blockStoreAddr = args[0]
	// }
	for _, arg := range args {
		blockStoreAddrs = append(blockStoreAddrs, arg)
	}
	// log.Println("received server args: ", blockStoreAddrs)
	// Valid service type argument
	if _, ok := SERVICE_TYPES[strings.ToLower(*service)]; !ok {
		flag.Usage()
		os.Exit(EX_USAGE)
	}

	// Add localhost if necessary
	addr := ""
	if *localOnly {
		addr += "localhost"
	}
	addr += ":" + strconv.Itoa(*port)

	// Disable log outputs if debug flag is missing
	if !(*debug) {
		log.SetFlags(0)
		log.SetOutput(io.Discard)
	}

	log.Fatal(startServer(addr, strings.ToLower(*service), blockStoreAddrs))
}

func startServer(hostAddr string, serviceType string, blockStoreAddrs []string) error {
	grpc_server := grpc.NewServer()
	// this does not cause delay
	meta_store := surfstore.NewMetaStore(blockStoreAddrs)
	block_store := surfstore.NewBlockStore()
	if serviceType == "meta" {
		surfstore.RegisterMetaStoreServer(grpc_server, meta_store)
	} else if serviceType == "block" {
		surfstore.RegisterBlockStoreServer(grpc_server, block_store)
	} else if serviceType == "both" {
		surfstore.RegisterMetaStoreServer(grpc_server, meta_store)
		surfstore.RegisterBlockStoreServer(grpc_server, block_store)
	} else {
		return errors.New("wrong service type")
	}
	log.Println("reached here")
	listener, err := net.Listen("tcp", hostAddr)
	log.Println("reached here 2")
	if err != nil {
		log.Println("got error while creating tcp listener")
		return err
	}
	log.Println("reached here 3")
	err = grpc_server.Serve(listener)
	log.Println("reached here 4")
	if err != nil {
		log.Println("got error while creating grpc_server")
		return err
	}
	return nil
}
