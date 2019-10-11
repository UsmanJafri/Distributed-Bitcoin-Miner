package main

import (
	"fmt"
	"net"
	"os"
)

// Attempt to connect miner as a client to the server.
func joinWithServer(hostport string) (net.Conn, error) {
	// TODO: implement this!

	return nil, nil
}

func main() {
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <hostport>", os.Args[0])
		return
	}

	hostport := os.Args[1]
	miner, err := joinWithServer(hostport)
	if err != nil {
		fmt.Println("Failed to join with server:", err)
		return
	}

	defer miner.Close()

	// TODO: implement this!
}
