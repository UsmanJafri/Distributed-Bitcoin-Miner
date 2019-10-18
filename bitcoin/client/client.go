package main

import (
	"DistributedBitcoinMiner/bitcoin"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
)

var LOGF *log.Logger

func main() {
	const numArgs = 4
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <hostport> <message> <maxNonce>", os.Args[0])
		return
	}
	hostport := os.Args[1]
	message := os.Args[2]
	maxNonce, err := strconv.ParseUint(os.Args[3], 10, 64)
	if err != nil {
		fmt.Printf("%s is not a number.\n", os.Args[3])
		return
	}

	client, err := net.Dial("tcp", hostport)
	if err != nil {
		fmt.Println("Failed to connect to server:", err)
		return
	}

	defer client.Close()

	// LOGGER ======================================================
	const (
		name = "/home/usman/go/src/DistributedBitcoinMiner/bitcoin/client_log.txt"
		flag = os.O_RDWR | os.O_CREATE | os.O_TRUNC
		perm = os.FileMode(0666)
	)
	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return

	}
	LOGF := log.New(file, "", log.Lshortfile|log.Lmicroseconds)
	defer file.Close()
	// ======================================================

	request := bitcoin.NewRequest(message, 0, maxNonce)
	err = json.NewEncoder(client).Encode(request)
	if err != nil {
		LOGF.Println("Error sending Request:", err)
		return
	}
	LOGF.Println("SENT:", request.String())

	var result bitcoin.Message
	err = json.NewDecoder(client).Decode(&result)
	if err != nil {
		LOGF.Println("Error reading Result:", err)
		printDisconnected()
		return
	}
	LOGF.Println("RECV:", result.String())

	printResult(result.Hash, result.Nonce)
}

// printResult prints the final result to stdout.
func printResult(hash, nonce uint64) {
	fmt.Println("Result", hash, nonce)
}

// printDisconnected prints a disconnected message to stdout.
func printDisconnected() {
	fmt.Println("Disconnected")
}
