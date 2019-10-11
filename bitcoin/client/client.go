package main

import (
	"DistributedBitcoinMiner/bitcoin"
	"bufio"
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
		name = "/home/usman/go/src//DistributedBitcoinMiner/bitcoin/client/log.txt"
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

	reader := bufio.NewReader(client)
	writer := bufio.NewWriter(client)

	request := bitcoin.NewRequest(message, 0, maxNonce)
	messageAsBytes, err := json.Marshal(request)
	if err != nil {
		LOGF.Println("Error marshalling Request:", err)
		return
	}
	_, err = writer.Write(messageAsBytes)
	if err != nil {
		LOGF.Println("Error writing Request:", err)
		return
	}
	err = writer.Flush()
	if err != nil {
		LOGF.Println("Error flushing Request:", err)
		return
	}
	LOGF.Println("SENT:", request.String())

	readBuffer := make([]byte, 1024)
	readLen, err := reader.Read(readBuffer)
	if err != nil {
		LOGF.Println("Error reading Result:", err)
		printDisconnected()
		return
	}
	var result bitcoin.Message
	err = json.Unmarshal(readBuffer[:readLen], &result)
	if err != nil {
		LOGF.Println("Error unmarshalling Result:", err)
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
