package main

import (
	"DistributedBitcoinMiner/bitcoin"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
)

// Attempt to connect miner as a client to the server.
func joinWithServer(hostport string) (net.Conn, error) {
	return net.Dial("tcp", hostport)
}

var LOGF *log.Logger

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

	// LOGGER ======================================================
	const (
		name = "/home/usman/go/src/DistributedBitcoinMiner/bitcoin/miner_log.txt"
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

	reader := json.NewDecoder(miner)
	writer := json.NewEncoder(miner)

	join := bitcoin.NewJoin()
	err = writer.Encode(join)
	if err != nil {
		LOGF.Println("Error sending Join:", err)
		return
	}
	LOGF.Println("SENT:", join.String())

	var request bitcoin.Message
	for {
		err := reader.Decode(&request)
		if err != nil {
			LOGF.Println("Error reading Request:", err)
			return
		}
		LOGF.Println("RECV:", request.String())

		minNonce := request.Upper
		minHash := bitcoin.Hash(request.Data, minNonce)
		for nonce := request.Lower; nonce < request.Upper; nonce++ {
			hash := bitcoin.Hash(request.Data, nonce)
			if hash < minHash {
				minNonce = nonce
				minHash = hash
			}
		}

		result := bitcoin.NewResult(minHash, minNonce)
		err = writer.Encode(result)
		if err != nil {
			LOGF.Println("Error sending Result:", err)
			return
		}
		LOGF.Println("SENT:", result.String())
	}
}
