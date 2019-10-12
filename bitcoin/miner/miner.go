package main

import (
	"DistributedBitcoinMiner/bitcoin"
	"bufio"
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
		name = "/home/usman/go/src/DistributedBitcoinMiner/bitcoin/miner/log.txt"
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
	writer := bufio.NewWriter(miner)
	reader := bufio.NewReader(miner)

	join := bitcoin.NewJoin()
	messageAsBytes, err := json.Marshal(join)
	if err != nil {
		LOGF.Println("Error marshalling Join:", err)
		return
	}
	_, err = writer.Write(messageAsBytes)
	if err != nil {
		LOGF.Println("Error writing Join:", err)
		return
	}
	err = writer.Flush()
	if err != nil {
		LOGF.Println("Error flushing Join:", err)
		return
	}
	LOGF.Println("SENT:", join.String())

	for {
		readBuffer := make([]byte, 1024)
		readLen, err := reader.Read(readBuffer)
		if err != nil {
			LOGF.Println("Error reading Request:", err)
			return
		}
		var request bitcoin.Message
		err = json.Unmarshal(readBuffer[:readLen], &request)
		if err != nil {
			LOGF.Println("Error unmarshalling Request:", err)
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
		messageAsBytes, err := json.Marshal(result)
		if err != nil {
			LOGF.Println("Error marshalling Result:", err)
			return
		}
		_, err = writer.Write(messageAsBytes)
		if err != nil {
			LOGF.Println("Error writing Result:", err)
			return
		}
		err = writer.Flush()
		if err != nil {
			LOGF.Println("Error flushing Result:", err)
			return
		}
		LOGF.Println("SENT:", result.String())
	}
}
