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

type server struct {
	listener    net.Listener
	clientJoin  chan client
	minerJoin   chan miner
	minerResult chan bitcoin.Message
}

type miner struct {
	toMiner chan bitcoin.Message
}

type client struct {
	job      bitcoin.Message
	toClient chan bitcoin.Message
	assigned bool
}

func startServer(port int) (*server, error) {
	srv := new(server)
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	srv.listener = listener
	srv.clientJoin = make(chan client)
	srv.minerJoin = make(chan miner)
	srv.minerResult = make(chan bitcoin.Message)
	return srv, err
}

var LOGF *log.Logger

func main() {
	// You may need a logger for debug purpose
	const (
		name = "/home/usman/go/src/DistributedBitcoinMiner/bitcoin/server/log.txt"
		flag = os.O_RDWR | os.O_CREATE | os.O_TRUNC
		perm = os.FileMode(0666)
	)

	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return
	}
	defer file.Close()

	LOGF = log.New(file, "", log.Lshortfile|log.Lmicroseconds)
	// Usage: LOGF.Println() or LOGF.Printf()

	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <port>", os.Args[0])
		return
	}

	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Port must be a number:", err)
		return
	}

	srv, err := startServer(port)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("Server listening on port", port)

	defer srv.listener.Close()

	go srv.scheduler()
	srv.acceptAndIdentify()
}

func (srv *server) acceptAndIdentify() {
	for {
		conn, err := srv.listener.Accept()
		if err != nil {
			LOGF.Println("Error accepting connection:", err)
			return
		}
		reader := bufio.NewReader(conn)
		readBuffer := make([]byte, 1024)
		readLen, err := reader.Read(readBuffer)
		if err != nil {
			LOGF.Println("Error reading first message:", err)
			return
		}
		var message bitcoin.Message
		err = json.Unmarshal(readBuffer[:readLen], &message)
		if err != nil {
			LOGF.Println("Error unmarshalling first message:", err)
			return
		}
		if message.Type == bitcoin.Join {
			toMiner := make(chan bitcoin.Message)
			go minerHandler(conn, reader, toMiner, srv.minerResult)
			srv.minerJoin <- miner{toMiner}
		} else if message.Type == bitcoin.Request {
			toClient := make(chan bitcoin.Message)
			srv.clientJoin <- client{message, toClient, false}
			go clientHandler(conn, toClient)
		}
	}

}

func clientHandler(conn net.Conn, toClient chan bitcoin.Message) {
	writer := bufio.NewWriter(conn)
	message := <-toClient
	messageAsBytes, err := json.Marshal(message)
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
	LOGF.Println("RESULT TO CLIENT:", message.String())
	conn.Close()
}

func minerHandler(conn net.Conn, reader *bufio.Reader, toMiner chan bitcoin.Message, toScheduler chan bitcoin.Message) {
	defer conn.Close()
	writer := bufio.NewWriter(conn)
	for {
		message := <-toMiner
		messageAsBytes, err := json.Marshal(message)
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
		LOGF.Println("SENT TO MINER:", message.String())

		readBuffer := make([]byte, 1024)
		readLen, err := reader.Read(readBuffer)
		if err != nil {
			LOGF.Println("Error reading Result:", err)
			return
		}
		var result bitcoin.Message
		err = json.Unmarshal(readBuffer[:readLen], &result)
		if err != nil {
			LOGF.Println("Error unmarshalling Result:", err)
			return
		}
		toScheduler <- result
		LOGF.Println("RECV FROM MINER:", result.String())
	}
}

func (srv *server) scheduler() {
	clientID := 0
	clients := make(map[int]client)
	minerID := 0
	var miners []miner
	for {
		select {
		case clientJoin := <-srv.clientJoin:
			LOGF.Println("JOB:", clientJoin.job.String())
			if len(miners) > 0 {
				miners[0].toMiner <- clientJoin.job
			}
			clients[clientID] = clientJoin
			clientID++
		case minerJoin := <-srv.minerJoin:
			miners = append(miners, minerJoin)
			minerID++
			for _, client := range clients {
				if !client.assigned {
					client.assigned = true
					miners[0].toMiner <- client.job
				}
			}
		case minerResult := <-srv.minerResult:
			clients[clientID-1].toClient <- minerResult
			delete(clients, clientID-1)
		}
	}

}
