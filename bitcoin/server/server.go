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

type server struct {
	listener       net.Listener
	clientJoin     chan client
	clientLeave    chan int
	minerJoin      chan miner
	minerLeave     chan int
	minerResult    chan chunk
	minerQueueSize int
	chunkSize      uint64
}

type miner struct {
	toMiner chan chunk
	conn    net.Conn
	reader  *json.Decoder
}

type client struct {
	ID       int
	toClient chan bitcoin.Message
	job      bitcoin.Message
	conn     net.Conn
	chunks   map[uint64]chunk
}

type chunk struct {
	chunkID  uint64
	clientID int
	minerID  int
	request  bitcoin.Message
	result   *bitcoin.Message
}

func startServer(port int) (*server, error) {
	srv := new(server)
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	srv.listener = listener
	srv.clientJoin = make(chan client)
	srv.clientLeave = make(chan int)
	srv.minerJoin = make(chan miner)
	srv.minerLeave = make(chan int)
	srv.minerResult = make(chan chunk)
	srv.minerQueueSize = 10
	srv.chunkSize = 50000
	return srv, err
}

var logf *log.Logger

func main() {
	// You may need a logger for debug purpose
	const (
		name = "/home/usman/go/src/DistributedBitcoinMiner/bitcoin/server_log.txt"
		flag = os.O_RDWR | os.O_CREATE | os.O_TRUNC
		perm = os.FileMode(0666)
	)

	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return
	}
	defer file.Close()

	logf = log.New(file, "", log.Lshortfile|log.Lmicroseconds)
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
	var message bitcoin.Message
	for {
		conn, err := srv.listener.Accept()
		if err != nil {
			logf.Println("Error accepting connection:", err)
			return
		}
		reader := json.NewDecoder(conn)
		err = reader.Decode(&message)
		if err != nil {
			logf.Println("Error reading first message:", err)
			continue
		}
		if message.Type == bitcoin.Join {
			srv.minerJoin <- miner{make(chan chunk, srv.minerQueueSize), conn, reader}
		} else if message.Type == bitcoin.Request {
			srv.clientJoin <- client{-1, make(chan bitcoin.Message), message, conn, nil}
		}
	}

}

func (myClient *client) clientHandler(leave chan<- int) {
	message := <-myClient.toClient
	err := json.NewEncoder(myClient.conn).Encode(message)
	if err != nil {
		logf.Println("Error sending Result:", err)
	}
	leave <- myClient.ID
	myClient.conn.Close()
}

func (myClient *client) createChunks(chunkSize uint64) {
	myClient.chunks = make(map[uint64]chunk)
	for lower := myClient.job.Lower; lower < myClient.job.Upper; lower += chunkSize {
		upper := lower + chunkSize
		if upper > myClient.job.Upper {
			upper = myClient.job.Upper
		}
		newChunk := chunk{lower, myClient.ID, -1, *bitcoin.NewRequest(myClient.job.Data, lower, upper), nil}
		myClient.chunks[lower] = newChunk
	}
}

func (myMiner *miner) minerHandler(toScheduler chan<- chunk, leave chan<- int, ID int) {
	writer := json.NewEncoder(myMiner.conn)
	for {
		chunk := <-myMiner.toMiner
		err := writer.Encode(chunk.request)
		if err != nil {
			logf.Println("MINER: Error sending Request:", err)
			break
		}

		var result bitcoin.Message
		err = myMiner.reader.Decode(&result)
		if err != nil {
			logf.Println("MINER: Error reading Result:", err)
			break
		}
		chunk.result = &result
		toScheduler <- chunk
	}
	myMiner.conn.Close()
	leave <- ID
	return
}

func (srv *server) scheduler() {
	minerID := 0
	clientID := 0
	lastAssignedClient := -1
	clients := make([]client, 0)
	miners := make(map[int]miner)
	failedMiners := make([]int, 0)

	for {
		select {
		case clientJoin := <-srv.clientJoin:
			logf.Println("CLIENT JOIN:", clientID, "|", clientJoin.job.String())
			clientJoin.ID = clientID
			clientJoin.createChunks(srv.chunkSize)
			go clientJoin.clientHandler(srv.clientLeave)
			clients = append(clients, clientJoin)
			clientID++
		case clientLeave := <-srv.clientLeave:
			logf.Println("CLIENT LEAVE:", clientLeave)
			index := -1
			for clientIndex, client := range clients {
				if client.ID == clientLeave {
					index = clientIndex
					break
				}
			}
			clients = append(clients[:index], clients[index+1:]...)
		case minerJoin := <-srv.minerJoin:
			logf.Println("MINER JOIN:", minerID)
			miners[minerID] = minerJoin
			go minerJoin.minerHandler(srv.minerResult, srv.minerLeave, minerID)
			minerID++
		case minerLeave := <-srv.minerLeave:
			logf.Println("MINER LEAVE:", minerLeave)
			for clientID, client := range clients {
				for chunkID, chunk := range client.chunks {
					if chunk.minerID == minerLeave && chunk.result == nil {
						chunk.minerID = -1
						clients[clientID].chunks[chunkID] = chunk
						failedMiners = append(failedMiners, minerLeave)
						logf.Println("FAILED:", minerLeave)
					}
				}
			}
			delete(miners, minerLeave)
		case minerResult := <-srv.minerResult:
			logf.Println("CHUNK  RESULT:", minerResult.minerID, "|", minerResult.clientID, "|", minerResult.result.String())
			index := -1
			for clientIndex, client := range clients {
				if client.ID == minerResult.clientID {
					index = clientIndex
					break
				}
			}
			clients[index].chunks[minerResult.chunkID] = minerResult
			allChunksDone := true
			for _, chunk := range clients[index].chunks {
				if chunk.result == nil {
					allChunksDone = false
					break
				}
			}
			if allChunksDone {
				minNonce := clients[index].chunks[0].result.Nonce
				minHash := clients[index].chunks[0].result.Hash
				for _, chunk := range clients[index].chunks {
					if chunk.result.Hash < minHash {
						minNonce = chunk.result.Nonce
						minHash = chunk.result.Hash
					}
				}
				result := *bitcoin.NewResult(minHash, minNonce)
				clients[index].toClient <- result
				logf.Println("RESULT TO CLIENT:", minerResult.clientID, "|", result.String())
			}
		default:
			if len(miners) > 0 && len(clients) > 0 {
				lastAssignedClient = (lastAssignedClient + 1) % len(clients)
				var chunkToAssign chunk
				found := false
				for _, chunk := range clients[lastAssignedClient].chunks {
					if chunk.minerID == -1 && chunk.result == nil {
						found = true
						chunkToAssign = chunk
						break
					}
				}
				if found {
					done := false
					for minerID, miner := range miners {
						chunkToAssign.minerID = minerID
						select {
						case miner.toMiner <- chunkToAssign:
							clients[lastAssignedClient].chunks[chunkToAssign.chunkID] = chunkToAssign
							logf.Println("CHUNK REQUEST:", minerID, "|", chunkToAssign.clientID, "|", chunkToAssign.request.String())
							done = true
						default:
						}
						if done {
							break
						}
					}
				}
			}
		}
	}
}
