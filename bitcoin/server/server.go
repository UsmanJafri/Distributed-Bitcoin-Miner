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
	listener          net.Listener
	clientJoin        chan client
	clientLeave       chan int
	minerJoin         chan miner
	minerResult       chan chunk
	minerLeave        chan int
	clientID          int
	minerID           int
	lastAssignedMiner int
	clients           map[int]client
	miners            map[int]miner
	minerQueueSize    int
	chunkSize         uint64
}

type miner struct {
	toMiner chan chunk
	conn    net.Conn
	reader  *json.Decoder
}

type client struct {
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
	srv.minerResult = make(chan chunk)
	srv.minerLeave = make(chan int)
	srv.clientID = 0
	srv.minerID = 0
	srv.lastAssignedMiner = 0
	srv.clients = make(map[int]client)
	srv.miners = make(map[int]miner)
	srv.minerQueueSize = 10
	srv.chunkSize = 50000
	return srv, err
}

var LOGF *log.Logger

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
	var message bitcoin.Message
	for {
		conn, err := srv.listener.Accept()
		if err != nil {
			LOGF.Println("Error accepting connection:", err)
			return
		}
		reader := json.NewDecoder(conn)
		err = reader.Decode(&message)
		if err != nil {
			LOGF.Println("Error reading first message:", err)
			continue
		}
		if message.Type == bitcoin.Join {
			srv.minerJoin <- miner{make(chan chunk, srv.minerQueueSize), conn, reader}
		} else if message.Type == bitcoin.Request {
			srv.clientJoin <- client{make(chan bitcoin.Message), message, conn, make(map[uint64]chunk)}
		}
	}

}

func (myClient *client) clientHandler(ID int, leave chan<- int) {
	message := <-myClient.toClient
	err := json.NewEncoder(myClient.conn).Encode(message)
	if err != nil {
		LOGF.Println("Error sending Result:", err)
	}
	leave <- ID
	myClient.conn.Close()
}

func (myClient *client) createChunks(chunkSize uint64, clientID int) {
	for lower := myClient.job.Lower; lower < myClient.job.Upper; lower += chunkSize {
		upper := lower + chunkSize
		if upper > myClient.job.Upper {
			upper = myClient.job.Upper
		}
		newChunk := chunk{lower, clientID, -1, *bitcoin.NewRequest(myClient.job.Data, lower, upper), nil}
		myClient.chunks[lower] = newChunk
	}
}

func (myMiner *miner) minerHandler(toScheduler chan<- chunk, leave chan<- int, ID int) {
	writer := json.NewEncoder(myMiner.conn)
	for {
		chunk := <-myMiner.toMiner
		err := writer.Encode(chunk.request)
		if err != nil {
			LOGF.Println("MINER: Error sending Request:", err)
			break
		}

		var result bitcoin.Message
		err = myMiner.reader.Decode(&result)
		if err != nil {
			LOGF.Println("MINER: Error reading Result:", err)
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
	lastAssignedClient := -1
	failedMiners := make([]int, 0)

	for {
		select {
		case clientJoin := <-srv.clientJoin:
			LOGF.Println("CLIENT JOIN:", srv.clientID, "|", clientJoin.job.String())
			clientJoin.createChunks(srv.chunkSize, srv.clientID)
			go clientJoin.clientHandler(srv.clientID, srv.clientLeave)
			srv.clients[srv.clientID] = clientJoin
			srv.clientID++
		case clientLeave := <-srv.clientLeave:
			LOGF.Println("CLIENT LEAVE:", clientLeave)
			// delete(srv.clients, clientLeave)
		case minerJoin := <-srv.minerJoin:
			LOGF.Println("MINER JOIN:", srv.minerID)
			srv.miners[srv.minerID] = minerJoin
			go minerJoin.minerHandler(srv.minerResult, srv.minerLeave, srv.minerID)
			srv.minerID++
		case minerLeave := <-srv.minerLeave:
			LOGF.Println("MINER LEAVE:", minerLeave)
			for clientID, client := range srv.clients {
				for chunkID, chunk := range client.chunks {
					if chunk.minerID == minerLeave && chunk.result == nil {
						chunk.minerID = -1
						srv.clients[clientID].chunks[chunkID] = chunk
						failedMiners = append(failedMiners, minerLeave)
						LOGF.Println("FAILED:", minerLeave)
					}
				}
			}
			delete(srv.miners, minerLeave)
		case minerResult := <-srv.minerResult:
			LOGF.Println("CHUNK RESULT:", minerResult.result.String())
			srv.clients[minerResult.clientID].chunks[minerResult.chunkID] = minerResult
			allChunksDone := true
			for _, chunk := range srv.clients[minerResult.clientID].chunks {
				if chunk.result == nil {
					allChunksDone = false
					break
				}
			}
			if allChunksDone {
				minNonce := srv.clients[minerResult.clientID].chunks[0].result.Nonce
				minHash := srv.clients[minerResult.clientID].chunks[0].result.Hash
				for _, chunk := range srv.clients[minerResult.clientID].chunks {
					if chunk.result.Hash < minHash {
						minNonce = chunk.result.Nonce
						minHash = chunk.result.Hash
					}
				}
				result := *bitcoin.NewResult(minHash, minNonce)
				srv.clients[minerResult.clientID].toClient <- result
				LOGF.Println("RESULT TO CLIENT:", minerResult.clientID, "|", result.String())
			}
		default:
			if len(srv.miners) > 0 {
				lastAssignedClient = (lastAssignedClient + 1) % len(srv.clients)
				var chunkToAssign chunk
				found := false
				for _, chunk := range srv.clients[lastAssignedClient].chunks {
					if chunk.minerID == -1 && chunk.result == nil {
						found = true
						chunkToAssign = chunk
						break
					}
				}
				if found {
					done := false
					for minerID, miner := range srv.miners {
						select {
						case miner.toMiner <- chunkToAssign:
							chunkToAssign.minerID = minerID
							srv.clients[lastAssignedClient].chunks[chunkToAssign.chunkID] = chunkToAssign
							LOGF.Println("CHUNK REQUEST:", minerID, "|", chunkToAssign.request.String())
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
