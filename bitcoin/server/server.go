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
	minerJoin         chan miner
	minerResult       chan chunk
	clientID          int
	minerID           int
	lastAssignedMiner int
	chunkSize         uint64
	clients           map[int]client
	miners            []miner
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
	srv.minerJoin = make(chan miner)
	srv.minerResult = make(chan chunk)
	srv.clientID = 0
	srv.minerID = 0
	srv.lastAssignedMiner = 0
	srv.chunkSize = 50000
	srv.clients = make(map[int]client)
	srv.miners = make([]miner, 0)
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
			srv.minerJoin <- miner{make(chan chunk, 100000), conn, reader}
		} else if message.Type == bitcoin.Request {
			srv.clientJoin <- client{make(chan bitcoin.Message), message, conn, make(map[uint64]chunk)}
		}
	}

}

func (myClient *client) clientHandler() {
	defer myClient.conn.Close()
	message := <-myClient.toClient
	err := json.NewEncoder(myClient.conn).Encode(message)
	if err != nil {
		LOGF.Println("Error sending Result:", err)
		return
	}
}

func minerQueue(fromScheduler, toMiner chan chunk) {
	queue := make([]chunk, 0)
	dequeueChan := make(chan chunk, 1)
	for {
		select {
		case enqueue := <-fromScheduler:
			queue = append(queue, enqueue)
		case message := <-dequeueChan:
			select {
			case toMiner <- message:
			default:
				dequeueChan <- message
			}
		default:
			if len(queue) > 0 {
				select {
				case dequeueChan <- queue[0]:
					queue = queue[1:]
				}
			}
		}
	}
}

func (myMiner *miner) minerHandler(ID int, toScheduler chan chunk) {
	defer myMiner.conn.Close()

	// fromScheduler := make(chan chunk)
	// go minerQueue(myMiner.toMiner, fromScheduler)

	writer := json.NewEncoder(myMiner.conn)
	for {
		chunk := <-myMiner.toMiner
		err := writer.Encode(chunk.request)
		if err != nil {
			LOGF.Println("MINER: Error sending Request:", err)
			return
		}

		var result bitcoin.Message
		err = myMiner.reader.Decode(&result)
		if err != nil {
			LOGF.Println("MINER: Error reading Result:", err)
			return
		}
		chunk.result = &result
		toScheduler <- chunk
	}
}

func (srv *server) schedule(job client, clientID int) {
	if len(srv.miners) > 0 {
		for lower := job.job.Lower; lower < job.job.Upper; lower += srv.chunkSize {
			upper := lower + srv.chunkSize
			if upper > job.job.Upper {
				upper = job.job.Upper
			}
			newChunk := chunk{lower, clientID, srv.lastAssignedMiner, *bitcoin.NewRequest(job.job.Data, lower, upper), nil}
			job.chunks[lower] = newChunk
			srv.miners[srv.lastAssignedMiner].toMiner <- newChunk
			srv.lastAssignedMiner = (1 + srv.lastAssignedMiner) % len(srv.miners)
			LOGF.Println("CHUNK:", srv.lastAssignedMiner, "|", newChunk.request.String())
		}
		srv.clients[clientID] = job
	}
}

func (srv *server) scheduler() {

	for {
		select {
		case clientJoin := <-srv.clientJoin:
			LOGF.Println("JOB:", clientJoin.job.String())
			go clientJoin.clientHandler()
			srv.clients[srv.clientID] = clientJoin
			srv.schedule(clientJoin, srv.clientID)
			srv.clientID++
		case minerJoin := <-srv.minerJoin:
			srv.miners = append(srv.miners, minerJoin)
			go minerJoin.minerHandler(srv.minerID, srv.minerResult)
			srv.minerID++
			for clientID, client := range srv.clients {
				if len(client.chunks) == 0 {
					srv.schedule(client, clientID)
				}
			}
			LOGF.Println("JOIN:", srv.minerID-1)
		case minerResult := <-srv.minerResult:
			LOGF.Println("RECV FROM MINER:", minerResult.result.String())
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
				LOGF.Println("RESULT TO CLIENT")
				srv.clients[minerResult.clientID].toClient <- *bitcoin.NewResult(minHash, minNonce)
				delete(srv.clients, minerResult.clientID)
			}
		}
	}
}
