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
	listener    net.Listener
	clientJoin  chan client
	minerJoin   chan miner
	minerResult chan minerResult
}

type miner struct {
	toMiner chan bitcoin.Message
	conn    net.Conn
	reader  *json.Decoder
}

type client struct {
	job        bitcoin.Message
	toClient   chan bitcoin.Message
	assignedTo int
}

type minerResult struct {
	ID     int
	result bitcoin.Message
}

func startServer(port int) (*server, error) {
	srv := new(server)
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	srv.listener = listener
	srv.clientJoin = make(chan client)
	srv.minerJoin = make(chan miner)
	srv.minerResult = make(chan minerResult)
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
		reader := json.NewDecoder(conn)
		var message bitcoin.Message
		err = reader.Decode(&message)
		if err != nil {
			LOGF.Println("Error reading first message:", err)
			return
		}
		if message.Type == bitcoin.Join {
			newMiner := miner{make(chan bitcoin.Message), conn, reader}
			srv.minerJoin <- newMiner
		} else if message.Type == bitcoin.Request {
			toClient := make(chan bitcoin.Message)
			srv.clientJoin <- client{message, toClient, -1}
			go clientHandler(conn, toClient)
		}
	}

}

func clientHandler(conn net.Conn, toClient <-chan bitcoin.Message) {
	message := <-toClient
	err := json.NewEncoder(conn).Encode(message)
	if err != nil {
		LOGF.Println("Error sending Result:", err)
	}
	LOGF.Println("RESULT TO CLIENT:", message.String())
	conn.Close()
}

func (myMiner *miner) minerReader(ID int, toScheduler chan minerResult) {
	defer myMiner.conn.Close()

	var result bitcoin.Message
	for {
		err := myMiner.reader.Decode(&result)
		if err != nil {
			LOGF.Println("MINER: Error reading Result:", err)
			return
		}
		toScheduler <- minerResult{ID, result}
		LOGF.Println("RECV FROM MINER:", result.String())
	}
}

func minerQueue(fromScheduler, toMiner chan bitcoin.Message) {
	queue := make([]bitcoin.Message, 0)
	dequeueChan := make(chan bitcoin.Message, 1)
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

func (myMiner *miner) minerWriter() {
	toMiner := make(chan bitcoin.Message)
	writer := json.NewEncoder(myMiner.conn)
	go minerQueue(myMiner.toMiner, toMiner)
	for {
		message := <-toMiner
		err := writer.Encode(message)
		if err != nil {
			LOGF.Println("MINER: Error sending Request:", err)
			return
		}
		LOGF.Println("SENT TO MINER:", message.String())
	}
}

func (srv *server) scheduler() {
	clientID := 0
	clients := make(map[int]client)
	minerID := 0
	lastAssignedMiner := 0
	var miners []miner
	for {
		select {
		case clientJoin := <-srv.clientJoin:
			LOGF.Println("JOB:", clientJoin.job.String())
			if len(miners) > 0 {
				clientJoin.assignedTo = lastAssignedMiner
				miners[lastAssignedMiner].toMiner <- clientJoin.job
				lastAssignedMiner = (1 + lastAssignedMiner) % len(miners)
			}
			clients[clientID] = clientJoin
			clientID++
		case minerJoin := <-srv.minerJoin:
			miners = append(miners, minerJoin)
			go minerJoin.minerReader(minerID, srv.minerResult)
			go minerJoin.minerWriter()
			minerID++
			for clientID, client := range clients {
				if client.assignedTo == -1 {
					client.assignedTo = lastAssignedMiner
					miners[lastAssignedMiner].toMiner <- client.job
					lastAssignedMiner = (1 + lastAssignedMiner) % len(miners)
					clients[clientID] = client
				}
			}
		case minerResult := <-srv.minerResult:
			for clientID, client := range clients {
				if client.assignedTo == minerResult.ID {
					client.toClient <- minerResult.result
					delete(clients, clientID)
					break
				}
			}

		}
	}
}
