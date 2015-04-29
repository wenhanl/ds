package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"net/http"
	"time"
	"strconv"
	"io/ioutil"
	"bytes"
	"mime/multipart"
	"code.google.com/p/gcfg"
    _ "github.com/mattn/go-sqlite3"
    "sqlite"
    "encoding/json"
)

type Config struct{
	Profile map[string]*struct{
		Addr string
	}
}

type Client struct {
	conn     net.Conn
	nickname string
	ch       chan string
}

type Message struct {
	Src string
	Dest string
	Kind string
	Content string
}


var connections map[string]Client
var clients map[net.Conn]chan<- string
var ACKs map[string][2]string
var waitlist map[string]bool
var heartbeats map[string]int
var cfg Config
var localname string
var port string
var lb_replica string
var status bool

func main() {
	resp, _ := http.Get("https://s3.amazonaws.com/ds18842/chat.gcfg")
	err := gcfg.ReadInto(&cfg, resp.Body)
	if err != nil {               
		log.Fatalf("Failed to parse gcfg data: %s", err)
    }
	defer resp.Body.Close()

	localname = "loadbalancer2"
	lb_replica = "loadbalancer1"
	status = false
	port = ":5000"
	clients = make(map[net.Conn]chan<- string)
	connections = make(map[string]Client)
	ACKs = make(map[string][2]string)
	heartbeats = make(map[string]int)

	/*ACKs["Eryue_Chen.JPG"] = []string{}
	ACKs["image.jpeg"] = "node2&node3"
	writeFile("Eryue_Chen.JPG\tnode1&node2")
	writeFile("image.jpeg\tnode2&node3")*/

	ln, err := net.Listen("tcp", port)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	msgchan := make(chan Message)
	addchan := make(chan Client)
	rmchan := make(chan Client)

	go handleMessages(msgchan, addchan, rmchan)

	go check_heartbeat(msgchan, addchan, rmchan)


	http.HandleFunc("/upload/", upload)
	go http.ListenAndServe(":8080", nil)
	//http.HandleFunc("/download/", download_handler)
	//http.HandleFunc("/upload/", upload_handler)

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		log.Printf("accept")

		go handleConnection(conn, msgchan, addchan, rmchan)
	}
}

/*func download_handler(w http.ResponseWriter, r *http.Request) {
	sqlite.DecideDownloadNodes(r.URL.Path[10:])
	//location, ok := ACKs[r.URL.Path[10:]]
	if (ok){
		fmt.Fprintf(w, "The location of node is: %s", location)
	} else{
		fmt.Fprintf(w, "File not found")
	} 
}*/

/*func upload_handler(w http.ResponseWriter, r *http.Request) {
	location := "172.31.4.34"
	fmt.Fprintf(w, "The location of node is: %s", location)
}*/

func (c Client) ReadLinesInto(ch chan<- Message) {
	for {
		buf := make([]byte, 1024)
		bytesRead,_ := c.conn.Read(buf)
		if (bytesRead > 0){
			start := 0
			for i := range buf {
				if (buf[i] == 0){
					msgbuf = buf[start, i]
					length := i - start
					if (length > 0){
						var m Message
						json.Unmarshal(msgbuf[:length], &m)
						ch <- m
					}
				}
				start = i + 1
			}
			
		}
	}
}

func (c Client) WriteLinesFrom(ch <-chan string) {
	for msg := range ch {
		_, err := io.WriteString(c.conn, msg)
		if err != nil {
			return
		}
	}
}

func handleConnection(c net.Conn, msgchan chan<- Message, addchan chan<- Client, rmchan chan<- Client) {
	buf := make([]byte, 1024)
	bytesRead,_ := c.Read(buf)
	//log.Printf(strconv.Itoa(bytesRead))
	var m Message
	json.Unmarshal(buf[:bytesRead], &m)
	//fmt.Println(buf)
	sender := m.Src
	if (sender == lb_replica){
		if (status){
			log.Printf("Multicast to all lb is on")
			for i := range heartbeats{
				log.Printf(i)
				client_conn := connections[i].conn
				msg := Message{localname, i, "ON", lb_replica}
				data,_ := json.Marshal(msg)
				client_conn.Write(data)
			}
			Replicate()
			status = false
		}
	}
	log.Printf(sender)
	//client, ok := connections[sender]
	//if (!ok){
		client := Client{
			conn:     c,
			nickname: sender,
			ch:       make(chan string),
		}
		if strings.TrimSpace(client.nickname) == "" {
			io.WriteString(c, "Invalid Username\n")
			return
		}
		connections[sender] = client
		parseMessage(m)
		addchan <- client
		defer func() {
			m := Message{localname, sender, "MSG", fmt.Sprintf("User %s left the chat room.\n", client.nickname)}
			msgchan <- m
			log.Printf("Connection from %v closed.\n", c.RemoteAddr())
			rmchan <- client
		}()
		//m := Message{localname, sender, "MSG", fmt.Sprintf("Welcome, %s!\n", client.nickname)}
		//log.Printf(m.Content)
		//data,_ := json.Marshal(m)
		//c.Write(data)
		//msgchan <- fmt.Sprintf("New user %s has joined system.\n", client.nickname)
		//msgchan <- fmt.Sprintf("%s", string(text))
		// I/O
		go client.ReadLinesInto(msgchan)
		client.WriteLinesFrom(client.ch)
	//}
}

func handleMessages(msgchan <-chan Message, addchan <-chan Client, rmchan <-chan Client) {
	for {
		select {
		case msg := <-msgchan:
			parseMessage(msg)
		case client := <-addchan:
			log.Printf("New client: %v\n", client.conn)
			clients[client.conn] = client.ch
		case client := <-rmchan:
			log.Printf("Client disconnects: %v\n", client.conn)
			delete(clients, client.conn)
		}
	}
}

func initiateConnection(m Message, msgchan chan<- Message, addchan chan<- Client, rmchan chan<- Client){
		//m := Message{localname, lb_replica, "MSG", ""}
		msgdata,_ := json.Marshal(m)

		addr := fmt.Sprintf("%s%s",cfg.Profile[lb_replica].Addr, port)

		c, err := net.Dial("tcp", addr)

		if err != nil {
			log.Printf("error!!!")
			// handle error]
		}
		client := Client{
			conn:     c,
			nickname: lb_replica,
			ch:       make(chan string),
		}

		c.Write(msgdata)
		//io.WriteString(lb_replica, fmt.Sprintf("%s,%s", localname, text))
			
		addchan <-client
		clients[client.conn] = client.ch
		connections[lb_replica] = client

		go client.ReadLinesInto(msgchan)
		go client.WriteLinesFrom(client.ch)

}

func check_heartbeat(msgchan chan<- Message, addchan chan<- Client, rmchan chan<- Client){
	for {
		//check heart beat every 10 seconds
		time.Sleep(5000 * time.Millisecond)
		log.Printf(strconv.Itoa(len(heartbeats)))
		for k, _ := range heartbeats {
			if (heartbeats[k] > 0){
				heartbeats[k] = 0
			} else {
				//client,_:= connections[k]
				log.Printf("check " + k)
				if (status){
					delete(heartbeats, k)
					MoveData(k)
				} else {
					healthcheck(k, msgchan, addchan, rmchan)
				}
			}
		}
	}
}

func parseMessage(msg Message){
	kind := msg.Kind
	log.Printf("recieve" + msg.Src + "," + msg.Kind + "," + msg.Content)
	if (kind == "HB"){
		src := msg.Src
		node, ok := heartbeats[src]
		if (ok){
			if (node < 0){
				//send NACK
				log.Printf("receieve heartbeat again, keep the node")
				m := Message{localname, lb_replica, "KEEP", src}
				data,_ := json.Marshal(m)
				connections[lb_replica].conn.Write(data)
				//io.WriteString(lb_replica, fmt.Sprintf("%s,KEEP:%s", localname, src))
			}
			heartbeats[src] = node + 1
		} else {
			heartbeats[src] = 1
		}
	} else if (kind == "ACK" || kind == "ACKRQ"){
		files := strings.Split(msg.Content, ",")
		size,_ := strconv.Atoi(files[1])
		if (!status){
			log.Printf("forward to loadbalancer1")
			src := msg.Src
			m := Message{localname, lb_replica, msg.Kind, msg.Content + "," + src}
			data,_ := json.Marshal(m)
			connections[lb_replica].conn.Write(data)
			go checkACK(files[0], m, msg)
		} else {
			log.Printf("Modify the database")
			
			sqlite.UpdateNodes(files[0], cfg.Profile[msg.Src].Addr, size)
			sqlite.UpdateFiles(files[0], cfg.Profile[msg.Src].Addr, size)
			if (kind == "ACKRQ"){
				replica_node := sqlite.DecideUploadReplica(cfg.Profile[msg.Src].Addr)
				log.Printf("send copy to " + replica_node)
				m := Message{localname, msg.Src, "ACKCP", files[0] + "," + replica_node}
				data,_ := json.Marshal(m)
				data = append(data, 0)
				connections[msg.Src].conn.Write(data)
			} else {
				reply := Message{localname, msg.Src, "ACK", files[0]}
				reply_data,_ := json.Marshal(reply)
				reply_data = append(reply_data, 0)
				connections[msg.Src].conn.Write(reply_data)
			}
		}
	}  else if (kind == "REQ"){
		//reqs := strings.Split(msg, ",")
		//req_node := strings.Split(reqs[2], ":")
		node := msg.Content
		log.Printf("receive request to remove")
		if (heartbeats[node] < 0){
			log.Printf("send remove response")
			m := Message{localname, lb_replica, "RM", node}
			data,_ := json.Marshal(m)
			data = append(data, 0)
			connections[lb_replica].conn.Write(data)		
			delete(heartbeats, node)
			//io.WriteString(lb_replica, fmt.Sprintf("%s,RM:%s", localname, node))
		} else {
			//Not clear now, set to -1
			log.Printf("set to -1")
			heartbeats[node] = -1
		}
	} else {
		content := msg.Content
		log.Printf("New Message: %s", content)
	}
}


func MoveData(client string){
	delete(heartbeats, client)
	//log.Printf(strconv.Itoa(len(heartbeats)))
	log.Printf("movedata")
	files := sqlite.QueryNodes(cfg.Profile[client].Addr)
	file := strings.Split(files, "#")
	//fmt.Println(len(file))
	sqlite.DeleteNode(cfg.Profile[client].Addr)
	for i := range file {
		f := file[i]
		nodes := sqlite.QueryFiles(f)
		if (strings.Contains(nodes, "#")){
			node := strings.Split(nodes, "#")
			log.Printf(nodes)
			for j := range node {
				n := node[j]
				if (n != cfg.Profile[client].Addr){
					log.Printf(f)
					log.Printf(n)
					sqlite.UpdateFiles(f, n, -1)
					replica_node := sqlite.DecideUploadReplica(n)
					m := Message{localname, n, "CP", f + "," + replica_node}
					data,_ := json.Marshal(m)
					data = append(data, 0)
					connections[n].conn.Write(data)
				}
			}
		}
	}
}

func Replicate(){
	url := fmt.Sprintf("http://%s:8080/upload/development.sqlite3", cfg.Profile[lb_replica].Addr)
	postFile("development.sqlite3", url)
}


func healthcheck(client string, msgchan chan<- Message, addchan chan<- Client, rmchan chan<- Client){
	var m Message
	m = Message{localname, lb_replica, "RM", client}
	if (heartbeats[client] < 0){
		cl, ok := connections[lb_replica]
		if (!ok) {
			log.Printf(m.Kind)
			initiateConnection(m, msgchan, addchan, rmchan)
			cl = connections[lb_replica]	
		}else {
			data,_ := json.Marshal(m)
			data = append(data, 0)
			cl.conn.Write(data)
			delete(heartbeats, client)
		}
	}
}

func checkACK(fileName string, m Message, msg Message){
	count := 0
	for {
		time.Sleep(1000 * time.Millisecond)
		if (waitlist[fileName] == true){
			 break
		} else {
			data,_ := json.Marshal(m)
			data = append(data, 0)
			connections[lb_replica].conn.Write(data)
			count++
		}
		if (count >= 2){
			contents := strings.Split(msg.Content, ",")
			size,_ := strconv.Atoi(contents[1])
			sqlite.UpdateNodes(fileName, cfg.Profile[msg.Src].Addr, size)
			sqlite.UpdateFiles(fileName, cfg.Profile[msg.Src].Addr, size)
			if (msg.Kind == "ACKRQ"){
				replica_node := sqlite.DecideUploadReplica(cfg.Profile[msg.Src].Addr)
				log.Printf("send copy to " + replica_node)
				m := Message{localname, msg.Src, "ACKCP", contents[0] + "," + replica_node}
				data,_ := json.Marshal(m)
				data = append(data, 0)
				connections[msg.Src].conn.Write(data)
			} else {
				reply := Message{localname, msg.Src, "ACK", contents[0]}
				reply_data,_ := json.Marshal(reply)
				reply_data = append(reply_data, 0)
				connections[msg.Src].conn.Write(reply_data)
			}
			time.Sleep(500 * time.Millisecond)
			if (status == false){
				log.Printf("loadbalancer1 down")
				for i := range heartbeats{
					log.Printf(i)
					client_conn := connections[i].conn
					m := Message{localname, i, "DOWN", lb_replica}
					data,_ := json.Marshal(m)
					data = append(data, 0)
					client_conn.Write(data)
				}
				status = true
				break
			}
		}
	}
}

func upload(w http.ResponseWriter, r *http.Request) {
	fmt.Println("method:", r.Method)
	filename := r.URL.Path[8:]
	log.Printf(filename)
	r.ParseMultipartForm(32 << 20)
	file, handler, err := r.FormFile("uploadfile")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()
	fmt.Fprintf(w, "%v", handler.Header)
	f, err := os.OpenFile("/home/ubuntu/ds/front/db/"+handler.Filename, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer f.Close()
	io.Copy(f, file)
	i, err := f.Stat()
	if err != nil {
	}
	fsize := i.Size()
	fmt.Println(fsize)
	//sendACK(loadbalancer, filename, strconv.FormatInt(fsize, 10))
	//go checkACK(filename, strconv.FormatInt(fsize, 10))
}


func postFile(filename string, targetUrl string) error {
    bodyBuf := &bytes.Buffer{}
    bodyWriter := multipart.NewWriter(bodyBuf)

    // this step is very important
    fileWriter, err := bodyWriter.CreateFormFile("uploadfile", filename)
    if err != nil {
        fmt.Println("error writing to buffer")
        return err
    }

    // open file handle
    fh, err := os.Open("/home/ubuntu/ds/front/db/" + filename)
    if err != nil {
        fmt.Println("error opening file")
        return err
    }

    //iocopy
    _, err = io.Copy(fileWriter, fh)
    if err != nil {
        return err
    }

    contentType := bodyWriter.FormDataContentType()
    bodyWriter.Close()

    resp, err := http.Post(targetUrl, contentType, bodyBuf)
    if err != nil {
        return err
    }
    defer resp.Body.Close()
    resp_body, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        return err
    }
    fmt.Println(resp.Status)
    fmt.Println(string(resp_body))
    return nil
}

/*func writeFile(line string){
	f, err := os.OpenFile("./metadata", os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		f, err = os.Create("./metadata")
		if err != nil {
			log.Fatal(err)
		}
	}
    _, err = f.WriteString(line)
    if err != nil {
		return
	}
    f.Sync()
}*/

/*func check_replica(node string){
	f, err := os.Open("./metadata")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fmt.Println(scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}*/

