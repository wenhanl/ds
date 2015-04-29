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
//var ACKs map[string][2]string
var heartbeats map[string]int
var cfg Config
var localname string
var port string
var lb_replica string

func main() {
	resp, _ := http.Get("https://s3.amazonaws.com/ds18842/chat.gcfg")
	err := gcfg.ReadInto(&cfg, resp.Body)
	if err != nil {               
		log.Fatalf("Failed to parse gcfg data: %s", err)
    }
	defer resp.Body.Close()

	localname = "loadbalancer1"
	lb_replica = "loadbalancer2"


	port = ":5000"
	clients = make(map[net.Conn]chan<- string)
	connections = make(map[string]Client)
	//ACKs = make(map[string][2]string)
	heartbeats = make(map[string]int)

	http.HandleFunc("/upload/", upload)
	go http.ListenAndServe(":8080", nil)

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

	m := Message{localname, lb_replica, "ON", ""}

	initiateConnection(m, msgchan, addchan, rmchan)
	//http.HandleFunc("/download/", download_handler)
	//http.HandleFunc("/upload/", upload_handler)
	//sqlite.Initialize()
	//Replicate()

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
					length := i - start
					if (length > 0){
						msgbuf := buf[start:i]
						var m Message
						json.Unmarshal(msgbuf[:length], &m)
						ch <- m
					}
					start = i + 1
				}
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
	json.Unmarshal(buf[:(bytesRead-1)], &m)
	//fmt.Println(buf)
	sender := m.Src
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
		sqlite.RegisterNodes(cfg.Profile[sender].Addr)
		if (m.Kind == "HB"){
			Replicate()
		}
		addchan <- client
		defer func() {
			m := Message{localname, sender, "MSG", fmt.Sprintf("User %s left the chat room.\n", client.nickname)}
			msgchan <- m
			log.Printf("Connection from %v closed.\n", c.RemoteAddr())
			rmchan <- client
		}()

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
		msgdata = append(msgdata, 0)
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
				healthcheck(k, msgchan, addchan, rmchan)
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
				data = append(data, 0)
				connections[lb_replica].conn.Write(data)
			}
			heartbeats[src] = node + 1
		} else {
			heartbeats[src] = 1
		}
	} else if (kind == "ACK"){
		src := msg.Src
		content := msg.Content
		files := strings.Split(content, ",")
		//node, ok := ACKs[files[0]]
		size,_ := strconv.Atoi(files[1])

		if (src == lb_replica) {
			src = files[2]
		}
		reply := Message{localname, src, "ACK", files[0]}
		reply_data,_ := json.Marshal(reply)
		reply_data = append(reply_data, 0)
		connections[src].conn.Write(reply_data)

		sqlite.UpdateNodes(files[0], cfg.Profile[src].Addr, size)
		sqlite.UpdateFiles(files[0], cfg.Profile[src].Addr, size)
		Replicate()
	} else if (kind == "RM"){
		log.Printf("recieve confirm to remove the node")
		node := msg.Content
		MoveData(node)
	} else if (kind == "KEEP"){
		log.Printf("receive confirm to keep the node")
		node := msg.Content
		heartbeats[node] = 1
	} else if (kind == "ACKRQ"){
		log.Printf("receive ACKRQ")
		src := msg.Src
		content := msg.Content
		files := strings.Split(content, ",")
		//node, ok := ACKs[files[0]]
		size,_ := strconv.Atoi(files[1])
		//reply := Message{localname, src, "ACK", files[0]}
		//reply_data,_ := json.Marshal(reply)
		//connections[src].conn.Write(reply_data)
		if (src == lb_replica) {
			src = files[2]
		}
		sqlite.UpdateNodes(files[0], cfg.Profile[src].Addr, size)
		sqlite.UpdateFiles(files[0], cfg.Profile[src].Addr, size)
		
		replica_node := sqlite.DecideUploadReplica(cfg.Profile[src].Addr)
		log.Printf("send copy to " + replica_node)
		m := Message{localname, src, "ACKCP", files[0] + "," + replica_node}
		data,_ := json.Marshal(m)
		data = append(data, 0)
		connections[src].conn.Write(data)

		Replicate()
	} else {
		fmt.Println(msg)
		content := msg.Content
		log.Printf("New Message: %s", content)
		//log.Printf("New message: %s", msg)
	}
}


func MoveData(client string){
	delete(heartbeats, client)
	//log.Printf(strconv.Itoa(len(heartbeats)))
	log.Printf("movedata")
	files := sqlite.QueryNodes(cfg.Profile[client].Addr)
	file := strings.Split(files, "#")
	log.Printf("files" + files)
	sqlite.DeleteNode(cfg.Profile[client].Addr)
	log.Printf(cfg.Profile[client].Addr)
	for i := range file {
		f := file[i]
		nodes := sqlite.QueryFiles(f)
		if (strings.Contains(nodes, "#")){
			node := strings.Split(nodes, "#")
			log.Printf(nodes)
			for j := range node {
				n := node[j]
				log.Printf(cfg.Profile[client].Addr)
				if (n != cfg.Profile[client].Addr){
					log.Printf(f)
					log.Printf(n)
					sqlite.UpdateFiles(f, n, -1)
					replica_addr := sqlite.DecideUploadReplica(n)
					var copy_node string
					for k := range cfg.Profile {
						if (cfg.Profile[k].Addr == n){
							log.Printf(k)
							copy_node = k
						}
					}
					m := Message{localname, copy_node, "CP", f + "," + replica_addr}
					data,_ := json.Marshal(m)
					data = append(data, 0)
					fmt.Println(len(cfg.Profile))
					connections[copy_node].conn.Write(data)
				}
			}
		}
	}
	Replicate()
}

func Replicate(){
	url := fmt.Sprintf("http://%s:8080/upload/development.sqlite3", cfg.Profile[lb_replica].Addr)
	postFile("development.sqlite3", url)
}

func healthcheck(client string, msgchan chan<- Message, addchan chan<- Client, rmchan chan<- Client){
	var m Message
	m = Message{localname, lb_replica, "REQ", client}
	cl, _ := connections[lb_replica]
	/*if (!ok) {
		log.Printf(m.Kind)
		initiateConnection(m, msgchan, addchan, rmchan)
		cl = connections[lb_replica]
	} else {*/
	data,_ := json.Marshal(m)
	data = append(data, 0)
	cl.conn.Write(data)	
	
	log.Printf("request to remove")
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

