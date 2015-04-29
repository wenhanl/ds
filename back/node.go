package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"net/http"
	"time"
	"io/ioutil"
	"bytes"
	"strconv"
    "mime/multipart"
    "encoding/json"
    "code.google.com/p/gcfg"
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
var waitlist map[string]bool
var cfg Config
var localname string
var port string
var lb_conn net.Conn
var loadbalancer string
var loadbalancer_replica string
var msgchan chan Message
var addchan chan Client
var rmchan chan Client

func main() {
	resp, _ := http.Get("https://s3.amazonaws.com/ds18842/chat.gcfg")
	err := gcfg.ReadInto(&cfg, resp.Body)
	if err != nil {               
		log.Fatalf("Failed to parse gcfg data: %s", err)
    }
	defer resp.Body.Close()

	localname = os.Args[1]
	loadbalancer = "loadbalancer1"
	loadbalancer_replica = "loadbalancer2"

	port = ":5000"
	clients = make(map[net.Conn]chan<- string)
	connections = make(map[string]Client)
	waitlist = make(map[string]bool)
	//addrs := strings.Split(cfg.Profile[localname].Addr, ":")

	ln, err := net.Listen("tcp", port)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	msgchan = make(chan Message)
	addchan = make(chan Client)
	rmchan = make(chan Client)


	go handleMessages(msgchan, addchan, rmchan)

	//go initiateConnection(msgchan, addchan, rmchan)
	go sendHeartBeat(msgchan, addchan, rmchan)
	//go checkACK()

	http.HandleFunc("/upload/", upload)
	http.HandleFunc("/", download)

	go http.ListenAndServe(":8080", nil)

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
	bufc := bufio.NewReader(c)
	defer c.Close()
	text, _, _ := bufc.ReadLine()
	log.Printf(string(text))

	var sender string
	if (strings.Contains(string(text), "HTTP")){
		//secs := strings.Split(string(text), " ")
		//fileName := secs[1]

	} else {
		secs := strings.Split(string(text), ",")
		sender = secs[0]
	}
	client, ok := connections[sender]
	if (!ok){
		client = Client{
			conn:     c,
			nickname: sender,
			ch:       make(chan string),
		}
		

		if strings.TrimSpace(client.nickname) == "" {
			io.WriteString(c, "Invalid Username\n")
			return
		}
		connections[sender] = client
	
		addchan <- client
		defer func() {
			m := Message{localname, sender, "MSG", fmt.Sprintf("User %s left the chat room.\n", client.nickname)}
			msgchan <- m
			log.Printf("Connection from %v closed.\n", c.RemoteAddr())
			rmchan <- client
		}()

		io.WriteString(c, fmt.Sprintf("Welcome, %s!\n", client.nickname))
		//msgchan <- fmt.Sprintf("New user %s has joined system.\n", client.nickname)
		//msgchan <- fmt.Sprintf("%s: %s", client.nickname, string(text))
		// I/O

		go client.ReadLinesInto(msgchan)
		//client.WriteLinesFrom(client.ch)
	}
}

func handleMessages(msgchan <-chan Message, addchan <-chan Client, rmchan <-chan Client) {
	for {
		select {
		case msg := <-msgchan:
			parseMessage(msg)
			log.Printf("New message: %s", msg.Content)
		case client := <-addchan:

			log.Printf("New client: %v\n", client.conn)
			clients[client.conn] = client.ch
		case client := <-rmchan:
			log.Printf("Client disconnects: %v\n", client.conn)
			delete(clients, client.conn)
		}
	}
}

func initiateConnection(msg Message, msgchan chan<- Message, addchan chan<- Client, rmchan chan<- Client){
	msgdata,_ := json.Marshal(msg)
	dest := msg.Dest

	addr := fmt.Sprintf("%s%s", cfg.Profile[dest].Addr, port)
	log.Printf(addr)

	c, err := net.Dial("tcp", addr)

	if err != nil {
		log.Printf("error!!!")
				// handle error
	}
	client := Client{
		conn:     c,
		nickname: dest,
		ch:       make(chan string),
	}
	msgdata = append(msgdata, 0)
	fmt.Println(msgdata)
	c.Write(msgdata)
			//io.WriteString(c, fmt.Sprintf("%s,%s", localname, text))
	log.Printf("1")
	addchan <-client
	log.Printf("2")
			//clients[client.conn] = client.ch
	log.Printf("initiateConnection" + dest)
	connections[dest] = client

	go client.ReadLinesInto(msgchan)
			//go client.WriteLinesFrom(client.ch)

		//}
	//}	
}

func sendHeartBeat(msgchan chan<- Message, addchan chan<- Client, rmchan chan<- Client){
	hb_lb := Message{localname, "loadbalancer1", "HB", "heartbeat"}
	hb_lb_replica := Message{localname, "loadbalancer2", "HB", "heartbeat"}

	lb, ok := connections["loadbalancer1"]
	if (!ok){
		initiateConnection(hb_lb, msgchan, addchan, rmchan)
		lb = connections["loadbalancer1"]
	}

	if (lb_conn == nil){
		lb_conn = lb.conn
	}
	
	lb_replica, ok := connections["loadbalancer2"]
	if (!ok){
		log.Printf("create connection")
		initiateConnection(hb_lb_replica, msgchan, addchan, rmchan)
		lb_replica = connections["loadbalancer2"]
	}

	
	//count := 0
	for {
		time.Sleep(2000 * time.Millisecond)
		hb_data,_ := json.Marshal(hb_lb)
		hb_data = append(hb_data, 0)
		hb_data_replica,_ := json.Marshal(hb_lb_replica)
		hb_data_replica = append(hb_data_replica, 0)
		//if (count < 15) {
			lb_conn.Write(hb_data)
		//}
		lb_replica.conn.Write(hb_data_replica)
		//count++
		
		//io.WriteString(lb.conn, localname + ",loadbalancer,heartbeat\n")
		//io.WriteString(lb_replica.conn, localname + ",loadbalancer,heartbeat\n")
		
	}
}

func parseMessage(msg Message){
	log.Printf("recieve" + msg.Src + "," + msg.Kind + "," + msg.Content)
	if (msg.Kind == "DOWN" || msg.Kind == "ON"){
		tmp := loadbalancer_replica
		loadbalancer_replica = loadbalancer
		loadbalancer = tmp
		lb_conn = connections[loadbalancer].conn
		if (msg.Kind == "ON"){
			m := Message{localname, loadbalancer, "MSG", ""}
			initiateConnection(m, msgchan, addchan, rmchan)
			lb_conn = connections[loadbalancer].conn
		}
		log.Printf("The lb now is " + loadbalancer)
	} else if (msg.Kind == "CP"){
		content := msg.Content
		parts := strings.Split(content, ",")
		sendCopy(parts[0], parts[1])
	} else if (msg.Kind == "ACK"){
		file := msg.Content
		waitlist[file] = true
		log.Printf("recieve ACK!!!")
	} else if (msg.Kind == "ACKCP"){
		content := msg.Content
		parts := strings.Split(content, ",")
		waitlist[parts[0]] = true
		log.Printf("recieve ACK")
		
		//log.Printf("send copy to " + replica_node)
		go sendCopy(parts[0], parts[1])
	}
}

func sendCopy(filename string, node string){
	log.Printf("send copy of file " + filename + " to" + node)
	url := fmt.Sprintf("http://%s:8080/upload/", node)
	postFile(filename, url + filename)
}

func sendACK(lb string, fileName string, fsize string, request bool){
	lb_c := connections[lb].conn
	if (request){
		log.Printf("sent ACKRQ for file " + fileName)
		msg := Message{localname, lb, "ACKRQ", fileName + "," + fsize}
		data,_ := json.Marshal(msg)
		data = append(data, 0)
		lb_c.Write(data)
		waitlist[fileName] = false

	//io.WriteString(lb_conn, fmt.Sprintf("%s,loadbalancer,ACK:%s,%s\n", localname, fileName, fsize))
	} else {
		log.Printf("sent ACK for file " + fileName)
		msg := Message{localname, lb, "ACK", fileName + "," + fsize}
		data,_ := json.Marshal(msg)
		data = append(data, 0)
		lb_c.Write(data)
		waitlist[fileName] = false
	}
}

func checkACK(fileName string, fsize string, request bool){
	count := 0
	for {
		time.Sleep(1000 * time.Millisecond)
		if (waitlist[fileName] == true){
			 break
		} else {
			sendACK(loadbalancer, fileName, fsize, request)
			count++
		}
		if (count >= 2){
			sendACK(loadbalancer_replica, fileName, fsize, request)
			break
		}
	}
}

// upload logic
func upload(w http.ResponseWriter, r *http.Request) {
	fmt.Println("method:", r.Method)
	filename := r.URL.Path[8:]
	//params := strings.Split(path, "&")
	//replica := strings.Split(params[0], "=")[1]
	//filename := strings.Split(params[1], "=")[1]
	log.Printf(filename)
	r.ParseMultipartForm(32 << 20)
	file, handler, err := r.FormFile("uploadfile")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()
	fmt.Fprintf(w, "%v", handler.Header)
	f, err := os.OpenFile("/var/www/html/files/"+handler.Filename, os.O_WRONLY|os.O_CREATE, 0666)
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
	sendACK(loadbalancer, filename, strconv.FormatInt(fsize, 10), false)
	go checkACK(filename, strconv.FormatInt(fsize, 10), false)
}

func download(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		log.Fatal(err)
	}
	filename := r.FormValue("name")
	f, err := os.Open("/var/www/html/files/"+filename)
	i, err := f.Stat()
	if err != nil {
	}
	fsize := i.Size()
	if (strings.Contains(filename, ".")){
		sendACK(loadbalancer, filename, strconv.FormatInt(fsize, 10), true)
		go checkACK(filename, strconv.FormatInt(fsize, 10), true)
	}
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
    fh, err := os.Open("/var/www/html/files/"+filename)
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