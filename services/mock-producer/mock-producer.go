// =======================================================================================
// mock-producer: This program pretends to read a data source and send the data to a queue.
// @author Flávio Gomes da Silva Lisboa <flavio.lisboa@fgsl.eti.br>
// @license LGPL-2.1
// =======================================================================================
package main

import (
	"fmt"
	"os"
	"strconv"
	"time"
	"unicode/utf8"

	stomp "github.com/go-stomp/stomp"
)

func main() {
	var counter int

	fmt.Println("MOCK-PRODUCER: initializing data producing...")
	fmt.Println("MOCK-PRODUCER: version 1.0.0")

	time.Sleep(time.Second * 10)
	// continue even some error occurs
	counter = 0

	for {
		listEvents(counter)
		time.Sleep(time.Second)
		counter++
	}
}

func listEvents(eventid int) {
	var data string
	var conn *stomp.Conn
	var err error

	data = getDataForLog(eventid)

	messageSize := utf8.RuneCountInString(data)

	fmt.Printf("Message size: %d bytes\n", messageSize)

	if messageSize > 100*1024*1024 { // 100 MB
		fmt.Println("ERROR: Message size exceeds 100 MB")
		return
	}

	activemqHost := "mock-queue"
	if os.Getenv("QUEUE_HOST") != "" {
		activemqHost = os.Getenv("QUEUE_HOST")
	}

	activemqPort := "61616"
	if os.Getenv("QUEUE_PORT") != "" {
		activemqPort = os.Getenv("QUEUE_PORT")
	}

	activemqServer := activemqHost + ":" + activemqPort

	conn, err = stomp.Dial("tcp", activemqServer, stomp.ConnOpt.Login(os.Getenv("QUEUE_USERNAME"), os.Getenv("QUEUE_PASSWORD")))
	if err != nil {
		fmt.Println("ERROR: stomp.Dial: " + err.Error())
		return
	}

	defer conn.Disconnect()

	fmt.Println("MOCK-PRODUCER: Has connectivity with " + activemqServer)

	queueName := "pods"

	if os.Getenv("QUEUE_NAME") != "" {
		queueName = os.Getenv("QUEUE_NAME")
	}
	err = conn.Send(
		"/podips/"+queueName, // destination
		"application/json",   // content-type
		[]byte(data))         // body
	if err != nil {
		fmt.Println("ERROR WHEN SENDING TO QUEUE " + err.Error())
		return
	}

	fmt.Println("Message sent successfully:", data)
}

func getDataForLog(eventid int) string {
	var data string

	now := time.Now()
	data = "{" +
		"\"class\": \"audit\"," +
		"\"subclass\": \"pod_ip\"," +
		"\"origin\":\"mock\"," +
		"\"dc\":\"mock\"," +
		"\"host\":\"mock\"," +
		"\"pod_namespace\":\"mock\"," +
		"\"pod\":\"mock\"," +
		"\"pod_ip\":\"mock\"," +
		"\"pod_ip_status\":\"mock\"," +
		"\"short_message\":\"mock/mock:" + strconv.Itoa(eventid) + "\"," +
		"\"full_message\":\"mock/mock:mock:mock:" + now.String() + "\"," +
		"\"timestamp\":\"" + now.String() + "\"," +
		"\"logtype\": \"mock\"}"

	return data
}
