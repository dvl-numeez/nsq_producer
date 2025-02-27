package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"

	"github.com/nsqio/go-nsq"
)

type Company struct {
	Name    string `json:"name"`
	City    string `json:"city"`
	State   string `json:"state"`
	Country string `json:"country"`
}

type ErrResponse struct {
	Error string `json:"error"`
}

type Response struct {
	Message string `json:"message"`
}

func main() {

	router := http.NewServeMux()
	router.HandleFunc("/add", handlerProduce)
	fmt.Println("Server is running..........")
	if err := http.ListenAndServe(":4040", router); err != nil {
		log.Fatal("Server crashed due to error : ", err)
	}

}

func handlerProduce(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		writeErrorResponse(w, http.StatusBadRequest, errors.New("Request method should be POST only"))
		return
	}
	if err := pushDataToNSQ(); err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, err)
		return
	}

	writeResponse(w, "Produced data inserted successfully")

}

func fetchCompany() Company {
	companies := []string{"Apple", "Google", "Tesla", "Digivatelabs"}
	cities := []string{"Austin", "Munich", "Kyoto", "Cape Town"}
	states := []string{"Texas", "Bavaria", "Kyoto Prefecture", "Western Cape"}
	countries := []string{"United States", "Germany", "Japan", "South Africa"}
	randomNumber := rand.Intn(4)
	return Company{
		Name:    companies[randomNumber],
		City:    cities[randomNumber],
		State:   states[randomNumber],
		Country: countries[randomNumber],
	}
}

func pushDataToNSQ() error {
	config := nsq.NewConfig()

	producer, err := nsq.NewProducer("127.0.0.1:4150", config)
	defer producer.Stop()
	if err != nil {
		return err
	}
	company := fetchCompany()
	companyData, err := json.Marshal(company)
	if err != nil {
		return err
	}
	err = producer.Publish("NSQ_COMPANY_DATA", companyData)
	if err != nil {
		return err
	}

	return nil
}

func writeErrorResponse(w http.ResponseWriter, statusCode int, errorMessage error) {
	w.Header().Set("content-type","application/json")
	w.WriteHeader(statusCode)
	errorResp := ErrResponse{
		Error: errorMessage.Error(),
	}
	errData, _ := json.Marshal(errorResp)
	_, err := w.Write(errData)
	if err != nil {
		log.Println(err)
	}
}

func writeResponse(w http.ResponseWriter, message string) {
	w.Header().Set("content-type","application/json")
	w.WriteHeader(http.StatusOK)
	resp := Response{
		Message: message,
	}
	respData, _ := json.Marshal(resp)
	_, err := w.Write(respData)
	if err != nil {
		log.Println(err)
	}
}


// func spawnConsumer(ctx context.Context, conxn *websocket.Conn, typ websocket.MessageType) error {
// 	config := nsq.NewConfig()
// 	consumer, err := nsq.NewConsumer("NSQ_COMPANY_DATA", "websocket_channel", config)
// 	if err != nil {
// 		return err
// 	}
// 	consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
// 		var topicData md.Company
// 		if err := json.Unmarshal(message.Body, &topicData); err != nil {
// 			return err
// 		}
// 		resp := md.Response{
// 			TopicData: topicData,
// 		}
// 		fmt.Println("NSQ_DATA : ", topicData)
// 		respByte, err := json.Marshal(resp)
// 		if err != nil {
// 			return err
// 		}
// 		fmt.Println("Here 1")
// 		if err := sendResponse(ctx, conxn, typ, respByte); err != nil {
// 			fmt.Println("Here 2")
// 			return err
// 		}
// 		return nil
// 	}))
// 	if err := consumer.ConnectToNSQD("127.0.0.1:4150"); err != nil {
// 		return err
// 	}

// 	signalChannel := make(chan os.Signal, 1)
// 	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)
// 	<-signalChannel
// 	fmt.Println("Consumer shutting down .......")
// 	return nil
// }



