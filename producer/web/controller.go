package web

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/Shopify/sarama"
	"github.com/gocql/gocql"
	"github.com/google/uuid"
)

func NewController(producer sarama.SyncProducer, session *gocql.Session, topic string) *Controller {
	return &Controller{producer: producer, session: session, topic: topic}
}

type Controller struct {
	producer sarama.SyncProducer
	session  *gocql.Session
	topic    string
}

type Message struct {
	Key   string
	Value string
	Index string `json:"-"`
}

const (
	selectMessage     = `SELECT msg_value FROM test.messages WHERE id = ?;`
	selectAllMessages = `SELECT id,msg_value FROM test.messages;`
)

func (c *Controller) SendMessage(w http.ResponseWriter, r *http.Request) {
	var (
		msg  Message
		body []byte
		err  error
	)

	body, err = ioutil.ReadAll(r.Body)
	if err != nil {
		log.Fatal(err)
	}
	r.Body = ioutil.NopCloser(bytes.NewBuffer(body))
	fmt.Println(string(body))

	err = json.NewDecoder(r.Body).Decode(&msg)
	if err != nil {
		log.Fatal(err)
	}

	partition, offset, err := sendMessage(c.producer, msg.Key, c.topic, string(body))
	if err != nil {
		log.Fatal(err)
	}

	w.Write([]byte(
		fmt.Sprintf(
			"Message is stored in partition(%d)/offset(%d)/key(%v)",
			partition,
			offset,
			sarama.StringEncoder(msg.Key),
		),
	))
}

func (c *Controller) GetMessage(w http.ResponseWriter, r *http.Request) {
	id, ok := mux.Vars(r)["id"]
	if !ok {
		http.Error(w, "no id provided", http.StatusBadRequest)
		return
	}

	uid, err := uuid.Parse(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	msg, err := SelectMessage(c.session, uid)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write([]byte(
		fmt.Sprintf(
			"Message value is: %s",
			msg,
		),
	))
	w.WriteHeader(http.StatusOK)
}

func (c *Controller) GetAllMessages(w http.ResponseWriter, _ *http.Request) {
	messages := SelectAllMessages(c.session)

	w.Write([]byte(
		fmt.Sprintf(
			"Messages : %+v",
			messages,
		),
	))
	w.WriteHeader(http.StatusOK)
}

func sendMessage(producer sarama.SyncProducer, key, topic, message string) (int32, int64, error) {
	msg := &sarama.ProducerMessage{
		Key:   sarama.StringEncoder(key),
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return 0, 0, err
	}

	return partition, offset, nil
}

func SelectMessage(session *gocql.Session, id uuid.UUID) (string, error) {
	query := session.Query(selectMessage, gocql.UUID(id)).Consistency(gocql.All)
	var value string
	err := query.Scan(
		&value,
	)
	return value, err
}

func SelectAllMessages(session *gocql.Session) []Message {
	iter := session.Query(selectAllMessages).Consistency(gocql.All).Iter()

	var cqlID gocql.UUID
	var messages []Message
	item := Message{}
	for iter.Scan(
		&cqlID,
		&item.Value,
	) {
		item.Key = uuid.UUID(cqlID).String()
		messages = append(messages, item)
	}
	return messages
}
