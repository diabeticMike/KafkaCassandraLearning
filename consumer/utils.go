package main

import (
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/gocql/gocql"
	"github.com/google/uuid"
)

type MessageHandler interface {
	handle() error
	enrich(kafkaMessage *sarama.ConsumerMessage, m *Message)
}

type MessageValidator interface {
	validate(m Message) bool
}

var lastMessage = struct {
	kafkaMsg *sarama.ConsumerMessage
	mu       *sync.Mutex
}{mu: &sync.Mutex{}}

// Message represents cql object's info
type Message struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Index int    `json:"-"`
}

func NewMessageHandler(
	validator MessageValidator,
	kafkaConsumer *cluster.Consumer,
	cassandraSession *gocql.Session,
) *messageHandler {
	return &messageHandler{
		validator:        validator,
		kafkaConsumer:    kafkaConsumer,
		cassandraSession: cassandraSession,
	}
}

type messageHandler struct {
	validator        MessageValidator
	kafkaConsumer    *cluster.Consumer
	cassandraSession *gocql.Session
}

func (mh *messageHandler) handle() error {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// consume errors
	go func() {
		for err := range mh.kafkaConsumer.Errors() {
			panic(err)
		}
	}()

	// consume notifications
	go func() {
		for ntf := range mh.kafkaConsumer.Notifications() {
			fmt.Println("got notification", ntf)
		}
	}()
	//preparedOddStatement := mh.cassandraSession.Query(createOddMessage)
	//preparedEvenStatement := mh.cassandraSession.Query(createEvenMessage)
	start := time.Now()

	for i := 0; i < 1000; i++ {
		err := mh.cassandraSession.Query(createEvenMessage).Bind(
			gocql.UUID(uuid.New()),
			"key",
			"value",
			1,
		).Consistency(gocql.All).Consistency(gocql.All).Exec()
		if err != nil {
			panic(err)
		}
	}

	fmt.Println(time.Since(start).Milliseconds())
	//batch := mh.newBatchChan(preparedEvenStatement, preparedOddStatement)
	//
	//fmt.Println("GroupID:", consumerGroup)
	//// consume messages, watch signals
	//for {
	//	select {
	//	case msg, ok := <-mh.kafkaConsumer.Messages():
	//		if ok {
	//			fmt.Printf(
	//				"Message partition: %d\nMessage key: %s\nMessage value: %s\n",
	//				msg.Partition,
	//				msg.Key,
	//				msg.Value,
	//			)
	//			var message Message
	//			if err := json.NewDecoder(bytes.NewBuffer(msg.Value)).Decode(&message); err != nil {
	//				fmt.Println(err.Error())
	//				return err
	//			}
	//
	//			mh.enrich(msg, &message)
	//
	//			batch <- message
	//		}
	//	case sig := <-signals:
	//		return errors.New(sig.String())
	//	}
	//}

	return nil
}

func (mh *messageHandler) enrich(kafkaMessage *sarama.ConsumerMessage, m *Message) {
	lastMessage.mu.Lock()
	lastMessage.kafkaMsg = kafkaMessage
	lastMessage.mu.Unlock()
	m.Index = rand.Intn(100)
}

func (mh *messageHandler) newBatchChan(preparedEventStat, preparedOddStat *gocql.Query) chan<- Message {
	ch := make(chan Message)
	batch := make([]Message, 0, maxBatchSize)
	ticker := time.Tick(batchWaitTime)
	go func() {
		for {
			select {
			case <-ticker:
				fmt.Printf("%d sec is out\n", batchWaitTime)
				fmt.Printf("Batch content: %v \n", batch)
				for _, v := range batch {
					err := mh.insertMessage(v, preparedEventStat, preparedOddStat)
					if err != nil {
						panic(err)
					}
				}
				if len(batch) > 0 {
					// commit message only when it successfully stored into DB
					fmt.Println("COMMIT")
					mh.kafkaConsumer.MarkOffset(lastMessage.kafkaMsg, "")
				}
				batch = make([]Message, 0, maxBatchSize)
				ticker = time.Tick(batchWaitTime)
			case msg := <-ch:
				if len(batch) == maxBatchSize-1 {
					fmt.Println("Batch is full")
					fmt.Printf("Batch content: %v \n", batch)
					batch = append(batch, msg)
					for _, v := range batch {
						err := mh.insertMessage(v, preparedEventStat, preparedOddStat)
						if err != nil {
							panic(err)
						}
					}
					// commit message only when it successfully stored into DB
					fmt.Println("COMMIT")
					mh.kafkaConsumer.MarkOffset(lastMessage.kafkaMsg, "")
					batch = make([]Message, 0, maxBatchSize)
					ticker = time.Tick(batchWaitTime)
				} else {
					batch = append(batch, msg)
				}
			}
		}
	}()

	return ch
}

func (mh *messageHandler) insertMessage(msg Message, evenPS, oddPS *gocql.Query) error {
	switch mh.validator.validate(msg) {
	case true:
		err := evenPS.Bind(
			gocql.UUID(uuid.New()),
			msg.Key,
			msg.Value,
			msg.Index,
		).Consistency(gocql.All).Consistency(gocql.All).Exec()
		if err != nil {
			panic(err)
		}
	case false:
		err := oddPS.Bind(
			gocql.UUID(uuid.New()),
			msg.Key,
			msg.Value,
			msg.Index,
		).Consistency(gocql.All).Consistency(gocql.All).Exec()
		if err != nil {
			panic(err)
		}
	}
	return nil
}

func NewMessageValidator() *messageValidator {
	return &messageValidator{}
}

type messageValidator struct {
}

// even - true
// odd - false
func (*messageValidator) validate(m Message) bool {
	if m.Index%2 == 0 {
		return true
	}
	return false
}
