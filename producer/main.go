package main

import (
	"fmt"
	"net/http"
	"time"

	"github.com/KafkaTasks/producer/web"

	"github.com/Shopify/sarama"
	"github.com/gocql/gocql"
	"github.com/gorilla/mux"
)

const (
	broker        = "localhost:9092"
	topic         = "topic1"
	cassandraHost = "localhost:9042"
	keyspace      = "test"
	timeout       = 10
)

// Cassandra cassandra instance
type Cassandra struct {
	Client  *gocql.ClusterConfig
	Session *gocql.Session
}

func main() {
	c, err := Load([]string{cassandraHost}, keyspace, timeout)
	if err != nil {
		panic(err)
	}
	fmt.Println(c)

	configureTopics([]string{broker})
	producer, err := newProducer()
	if err != nil {
		panic(err)
	}

	ctr := web.NewController(producer, c.Session, topic)
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/send", ctr.SendMessage).Methods(http.MethodPost)
	router.HandleFunc("/{id}/get", ctr.GetMessage).Methods(http.MethodGet)
	router.HandleFunc("/get/all", ctr.GetAllMessages).Methods(http.MethodGet)

	if err = http.ListenAndServe("localhost:8081", router); err != nil {
		panic(err)
	}
}

var brokers = []string{"localhost:9092"}

// Load creates a Cassandra Client and populates it with initial data
func Load(hosts []string, keyspace string, timeout int) (*Cassandra, error) {
	clusterConfig := gocql.NewCluster(hosts...)
	clusterConfig.ProtoVersion = 4
	clusterConfig.Timeout = time.Duration(timeout) * time.Second
	clusterConfig.ConnectTimeout = clusterConfig.Timeout
	clusterConfig.DefaultTimestamp = false
	clusterConfig.Consistency = gocql.LocalQuorum

	s := &Cassandra{Client: clusterConfig}
	s.Client.Keyspace = keyspace

	session, err := s.Client.CreateSession()
	if err != nil {
		return nil, err
	}

	s.Session = session

	return s, nil
}

func newProducer() (sarama.SyncProducer, error) {
	producerConfig := sarama.NewConfig()
	producerConfig.Producer.Partitioner = sarama.NewHashPartitioner
	producerConfig.Producer.RequiredAcks = sarama.WaitForAll
	producerConfig.Producer.Return.Successes = true
	producerConfig.Producer.Return.Errors = true
	//producerConfig.Producer.Flush.Messages = 0

	producer, err := sarama.NewSyncProducer(brokers, producerConfig)
	if err != nil {
		return nil, err
	}

	return producer, nil
}

func configureTopics(brokers []string) {
	config := sarama.NewConfig()
	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		panic(err)
	}

	topics, err := admin.ListTopics()
	if _, ok := topics[topic]; !ok {
		err = admin.CreateTopic(
			topic,
			&sarama.TopicDetail{
				NumPartitions:     3,
				ReplicationFactor: 1,
				ReplicaAssignment: nil,
				ConfigEntries: map[string]*string{
					"cleanup.policy":      strptr("delete"),
					"delete.retention.ms": strptr("3600000"),
				},
			},
			false,
		)
		if err != nil {
			panic(err)
		}
	}

	fmt.Println(admin.ListTopics())
}

func strptr(str string) *string { return &str }
