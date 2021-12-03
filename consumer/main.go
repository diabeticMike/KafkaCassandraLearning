package main

import (
	"time"

	cluster "github.com/bsm/sarama-cluster"
	"github.com/gocql/gocql"
)

const (
	broker           = "localhost:9092"
	topic            = "topic1"
	consumerGroup    = "my-consumer-group2"
	cassandraHost    = "localhost:9042"
	keyspace         = "test"
	timeout          = 10
	batchWaitTime    = 10 * time.Second
	maxBatchSize     = 3
	createOddMessage = `
  		INSERT INTO odd (id, key, msg_value, indx) VALUES (?, ?, ?, ?);`
	createEvenMessage = `
  		INSERT INTO even (id, key, msg_value, indx) VALUES (?, ?, ?, ?);`
)

// Cassandra cassandra instance
type Cassandra struct {
	Client  *gocql.ClusterConfig
	Session *gocql.Session
}

func main() {
	var (
		msgHandler MessageHandler
		cassClient *Cassandra
		consumer   *cluster.Consumer
		err        error
	)
	cassClient, err = Load([]string{cassandraHost}, keyspace, timeout)
	if err != nil {
		panic(err)
	}
	defer cassClient.Session.Close()

	consumer, err = setupConsumer([]string{broker})
	if err != nil {
		panic(err)
	}
	defer func() {
		if err = consumer.Close(); err != nil {
			panic(err)
		}
	}()

	msgHandler = NewMessageHandler(NewMessageValidator(), consumer, cassClient.Session)
	if err = msgHandler.handle(); err != nil {
		panic(err)
	}
}

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

func setupConsumer(brokers []string) (*cluster.Consumer, error) {
	// init (custom) config, enable errors and notifications
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.Offsets.AutoCommit.Interval = time.Second
	config.Consumer.Offsets.CommitInterval = time.Second

	// init consumer
	topics := []string{topic}
	consumer, err := cluster.NewConsumer(brokers, consumerGroup, topics, config)
	if err != nil {
		return nil, err
	}

	return consumer, nil
}
