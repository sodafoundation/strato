package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"os"
	"log"
	"github.com/opensds/multi-cloud/datamover/pkg/drivers/https"
	"os/signal"
	"time"
)

var consumer *cluster.Consumer

var logger = log.New(os.Stdout, "", log.LstdFlags)

func Init(addrs []string, group string, topics []string) error{
	logger.Println("Init consumer ...")


	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	//config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Group.Return.Notifications = true
	config.Config.Version = sarama.V2_0_0_0
	config.Config.Admin.Timeout = 10 * time.Second

	var err error
	consumer,err = cluster.NewConsumer(addrs, group, topics, config)
	for try := 0; try < 10; try++ {
		if err == sarama.ErrOutOfBrokers {
			time.Sleep(2 * time.Second)
			consumer,err = cluster.NewConsumer(addrs, group, topics, config)
		}else {
			break
		}
	}
	if err != nil {
		logger.Printf("Create consumer failed, err:%v\n", err)
		return err
	}

	migration.Init()

	//log.Logf("Init consumer finish, err:%v\n", err)
	logger.Println("Init consumer finish")
	return err
}

func LoopConsume(){
	defer consumer.Close()

	//trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	//consume errors
	go func() {
		for err := range consumer.Errors() {
			logger.Printf("Error: %v\n", err)
		}
	}()

	//consume notifications
	go func() {
		for note := range consumer.Notifications() {
			logger.Printf("Rebalanced: %+v\n", note)
		}
	}()

	//consume messages, watch signals
	logger.Println("Loop: consume message.")
	for {
		select {
		case msg, ok := <-consumer.Messages():
			if ok {
				switch msg.Topic {
				case "migration":
					//TODO: think about how many jobs can run concurrently
					logger.Printf("Got an migration job:%s\n", msg.Value)
					go migration.HandleMsg(msg.Value)
				default:
					logger.Printf("Not support topic:%s\n", msg.Topic)
				}
				consumer.MarkOffset(msg, "")
			}
		case <- signals:
			logger.Println("trap system SIGINT signal")
			return
		}
	}
}
