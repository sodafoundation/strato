package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/micro/go-log"
	"time"
)

var Producer sarama.SyncProducer

func Init(addrs []string) error {
	log.Log("Init producer")
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Version = sarama.V2_0_0_0
	config.Admin.Timeout = 10 * time.Second

	var err error
	Producer, err = sarama.NewSyncProducer(addrs, config)
	for try := 0; try < 10; try++ {
		if err == sarama.ErrOutOfBrokers {
			time.Sleep(2 * time.Second)
			Producer, err = sarama.NewSyncProducer(addrs, config)
		}else {
			break
		}
	}
	if err != nil {
		log.Logf("Create producer failed, err:%v", err)
	}

	return err
}

func ProduceMsg(topic string, msg []byte) error{
	kafkaMsg := &sarama.ProducerMessage{Topic:topic}
	kafkaMsg.Value = sarama.ByteEncoder(msg)

	//producer.Input() <- msg
	log.Logf("send message:%s\n", kafkaMsg)
	partition, offset, err := Producer.SendMessage(kafkaMsg)
	if err != nil {
		log.Logf("Producer send message failed, err:%v\n", err)
	}else {
		log.Logf("message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
	}

	return err
}
