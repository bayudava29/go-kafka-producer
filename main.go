package main

import (
	"flag"
	"fmt"

	"github.com/bayudava29/go-kafka-producer/config"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type ProfileData struct {
	Name string `json:"name"`
}

func main() {
	envF := flag.String("env", "local", "define environment")
	flag.Parse()

	env := *envF
	config.InitConfig(env)

	// Init Producer
	var kafkaConn *kafka.Producer
	var errKafkaConn error
	kafkaConn, errKafkaConn = config.InitProducer()
	if errKafkaConn != nil {
		log.Error(errKafkaConn)
	} else {
		defer kafkaConn.Close()
		go func() {
			for e := range kafkaConn.Events() {
				switch ev := e.(type) {
				case *kafka.Message:
					if ev.TopicPartition.Error != nil {
						log.Error(fmt.Sprintf("Delivery profile message key %s failed: %v\n", string(ev.Key), ev.TopicPartition))
					} else {
						log.Info(fmt.Sprintf("Delivered profile message key %s to %v\n", string(ev.Key), ev.TopicPartition))
					}
				}
			}
		}()
	}

	// Get Schema
	schemaProfile, errKafkaSchema := config.GetLastSchema(viper.GetString("KAFKA_PRODUCED_TOPIC_LOGGING"))
	if errKafkaSchema != nil {
		log.Error("Error kafka connection")
		log.Error(errKafkaSchema)
	}

	schema := &config.Schema{
		Profile: schemaProfile,
	}

	// Testing Data
	profileData := &ProfileData{
		Name: "Testing",
	}

	// Resolve Message then Send to Kafka
	topic := viper.GetString("KAFKA_PRODUCED_TOPIC_LOGGING")
	key, recordValue, err := config.ResolvePayloadMessage(profileData, topic, *schema)
	if err == nil {
		config.SendMessage(kafkaConn, topic, *key, recordValue)
	}
}
