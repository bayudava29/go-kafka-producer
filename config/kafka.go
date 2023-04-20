package config

import (
	"encoding/binary"
	"encoding/json"
	"errors"

	// "log"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	"github.com/riferrei/srclient"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type Schema struct {
	Profile *srclient.Schema
}

type Producer struct {
	Profile *kafka.Producer
}

// InitProducer initialisasi producer
func InitProducer() (*kafka.Producer, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": viper.GetString("KAFKA_SERVERS"),
		// Uncomment jika kafka membutuhkan autentikasi
		// "sasl.mechanisms":   "PLAIN",
		// "security.protocol": "SASL_SSL",
		// "sasl.username":     viper.GetString("KAFKA_SASL_USERNAME"),
		// "sasl.password":     viper.GetString("KAFKA_SASL_PASSWORD"),
	})

	if err != nil {
		log.Error(fmt.Sprintf("Failed to create producer: %s", err))
		return nil, err
	}

	return producer, nil
}

func GetLastSchema(topic string) (*srclient.Schema, error) {
	topic = topic + "-value"

	schemaRegistryClient := srclient.CreateSchemaRegistryClient(viper.GetString("KAFKA_SCHEMA_REGISTRY_URL"))
	schemaRegistryClient.SetCredentials(viper.GetString("KAFKA_SASL_USERNAME"), viper.GetString("KAFKA_SASL_PASSWORD"))

	schema, errSchema := schemaRegistryClient.GetLatestSchema(topic)
	if errSchema != nil {
		log.Error(fmt.Sprintf("Failed to get schema: %s", errSchema.Error()))
		return nil, errSchema
	}

	return schema, nil
}

func SendMessage(producer *kafka.Producer, topic string, key string, recordValue []byte) {
	producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic: &topic, Partition: kafka.PartitionAny},
		Key: []byte(key), Value: recordValue}, nil)
}

func ResolvePayloadMessage(message interface{}, topic string, schema Schema) (*string, []byte, error) {
	value, _ := json.Marshal(message)
	key := uuid.NewString()

	schemaIDBytes := make([]byte, 4)
	schemaID := schema.Profile.ID()
	binary.BigEndian.PutUint32(schemaIDBytes, uint32(schemaID))

	native, _, err := schema.Profile.Codec().NativeFromTextual(value)
	if err != nil {
		log.Error("Error convert avro data from Textual")
		return nil, nil, errors.New("error convert avro data from textual")
	}

	valueBytes, err := schema.Profile.Codec().BinaryFromNative(nil, native)
	if err != nil {
		log.Error("Error append binary from Native")
		return nil, nil, errors.New("error append binary from native")
	}

	var recordValue []byte
	recordValue = append(recordValue, byte(0))
	recordValue = append(recordValue, schemaIDBytes...)
	recordValue = append(recordValue, valueBytes...)

	return &key, recordValue, nil
}
