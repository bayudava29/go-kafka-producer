package config

import (
	"log"
	"os"

	"github.com/spf13/viper"
)

type Configuration struct {
	ENV []Settings `mapstructure:"env"`
}

type Settings struct {
	NAME  string
	VALUE string
}

func InitConfig(pipeline string) {
	viper.SetConfigType("yaml")
	viper.SetConfigName("app.config")
	viper.AddConfigPath("./config")
	viper.AutomaticEnv()

	err := viper.ReadInConfig()
	if err != nil {
		log.Fatal(err)
	}

	var env Configuration
	config := viper.Sub(pipeline)
	if config == nil { // Sub returns nil if the key cannot be found
		log.Fatalf("configuration for %s not found", pipeline)
	}

	config.Unmarshal(&env)
	for _, value := range env.ENV {
		if _, ok := os.LookupEnv(value.NAME); !ok {
			os.Setenv(value.NAME, value.VALUE)
		}
	}
}
