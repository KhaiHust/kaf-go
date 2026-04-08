package config

type ClusterConfig struct {
	Brokers []BrokerConfig
}

type BrokerConfig struct {
	BrokerId int32
	Host     string
	Port     int32
}
