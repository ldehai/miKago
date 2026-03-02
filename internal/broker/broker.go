package broker

// Config holds broker configuration.
type Config struct {
	BrokerID int32
	Host     string
	Port     int32
}

// DefaultConfig returns default broker configuration.
func DefaultConfig() Config {
	return Config{
		BrokerID: 0,
		Host:     "localhost",
		Port:     9092,
	}
}

// Broker represents the miKago broker.
type Broker struct {
	Config       Config
	TopicManager *TopicManager
}

// NewBroker creates a new Broker with the given config.
func NewBroker(cfg Config) *Broker {
	return &Broker{
		Config:       cfg,
		TopicManager: NewTopicManager(),
	}
}
