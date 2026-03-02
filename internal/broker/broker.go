package broker

// Config holds the broker configuration.
type Config struct {
	BrokerID int32
	Host     string
	Port     int32
	DataDir  string
}

// Broker is the main broker instance.
type Broker struct {
	Config       Config
	TopicManager *TopicManager
}

// NewBroker creates a new Broker with the given configuration.
func NewBroker(cfg Config) *Broker {
	if cfg.DataDir == "" {
		cfg.DataDir = "./data"
	}
	return &Broker{
		Config:       cfg,
		TopicManager: NewTopicManager(cfg.DataDir),
	}
}

// Close shuts down the broker, flushing all data to disk.
func (b *Broker) Close() error {
	return b.TopicManager.Close()
}
