package broker

// Default configuration values matching Kafka defaults.
const (
	DefaultMaxMessageBytes int32 = 1 * 1024 * 1024        // 1MB - message.max.bytes
	DefaultMaxRequestBytes int32 = 100 * 1024 * 1024      // 100MB - max request size on the wire
	DefaultLogSegmentBytes int64 = 1 * 1024 * 1024 * 1024 // 1GB - log.segment.bytes
)

// Config holds the broker configuration.
type Config struct {
	BrokerID        int32
	Host            string
	Port            int32
	DataDir         string
	MaxMessageBytes int32 // max size of a single message (key + value)
	MaxRequestBytes int32 // max size of a single request on the wire
	LogSegmentBytes int64 // max size of a single log segment file
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
	if cfg.MaxMessageBytes <= 0 {
		cfg.MaxMessageBytes = DefaultMaxMessageBytes
	}
	if cfg.MaxRequestBytes <= 0 {
		cfg.MaxRequestBytes = DefaultMaxRequestBytes
	}
	if cfg.LogSegmentBytes <= 0 {
		cfg.LogSegmentBytes = DefaultLogSegmentBytes
	}
	return &Broker{
		Config:       cfg,
		TopicManager: NewTopicManager(cfg.DataDir, cfg.LogSegmentBytes),
	}
}

// Close shuts down the broker, flushing all data to disk.
func (b *Broker) Close() error {
	return b.TopicManager.Close()
}
