package broker

// Default configuration values matching Kafka defaults.
const (
	DefaultMaxMessageBytes int32 = 1 * 1024 * 1024         // 1MB - message.max.bytes
	DefaultMaxRequestBytes int32 = 100 * 1024 * 1024       // 100MB - max request size on the wire
	DefaultLogSegmentBytes int64 = 1 * 1024 * 1024 * 1024  // 1GB - log.segment.bytes
	DefaultRetentionMs     int64 = 7 * 24 * 60 * 60 * 1000 // 7 days
)

// Config holds the broker configuration.
type Config struct {
	BrokerID        int32
	Host            string
	Port            int32
	DataDir         string
	MaxMessageBytes int32
	MaxRequestBytes int32
	LogSegmentBytes int64
	RetentionMs     int64 // log.retention.ms, -1 = keep forever
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
	if cfg.RetentionMs == 0 {
		cfg.RetentionMs = DefaultRetentionMs
	}
	return &Broker{
		Config:       cfg,
		TopicManager: NewTopicManager(cfg.DataDir, cfg.LogSegmentBytes, cfg.RetentionMs),
	}
}

// Close shuts down the broker, flushing all data to disk.
func (b *Broker) Close() error {
	return b.TopicManager.Close()
}
