package models

// SensorValue 表示单个测点的数据
type SensorValue struct {
	Value     float64 `json:"value"`
	Timestamp int64   `json:"timestamp"`
	State     int     `json:"state"`
}

// BatchMessage MQTT 收到的批量消息（key 为测点名，value 为 SensorValue）
type BatchMessage map[string]SensorValue

// KafkaMessage 写入 Kafka 的消息结构（每条记录一个测点）
type KafkaMessage struct {
	TagName   string  `json:"tag_name"`
	Value     float64 `json:"value"`
	Timestamp int64   `json:"timestamp"`
	Quality   int     `json:"quality"`
}
