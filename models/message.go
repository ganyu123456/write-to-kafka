package models

// SensorValue 表示单个测点的数据
type SensorValue struct {
	Value     float64 `json:"value"`
	Timestamp int64   `json:"timestamp"`
	State     int     `json:"state"`
}

// BatchMessage MQTT 消息中 batchData 字段（key 为测点名，value 为 SensorValue）
type BatchMessage map[string]SensorValue

// DeviceBatchMessage MQTT 消息外层包装
// 实际消息格式：
//
//	{
//	  "timestamp": 1780076839551,      ← 消息发送时间，Unix 毫秒
//	  "deviceId":  "sis-collect-dev-dy",
//	  "batchData": { ... }
//	}
type DeviceBatchMessage struct {
	Timestamp int64        `json:"timestamp"`
	DeviceID  string       `json:"deviceId"`
	BatchData BatchMessage `json:"batchData"`
}

// KafkaMessage 写入 Kafka 的消息结构（每条记录一个测点）
type KafkaMessage struct {
	TagName   string  `json:"tag_name"`
	DeviceID  string  `json:"device_id"`
	Value     float64 `json:"value"`
	Timestamp int64   `json:"timestamp"`
	Quality   int     `json:"quality"`
}
