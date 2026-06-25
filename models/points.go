package models

import (
	"encoding/csv"
	"fmt"
	"os"
	"strings"
)

// PointsTable 点表：MQTT 测点名 -> Kafka 测点名的映射（兼白名单过滤）
type PointsTable struct {
	mqttToKafka map[string]string
}

// LoadPointsTable 从 CSV 文件加载点表。
// CSV 格式：mqtt_point_name,kafka_point_name（无表头）
func LoadPointsTable(filePath string) (*PointsTable, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("打开点表文件失败 %s: %w", filePath, err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.TrimLeadingSpace = true

	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("解析点表CSV失败 %s: %w", filePath, err)
	}

	pt := &PointsTable{
		mqttToKafka: make(map[string]string, len(records)),
	}

	for _, record := range records {
		if len(record) < 2 {
			continue
		}
		mqttName := strings.TrimSpace(record[0])
		kafkaName := strings.TrimSpace(record[1])
		if mqttName == "" || kafkaName == "" {
			continue
		}
		pt.mqttToKafka[mqttName] = kafkaName
	}

	return pt, nil
}

// Filter 过滤并重命名批量数据。
// 只保留点表中存在的测点，并将名称转换为 Kafka 侧的名称。
// 如果点表为空（未配置），则返回原始数据不做过滤。
func (pt *PointsTable) Filter(batch BatchMessage) BatchMessage {
	if pt == nil || len(pt.mqttToKafka) == 0 {
		return batch
	}
	result := make(BatchMessage, len(batch))
	for mqttName, sv := range batch {
		if kafkaName, ok := pt.mqttToKafka[mqttName]; ok {
			result[kafkaName] = sv
		}
	}
	return result
}

// Len 返回点表条目数
func (pt *PointsTable) Len() int {
	if pt == nil {
		return 0
	}
	return len(pt.mqttToKafka)
}
