package mqtt

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"write-to-kafka/config"
	"write-to-kafka/models"
)

// Pipeline 一条 MQTT → Kafka 管道的运行时定义
type Pipeline struct {
	MqttTopic   string
	KafkaTopic  string
	PointsTable *models.PointsTable

	// MQTT 连接参数（已解析，来自 PipelineConfig 覆盖或全局配置）
	MqttBroker string // 完整 broker URL (tcp://host:port)
	ClientID   string
	Username   string
	Password   string
	QoS        byte
}

// Consumer MQTT 消费者，管理多个 MQTT 客户端（按 broker 分组），
// 订阅主题并将消息路由到对应管道后转发到内部 channel。
type Consumer struct {
	clients          []mqtt.Client
	cfg              config.MqttConfig
	outgoing         chan<- models.DeviceBatchMessage
	topicToPipelines map[string][]*Pipeline
}

// NewConsumer 创建 MQTT 消费者并建立连接。
// 按 broker 地址分组，每个唯一 broker 创建一个 MQTT 客户端。
func NewConsumer(cfg config.MqttConfig, outgoing chan<- models.DeviceBatchMessage, pipelines []*Pipeline) (*Consumer, error) {
	// 按 broker URL 分组管道
	brokerPipelines := make(map[string][]*Pipeline)
	for _, pl := range pipelines {
		brokerPipelines[pl.MqttBroker] = append(brokerPipelines[pl.MqttBroker], pl)
	}

	// 建立 MQTT topic → pipelines 映射（所有 broker 共享同一路由表）
	topicToPipelines := make(map[string][]*Pipeline)
	for _, pl := range pipelines {
		topicToPipelines[pl.MqttTopic] = append(topicToPipelines[pl.MqttTopic], pl)
	}

	c := &Consumer{
		cfg:              cfg,
		outgoing:         outgoing,
		topicToPipelines: topicToPipelines,
	}

	// 为每个唯一 broker 创建独立的 MQTT 客户端
	idx := 0
	for brokerURL, pls := range brokerPipelines {
		firstPl := pls[0]

		clientID := firstPl.ClientID
		if clientID == "" {
			clientID = fmt.Sprintf("%s-%d", cfg.ClientID, idx)
		}

		opts := mqtt.NewClientOptions()
		opts.AddBroker(brokerURL)
		opts.SetClientID(clientID)
		opts.SetUsername(firstPl.Username)
		opts.SetPassword(firstPl.Password)
		opts.SetCleanSession(true)
		opts.SetAutoReconnect(true)
		opts.SetConnectionLostHandler(func(client mqtt.Client, err error) {
			log.Printf("[MQTT] 连接断开 (broker=%s): %v", brokerURL, err)
		})
		opts.SetOnConnectHandler(func(client mqtt.Client) {
			log.Printf("[MQTT] 已连接至 %s", brokerURL)
		})

		client := mqtt.NewClient(opts)
		token := client.Connect()
		if !token.WaitTimeout(10 * time.Second) {
			return nil, fmt.Errorf("连接 MQTT broker %s 超时: %w", brokerURL, token.Error())
		}
		if token.Error() != nil {
			return nil, fmt.Errorf("连接 MQTT broker %s 失败: %w", brokerURL, token.Error())
		}

		log.Printf("[MQTT] broker=%s client_id=%s 连接成功", brokerURL, clientID)

		// 订阅该 broker 下的所有 topic
		for _, pl := range pls {
			token := client.Subscribe(pl.MqttTopic, pl.QoS, c.handler)
			if !token.WaitTimeout(10 * time.Second) {
				return nil, fmt.Errorf("订阅 topic %s 超时 (broker=%s): %w", pl.MqttTopic, brokerURL, token.Error())
			}
			if token.Error() != nil {
				return nil, fmt.Errorf("订阅 topic %s 失败 (broker=%s): %w", pl.MqttTopic, brokerURL, token.Error())
			}
			log.Printf("[MQTT] 已订阅: %s (broker=%s, QoS=%d)", pl.MqttTopic, brokerURL, pl.QoS)
		}

		c.clients = append(c.clients, client)
		idx++
	}

	return c, nil
}

// handler MQTT 消息处理回调（所有 broker 共用）
func (c *Consumer) handler(client mqtt.Client, msg mqtt.Message) {
	var deviceMsg models.DeviceBatchMessage
	if err := json.Unmarshal(msg.Payload(), &deviceMsg); err != nil {
		log.Printf("[MQTT] 消息解析失败 (topic=%s): %v", msg.Topic(), err)
		return
	}

	if deviceMsg.BatchData == nil || len(deviceMsg.BatchData) == 0 {
		log.Printf("[MQTT] 收到空消息 (topic=%s, deviceId=%s)", msg.Topic(), deviceMsg.DeviceID)
		return
	}

	pipelines := c.topicToPipelines[msg.Topic()]
	if len(pipelines) == 0 {
		log.Printf("[MQTT] 未找到 topic=%s 对应的管道，丢弃消息", msg.Topic())
		return
	}

	log.Printf("[MQTT] 收到消息 topic=%s, deviceId=%s, 包含 %d 个测点, 匹配 %d 条管道",
		msg.Topic(), deviceMsg.DeviceID, len(deviceMsg.BatchData), len(pipelines))

	for _, pl := range pipelines {
		batch := deviceMsg.BatchData
		if pl.PointsTable != nil {
			batch = pl.PointsTable.Filter(batch)
		}
		if len(batch) == 0 {
			continue
		}

		routedMsg := deviceMsg
		routedMsg.BatchData = batch
		routedMsg.KafkaTopic = pl.KafkaTopic

		select {
		case c.outgoing <- routedMsg:
		default:
			log.Printf("[MQTT] channel 已满，丢弃消息 (topic=%s)", msg.Topic())
		}
	}
}
