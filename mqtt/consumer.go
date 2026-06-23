package mqtt

import (
	"encoding/json"
	"log"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"write-to-kafka/config"
	"write-to-kafka/models"
)

// Consumer MQTT 消费者，订阅主题并将消息转发到内部 channel
type Consumer struct {
	client  mqtt.Client
	cfg     config.MqttConfig
	outgoing chan<- models.BatchMessage
}

// NewConsumer 创建 MQTT 消费者并建立连接
func NewConsumer(cfg config.MqttConfig, outgoing chan<- models.BatchMessage) (*Consumer, error) {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(cfg.Broker)
	opts.SetClientID(cfg.ClientID)
	opts.SetUsername(cfg.Username)
	opts.SetPassword(cfg.Password)
	opts.SetCleanSession(true)
	opts.SetAutoReconnect(true)
	opts.SetConnectionLostHandler(func(c mqtt.Client, err error) {
		log.Printf("[MQTT] 连接断开: %v", err)
	})
	opts.SetOnConnectHandler(func(c mqtt.Client) {
		log.Printf("[MQTT] 已连接至 %s", cfg.Broker)
	})

	client := mqtt.NewClient(opts)
	token := client.Connect()
	if !token.WaitTimeout(10 * time.Second) {
		return nil, token.Error()
	}
	if token.Error() != nil {
		return nil, token.Error()
	}

	c := &Consumer{
		client:   client,
		cfg:      cfg,
		outgoing: outgoing,
	}

	for _, topic := range cfg.Topics {
		if err := c.subscribe(topic); err != nil {
			return nil, err
		}
	}

	return c, nil
}

func (c *Consumer) subscribe(topic string) error {
	token := c.client.Subscribe(topic, c.cfg.QoS, c.handler)
	if !token.WaitTimeout(10 * time.Second) {
		return token.Error()
	}
	log.Printf("[MQTT] 已订阅主题: %s (QoS %d)", topic, c.cfg.QoS)
	return token.Error()
}

// handler MQTT 消息处理回调
func (c *Consumer) handler(client mqtt.Client, msg mqtt.Message) {
	var batch models.BatchMessage
	if err := json.Unmarshal(msg.Payload(), &batch); err != nil {
		log.Printf("[MQTT] 消息解析失败 (topic=%s): %v", msg.Topic(), err)
		return
	}

	log.Printf("[MQTT] 收到消息 topic=%s, 包含 %d 个测点", msg.Topic(), len(batch))

	// 非阻塞发送到 channel（如果 channel 满则丢弃，避免阻塞 MQTT 回调）
	select {
	case c.outgoing <- batch:
	default:
		log.Printf("[MQTT] channel 已满，丢弃消息 (topic=%s)", msg.Topic())
	}
}
