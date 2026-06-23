package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"write-to-kafka/config"
	"write-to-kafka/models"
)

// Producer Kafka 生产者，从 channel 读取消息并写入 Kafka
type Producer struct {
	producer sarama.SyncProducer
	cfg      config.KafkaConfig
	topic    string
}

// NewProducer 创建 Kafka 生产者
func NewProducer(cfg config.KafkaConfig) (*Producer, error) {
	saramaCfg := sarama.NewConfig()

	// 生产者配置
	saramaCfg.Producer.Return.Successes = true
	saramaCfg.Producer.Return.Errors = true
	saramaCfg.Producer.RequiredAcks = sarama.WaitForLocal
	saramaCfg.Producer.Partitioner = sarama.NewHashPartitioner // 按 key hash
	saramaCfg.Producer.Flush.Bytes = 1024 * 100                // 100KB 或
	saramaCfg.Producer.Flush.Messages = 100                    // 100 条
	saramaCfg.Producer.Flush.Frequency = 500 * time.Millisecond
	saramaCfg.Producer.Timeout = 10 * time.Second

	// 网络配置
	saramaCfg.Net.DialTimeout = 10 * time.Second
	saramaCfg.Net.ReadTimeout = 30 * time.Second
	saramaCfg.Net.WriteTimeout = 30 * time.Second

	// ── SASL 认证配置 ──────────────────────────────────────
	sasl := cfg.SASL
	if sasl.Mechanism != "" && strings.ToLower(sasl.Mechanism) != "none" {
		saramaCfg.Net.SASL.Enable = true

		switch strings.ToLower(sasl.Mechanism) {
		case "plain":
			saramaCfg.Net.SASL.Mechanism = sarama.SASLTypePlaintext
			saramaCfg.Net.SASL.User = sasl.Username
			saramaCfg.Net.SASL.Password = sasl.Password

		case "gssapi", "kerberos":
			saramaCfg.Net.SASL.Mechanism = sarama.SASLTypeGSSAPI
			saramaCfg.Net.SASL.GSSAPI.ServiceName = defaultIfEmpty(sasl.GSSAPIServiceName, "kafka")
			saramaCfg.Net.SASL.GSSAPI.Username = sasl.Username
			saramaCfg.Net.SASL.GSSAPI.KerberosConfigPath = defaultIfEmpty(sasl.KRB5ConfigPath, "/etc/krb5.conf")

			if sasl.GSSAPIRealm != "" {
				saramaCfg.Net.SASL.GSSAPI.Realm = sasl.GSSAPIRealm
			}

			// 自定义 SPN 构建：当指定 domain_name 时，使用 domain 而非 broker IP
			// 对应 producer.properties 中的 kerberos.domain.name = hadoop.hadoop.com
			svcName := saramaCfg.Net.SASL.GSSAPI.ServiceName
			domainName := sasl.GSSAPIDomainName
			if domainName != "" {
				saramaCfg.Net.SASL.GSSAPI.BuildSpn = func(serviceName, host string) string {
					return svcName + "/" + domainName
				}
				log.Printf("[Kafka] Kerberos SPN: %s/%s (domain=%s)", svcName, domainName, domainName)
			}

			switch strings.ToLower(sasl.GSSAPIAuthType) {
			case "password":
				// 用户名+密码方式：程序自动获取 Kerberos 票据
				saramaCfg.Net.SASL.GSSAPI.AuthType = sarama.KRB5_USER_AUTH
				saramaCfg.Net.SASL.GSSAPI.Password = sasl.Password
				log.Printf("[Kafka] Kerberos 认证: 用户名密码方式 (user=%s, realm=%s)",
					sasl.Username, sasl.GSSAPIRealm)

			case "keytab":
				// Keytab 文件方式
				saramaCfg.Net.SASL.GSSAPI.AuthType = sarama.KRB5_KEYTAB_AUTH
				saramaCfg.Net.SASL.GSSAPI.KeyTabPath = defaultIfEmpty(sasl.GSSAPIKeyTabPath, "/etc/gyhlw.keytab")
				log.Printf("[Kafka] Kerberos 认证: keytab 文件方式 (path=%s)",
					saramaCfg.Net.SASL.GSSAPI.KeyTabPath)

			default:
				// CCACHE 方式（默认，依赖 kinit 提前获取票据）
				saramaCfg.Net.SASL.GSSAPI.AuthType = sarama.KRB5_CCACHE_AUTH
				if sasl.GSSAPICCachePath != "" {
					saramaCfg.Net.SASL.GSSAPI.CCachePath = sasl.GSSAPICCachePath
				}
				log.Printf("[Kafka] Kerberos 认证: 缓存票据方式 (需先执行 kinit %s)", sasl.Username)
			}

		default:
			return nil, fmt.Errorf("不支持的 SASL 机制: %s (可选: plain, gssapi)", sasl.Mechanism)
		}
	}

	// 版本兼容性：使用最低版本以兼容华为 FusionInsight Kafka
	saramaCfg.Version = sarama.V2_0_0_0

	producer, err := sarama.NewSyncProducer(cfg.Brokers, saramaCfg)
	if err != nil {
		return nil, fmt.Errorf("创建 Kafka 生产者失败: %w", err)
	}

	p := &Producer{
		producer: producer,
		cfg:      cfg,
		topic:    cfg.Topic,
	}

	log.Printf("[Kafka] 生产者已初始化: brokers=%v, topic=%s", cfg.Brokers, cfg.Topic)
	return p, nil
}

// Start 启动消费循环，从 channel 读取批量消息并写入 Kafka
func (p *Producer) Start(ctx context.Context, incoming <-chan models.BatchMessage) {
	log.Printf("[Kafka] 开始消费消息循环")
	for {
		select {
		case <-ctx.Done():
			log.Printf("[Kafka] 收到停止信号，关闭生产者")
			if err := p.producer.Close(); err != nil {
				log.Printf("[Kafka] 关闭 producer 失败: %v", err)
			}
			return

		case batch := <-incoming:
			if err := p.writeBatch(ctx, batch); err != nil {
				log.Printf("[Kafka] 写入批次失败: %v", err)
			}
		}
	}
}

// writeBatch 将一批测点数据写入 Kafka
func (p *Producer) writeBatch(ctx context.Context, batch models.BatchMessage) error {
	if len(batch) == 0 {
		return nil
	}

	messages := make([]*sarama.ProducerMessage, 0, len(batch))
	for tagName, sv := range batch {
		km := models.KafkaMessage{
			TagName:   tagName,
			Value:     sv.Value,
			Timestamp: sv.Timestamp,
			Quality:   sv.State,
		}

		payload, err := json.Marshal(km)
		if err != nil {
			log.Printf("[Kafka] 序列化消息失败 tag=%s: %v", tagName, err)
			continue
		}

		// 使用 tag_name 作为 key，确保同一测点进入同一分区
		messages = append(messages, &sarama.ProducerMessage{
			Topic: p.topic,
			Key:   sarama.StringEncoder(tagName),
			Value: sarama.ByteEncoder(payload),
		})
	}

	if len(messages) == 0 {
		return nil
	}

	// 分批发送（Sarama 的 SendMessages 自动处理批量）
	if err := p.producer.SendMessages(messages); err != nil {
		if pe, ok := err.(sarama.ProducerErrors); ok {
			for _, e := range pe {
				log.Printf("[Kafka] 发送失败: key=%s, err=%v", e.Msg.Key, e.Err)
			}
		}
		return fmt.Errorf("发送到 Kafka 失败: %w", err)
	}

	log.Printf("[Kafka] 成功发送 %d 条消息到 topic=%s", len(messages), p.topic)
	return nil
}

// Close 关闭生产者
func (p *Producer) Close() error {
	return p.producer.Close()
}

func defaultIfEmpty(val, defaultVal string) string {
	if val != "" {
		return val
	}
	return defaultVal
}
