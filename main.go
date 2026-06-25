package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"write-to-kafka/config"
	"write-to-kafka/kafka"
	"write-to-kafka/models"
	"write-to-kafka/mqtt"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmsgprefix)
	log.SetPrefix("[write-to-kafka] ")

	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}
	log.Printf("配置加载完成: Kafka=%v", cfg.Kafka.Brokers)

	// ── 构建管道 ──────────────────────────────────────
	pipelines := buildPipelines(cfg)
	if len(pipelines) == 0 {
		log.Fatalf("没有配置任何管道，请设置 PIPELINES 环境变量或 config.json")
	}
	for _, pl := range pipelines {
		ptInfo := "无点表"
		if pl.PointsTable != nil {
			ptInfo = fmt.Sprintf("点表(%d条)", pl.PointsTable.Len())
		}
		log.Printf("管道: MQTT[%s@%s] → Kafka[%s] %s",
			pl.MqttTopic, pl.MqttBroker, pl.KafkaTopic, ptInfo)
	}

	msgChan := make(chan models.DeviceBatchMessage, 1024)

	producer, err := kafka.NewProducer(cfg.Kafka)
	if err != nil {
		log.Fatalf("创建 Kafka 生产者失败: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go producer.Start(ctx, msgChan)

	consumer, err := mqtt.NewConsumer(cfg.Mqtt, msgChan, pipelines)
	if err != nil {
		log.Fatalf("创建 MQTT 消费者失败: %v", err)
	}
	_ = consumer

	log.Printf("服务启动成功，等待 MQTT 消息...")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("收到信号 %v，正在退出...", sig)
}

// buildPipelines 根据配置构建管道列表，加载点表。
// 优先使用 Pipelines（多管道模式），为空时回退到 Mqtt.Topics + Kafka.Topic（单管道兼容模式）。
// 每条管道可覆盖全局 MQTT broker 配置，实现不同 topic 连接不同 broker。
func buildPipelines(cfg *config.AppSettings) []*mqtt.Pipeline {
	var pipelines []*mqtt.Pipeline

	if len(cfg.Pipelines) > 0 {
		for _, pc := range cfg.Pipelines {
			pl := &mqtt.Pipeline{
				MqttTopic:  pc.MqttTopic,
				KafkaTopic: pc.KafkaTopic,
			}

			// 解析 MQTT 连接参数：管道级覆盖 > 全局配置
			broker := pc.MqttBroker
			if broker == "" {
				broker = cfg.Mqtt.Broker
			}
			port := pc.MqttPort
			if port == 0 {
				port = cfg.Mqtt.Port
			}
			broker = formatBrokerURL(broker, port)
			pl.MqttBroker = broker

			pl.ClientID = pc.MqttClientID
			if pl.ClientID == "" {
				pl.ClientID = cfg.Mqtt.ClientID
			}

			pl.Username = pc.MqttUsername
			if pl.Username == "" {
				pl.Username = cfg.Mqtt.Username
			}
			pl.Password = pc.MqttPassword
			if pl.Password == "" {
				pl.Password = cfg.Mqtt.Password
			}

			if pc.MqttQoS > 0 {
				pl.QoS = byte(pc.MqttQoS)
			} else {
				pl.QoS = cfg.Mqtt.QoS
			}

			if pc.PointsFile != "" {
				pt, err := models.LoadPointsTable(pc.PointsFile)
				if err != nil {
					log.Printf("警告: 加载点表失败 %s: %v", pc.PointsFile, err)
				} else {
					pl.PointsTable = pt
				}
			}

			pipelines = append(pipelines, pl)
		}
		return pipelines
	}

	// 兼容模式：从旧 Mqtt.Topics + Kafka.Topic 构建管道
	broker := formatBrokerURL(cfg.Mqtt.Broker, cfg.Mqtt.Port)
	for _, topic := range cfg.Mqtt.Topics {
		pipelines = append(pipelines, &mqtt.Pipeline{
			MqttTopic:   topic,
			KafkaTopic:  cfg.Kafka.Topic,
			MqttBroker:  broker,
			ClientID:    cfg.Mqtt.ClientID,
			Username:    cfg.Mqtt.Username,
			Password:    cfg.Mqtt.Password,
			QoS:         cfg.Mqtt.QoS,
		})
	}
	return pipelines
}

func loadConfig() (*config.AppSettings, error) {
	cfg := &config.AppSettings{
		Mqtt: config.MqttConfig{
			Broker:   lookupEnv("MQTT__BROKER", "tcp://127.0.0.1"),
			Port:     lookupEnvInt("MQTT__PORT", 1883),
			ClientID: lookupEnv("MQTT__CLIENTID", "write-to-kafka"),
			Username: os.Getenv("MQTT__USERNAME"),
			Password: os.Getenv("MQTT__PASSWORD"),
			QoS:      byte(lookupEnvInt("MQTT__QOS", 1)),
			Topics:   lookupEnvSlice("MQTT__TOPICS", []string{"sensors/batch"}),
		},
		Kafka: config.KafkaConfig{
			Brokers: lookupEnvSlice("KAFKA__BROKERS", []string{"127.0.0.1:9092"}),
			Topic:   lookupEnv("KAFKA__TOPIC", "sensor-data"),
			SASL: config.KafkaSASLConfig{
				Mechanism:         lookupEnv("KAFKA__SASL_MECHANISM", "none"),
				Username:          os.Getenv("KAFKA__SASL_USER"),
				Password:          os.Getenv("KAFKA__SASL_PASSWORD"),
				GSSAPIAuthType:    lookupEnv("KAFKA__SASL_GSSAPI_AUTH_TYPE", "ccache"),
				GSSAPIRealm:       os.Getenv("KAFKA__SASL_GSSAPI_REALM"),
				GSSAPIServiceName: lookupEnv("KAFKA__SASL_GSSAPI_SERVICE_NAME", "kafka"),
				GSSAPIDomainName:  os.Getenv("KAFKA__SASL_GSSAPI_DOMAIN_NAME"),
				GSSAPIKeyTabPath:  lookupEnv("KAFKA__SASL_GSSAPI_KEYTAB_PATH", ""),
				GSSAPICCachePath:  os.Getenv("KAFKA__SASL_GSSAPI_CCACHE_PATH"),
				KRB5ConfigPath:    lookupEnv("KAFKA__KRB5_CONFIG", "/etc/krb5.conf"),
			},
		},
	}

	// 加载 PIPELINES 环境变量（JSON 数组）
	if pipelinesJSON := os.Getenv("PIPELINES"); pipelinesJSON != "" {
		var pipelines []config.PipelineConfig
		if err := json.Unmarshal([]byte(pipelinesJSON), &pipelines); err != nil {
			log.Printf("警告: 解析 PIPELINES 环境变量失败: %v", err)
		} else {
			cfg.Pipelines = pipelines
		}
	}

	configFile := os.Getenv("CONFIG_FILE")
	if configFile == "" {
		configFile = "config.json"
	}

	if data, err := os.ReadFile(configFile); err == nil {
		var fileCfg config.AppSettings
		if err := json.Unmarshal(data, &fileCfg); err == nil {
			mergeConfig(cfg, &fileCfg)
		}
	}

	return cfg, nil
}

func mergeConfig(cfg, fileCfg *config.AppSettings) {
	if os.Getenv("MQTT__BROKER") == "" && fileCfg.Mqtt.Broker != "" {
		cfg.Mqtt.Broker = fileCfg.Mqtt.Broker
	}
	if os.Getenv("MQTT__PORT") == "" && fileCfg.Mqtt.Port != 0 {
		cfg.Mqtt.Port = fileCfg.Mqtt.Port
	}
	if os.Getenv("MQTT__CLIENTID") == "" && fileCfg.Mqtt.ClientID != "" {
		cfg.Mqtt.ClientID = fileCfg.Mqtt.ClientID
	}
	if os.Getenv("MQTT__USERNAME") == "" && fileCfg.Mqtt.Username != "" {
		cfg.Mqtt.Username = fileCfg.Mqtt.Username
	}
	if os.Getenv("MQTT__PASSWORD") == "" && fileCfg.Mqtt.Password != "" {
		cfg.Mqtt.Password = fileCfg.Mqtt.Password
	}
	if len(fileCfg.Mqtt.Topics) > 0 {
		cfg.Mqtt.Topics = fileCfg.Mqtt.Topics
	}

	if len(fileCfg.Kafka.Brokers) > 0 {
		cfg.Kafka.Brokers = fileCfg.Kafka.Brokers
	}
	if fileCfg.Kafka.Topic != "" {
		cfg.Kafka.Topic = fileCfg.Kafka.Topic
	}

	// 管道配置：环境变量 PIPELINES 优先，否则使用 config.json 中的 pipelines
	if os.Getenv("PIPELINES") == "" && len(fileCfg.Pipelines) > 0 {
		cfg.Pipelines = fileCfg.Pipelines
	}

	if os.Getenv("KAFKA__SASL_MECHANISM") == "" && fileCfg.Kafka.SASL.Mechanism != "" {
		cfg.Kafka.SASL.Mechanism = fileCfg.Kafka.SASL.Mechanism
	}
	if os.Getenv("KAFKA__SASL_USER") == "" && fileCfg.Kafka.SASL.Username != "" {
		cfg.Kafka.SASL.Username = fileCfg.Kafka.SASL.Username
	}
	if os.Getenv("KAFKA__SASL_PASSWORD") == "" && fileCfg.Kafka.SASL.Password != "" {
		cfg.Kafka.SASL.Password = fileCfg.Kafka.SASL.Password
	}
	if os.Getenv("KAFKA__SASL_GSSAPI_REALM") == "" && fileCfg.Kafka.SASL.GSSAPIRealm != "" {
		cfg.Kafka.SASL.GSSAPIRealm = fileCfg.Kafka.SASL.GSSAPIRealm
	}
	if os.Getenv("KAFKA__SASL_GSSAPI_DOMAIN_NAME") == "" && fileCfg.Kafka.SASL.GSSAPIDomainName != "" {
		cfg.Kafka.SASL.GSSAPIDomainName = fileCfg.Kafka.SASL.GSSAPIDomainName
	}
}

func formatBrokerURL(broker string, port int) string {
	if len(broker) < 6 || (broker[:3] != "tcp" && broker[:3] != "ssl" && broker[:3] != "wss") {
		proto := "tcp"
		if port == 8883 {
			proto = "ssl"
		}
		broker = proto + "://" + broker
	}
	// 如果 broker 中已包含端口号则不再追加
	if !hasPort(broker) {
		broker = fmt.Sprintf("%s:%d", broker, port)
	}
	return broker
}

func hasPort(rawURL string) bool {
	s := rawURL
	if idx := strings.Index(s, "://"); idx != -1 {
		s = s[idx+3:]
	}
	_, _, err := net.SplitHostPort(s)
	return err == nil
}

func lookupEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func lookupEnvInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		var i int
		if _, err := fmt.Sscanf(val, "%d", &i); err == nil {
			return i
		}
	}
	return defaultVal
}

func lookupEnvSlice(key string, defaultVal []string) []string {
	if val := os.Getenv(key); val != "" {
		var s []string
		if err := json.Unmarshal([]byte(val), &s); err == nil {
			return s
		}
		return []string{val}
	}
	return defaultVal
}
