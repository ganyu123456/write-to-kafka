package config

// AppSettings 整体配置
type AppSettings struct {
	Mqtt      MqttConfig       `yaml:"mqtt" json:"mqtt"`
	Kafka     KafkaConfig      `yaml:"kafka" json:"kafka"`
	Pipelines []PipelineConfig `yaml:"pipelines" json:"pipelines"`
}

// PipelineConfig 单条 MQTT → Kafka 管道配置
type PipelineConfig struct {
	MqttTopic  string `yaml:"mqtt_topic" json:"mqtt_topic"`
	KafkaTopic string `yaml:"kafka_topic" json:"kafka_topic"`
	PointsFile string `yaml:"points_file" json:"points_file"`

	// 可选：覆盖全局 MQTT broker 配置（为空则使用全局 MqttConfig）
	MqttBroker   string `yaml:"mqtt_broker" json:"mqtt_broker"`
	MqttPort     int    `yaml:"mqtt_port" json:"mqtt_port"`
	MqttClientID string `yaml:"mqtt_client_id" json:"mqtt_client_id"`
	MqttUsername string `yaml:"mqtt_username" json:"mqtt_username"`
	MqttPassword string `yaml:"mqtt_password" json:"mqtt_password"`
	MqttQoS      int    `yaml:"mqtt_qos" json:"mqtt_qos"`
}

// MqttConfig MQTT 客户端配置
type MqttConfig struct {
	Broker   string   `yaml:"broker" json:"broker"`
	Port     int      `yaml:"port" json:"port"`
	ClientID string   `yaml:"client_id" json:"client_id"`
	Username string   `yaml:"username" json:"username"`
	Password string   `yaml:"password" json:"password"`
	QoS      byte     `yaml:"qos" json:"qos"`
	Topics   []string `yaml:"topics" json:"topics"`
}

// KafkaSASLConfig SASL 认证配置
type KafkaSASLConfig struct {
	// SASL 机制: "plain", "gssapi" (Kerberos)
	Mechanism string `yaml:"mechanism" json:"mechanism"`
	// SASL/PLAIN 用
	Username string `yaml:"username" json:"username"`
	Password string `yaml:"password" json:"password"`
	// SASL/GSSAPI (Kerberos) 用
	// 认证方式: "password", "keytab", "ccache"
	GSSAPIAuthType    string `yaml:"gssapi_auth_type" json:"gssapi_auth_type"`
	GSSAPIRealm       string `yaml:"gssapi_realm" json:"gssapi_realm"`
	GSSAPIServiceName string `yaml:"gssapi_service_name" json:"gssapi_service_name"`
	GSSAPIDomainName  string `yaml:"gssapi_domain_name" json:"gssapi_domain_name"`
	GSSAPIKeyTabPath  string `yaml:"gssapi_keytab_path" json:"gssapi_keytab_path"`
	GSSAPICCachePath  string `yaml:"gssapi_ccache_path" json:"gssapi_ccache_path"`
	// Kerberos 配置文件路径（默认 /etc/krb5.conf）
	KRB5ConfigPath string `yaml:"krb5_config_path" json:"krb5_config_path"`
}

// KafkaConfig Kafka 生产者配置
type KafkaConfig struct {
	Brokers []string        `yaml:"brokers" json:"brokers"`
	Topic   string          `yaml:"topic" json:"topic"`
	SASL    KafkaSASLConfig `yaml:"sasl" json:"sasl"`
}
