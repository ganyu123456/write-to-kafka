package config

// AppSettings 整体配置
type AppSettings struct {
	Mqtt  MqttConfig  `yaml:"mqtt" json:"mqtt"`
	Kafka KafkaConfig `yaml:"kafka" json:"kafka"`
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
