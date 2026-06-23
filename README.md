# write-to-kafka

订阅 MQTT 主题，将传感器数据实时转发到 Kafka 的 Go 服务。

## 架构

```
sensor-simulator-mapper / IoT Gateway
        │
        │ MQTT 批量消息（JSON 字典）
        │ Topic: sensors/batch、sensors2/batch ...
        ▼
  MqttConsumer（消息接收）
        │
        │ Channel 传递
        ▼
  KafkaProducer（批量写入）
        │
        │ Topic: sensor-data
        ▼
     Kafka
```

## MQTT 消息格式

MQTT 客户端推送的消息为 **JSON 字典**，key 为测点名，value 包含值、Unix 秒级时间戳和状态：

```json
{
  "DDM.SIS.1DCS_BBA01XP01": {"value": 1, "timestamp": 1780041492, "state": 1},
  "DDM.SIS.1DCS_BBA01XP02": {"value": 0, "timestamp": 1780041574, "state": 1},
  "DDM.SIS.1DCS_BBA02XP01": {"value": 1, "timestamp": 1780041492, "state": 0}
}
```

| 字段        | 类型   | 说明                          |
|-------------|--------|-------------------------------|
| `value`     | double | 测点值                        |
| `timestamp` | long   | Unix 秒级时间戳               |
| `state`     | int    | 质量码：`1` = Good，`0` = Bad |

## Kafka 消息格式

每条消息对应一个测点，序列化为 JSON：

```json
{
  "tag_name": "DDM.SIS.1DCS_BBA01XP01",
  "value": 1,
  "timestamp": 1780041492,
  "quality": 1
}
```

- **Key**: `tag_name`（确保同一测点进入同一分区，保持有序）
- **Value**: 上述 JSON

## 配置说明

### 环境变量（推荐）

Go 程序通过环境变量读取配置，使用双下划线分隔层级：

| 环境变量                               | 默认值                   | 说明                              |
|---------------------------------------|-------------------------|-----------------------------------|
| `MQTT__BROKER`                        | `tcp://127.0.0.1`       | MQTT Broker 地址                  |
| `MQTT__PORT`                          | `1883`                  | MQTT 端口                         |
| `MQTT__CLIENTID`                      | `write-to-kafka`        | MQTT 客户端 ID                    |
| `MQTT__USERNAME`                      | `""`                    | MQTT 用户名                       |
| `MQTT__PASSWORD`                      | `""`                    | MQTT 密码                         |
| `MQTT__QOS`                           | `1`                     | MQTT QoS                          |
| `MQTT__TOPICS`                        | `["sensors/batch"]`     | 订阅的 MQTT 主题（JSON 数组）         |
| `KAFKA__BROKERS`                      | `["127.0.0.1:9092"]`    | Kafka Broker 列表（JSON 数组）       |
| `KAFKA__TOPIC`                        | `sensor-data`           | Kafka 目标主题                    |
| `KAFKA__SASL_MECHANISM`               | `"none"`                | SASL 机制：`none`, `plain`, `gssapi` |
| `KAFKA__SASL_USER`                    | `""`                    | SASL 用户名 / Kerberos principal  |
| `KAFKA__SASL_PASSWORD`                | `""`                    | SASL 密码 / Kerberos 密码         |
| `KAFKA__SASL_GSSAPI_AUTH_TYPE`        | `"ccache"`              | Kerberos 认证方式：`password`, `keytab`, `ccache` |
| `KAFKA__SASL_GSSAPI_REALM`            | `""`                    | Kerberos Realm                   |
| `KAFKA__SASL_GSSAPI_SERVICE_NAME`     | `"kafka"`               | Kerberos Service Name            |
| `KAFKA__SASL_GSSAPI_DOMAIN_NAME`     | `""`                    | Kerberos 域名（对应 producer.properties 中的 kerberos.domain.name）|
| `KAFKA__SASL_GSSAPI_KEYTAB_PATH`      | `""`                    | Keytab 文件路径                   |
| `KAFKA__KRB5_CONFIG`                  | `"/etc/krb5.conf"`      | krb5.conf 配置文件路径            |

### JSON 配置文件（可选）

可将配置写入 `config.json` 文件（环境变量优先级更高）：

```json
{
  "mqtt": {
    "broker": "tcp://192.168.122.231",
    "port": 1883,
    "client_id": "write-to-kafka",
    "username": "",
    "password": "",
    "qos": 1,
    "topics": ["sensors/batch"]
  },
  "kafka": {
    "brokers": ["10.81.151.147:21007"],
    "topic": "gyhlw_dp_test",
    "sasl": {
      "mechanism": "gssapi",
      "username": "gyhlw",
      "password": "Huawei12#$%",
      "gssapi_auth_type": "password",
      "gssapi_realm": "",
      "gssapi_service_name": "kafka",
      "krb5_config_path": "/etc/krb5.conf"
    }
  }
}
```

### Kerberos (GSSAPI) 认证

对端 Kafka 集群开启了 Kerberos 认证，支持三种认证方式：

#### 方式一：用户名密码方式（推荐）

程序自动调用 Kerberos KDC 获取票据，无需提前 `kinit`：

```bash
# 需配置 krb5.conf（含 KDC 地址和 Realm 信息）
KAFKA__SASL_MECHANISM=gssapi \
KAFKA__SASL_USER=gyhlw \
KAFKA__SASL_PASSWORD=Huawei12#$% \
KAFKA__SASL_GSSAPI_AUTH_TYPE=password \
KAFKA__SASL_GSSAPI_REALM=YOUR_REALM \
KAFKA__KRB5_CONFIG=/etc/krb5.conf
```

#### 方式二：缓存票据方式（需提前 kinit）

在服务器上先执行 `kinit` 获取 Kerberos 票据，程序自动使用缓存票据：

```bash
# 先手动 kinit
kinit gyhlw

# 再启动程序
KAFKA__SASL_MECHANISM=gssapi \
KAFKA__SASL_USER=gyhlw \
KAFKA__SASL_GSSAPI_AUTH_TYPE=ccache \
go run .
```

#### 方式三：Keytab 文件方式

使用 Keytab 文件进行无密码认证：

```bash
KAFKA__SASL_MECHANISM=gssapi \
KAFKA__SASL_USER=gyhlw \
KAFKA__SASL_GSSAPI_AUTH_TYPE=keytab \
KAFKA__SASL_GSSAPI_KEYTAB_PATH=/etc/gyhlw.keytab \
go run .
```

### SASL/PLAIN 认证

如果对端 Kafka 支持 SASL/PLAIN 认证：

```bash
KAFKA__SASL_MECHANISM=plain \
KAFKA__SASL_USER=gyhlw \
KAFKA__SASL_PASSWORD=Huawei12#$% \
go run .
```

## 本地运行

```bash
# 使用默认配置运行（需先启动本地的 MQTT Broker 和 Kafka）
go run .

# 通过环境变量覆盖配置
MQTT__BROKER=tcp://192.168.122.231 \
MQTT__PORT=1883 \
MQTT__TOPICS='["sensors/batch"]' \
KAFKA__BROKERS='["10.81.151.147:21007"]' \
KAFKA__TOPIC=gyhlw_dp_test \
go run .
```

## Docker 构建

```bash
# 构建镜像
docker build -t write-to-kafka:latest .

# 运行
docker run -d \
  -e MQTT__BROKER=tcp://192.168.122.231 \
  -e MQTT__PORT=1883 \
  -e MQTT__TOPICS='["sensors/batch"]' \
  -e KAFKA__BROKERS='["10.81.151.147:21007"]' \
  -e KAFKA__TOPIC=gyhlw_dp_test \
  --name write-to-kafka \
  write-to-kafka:latest
```

## Helm 部署

### 安装 / 升级

```bash
helm upgrade --install write-to-kafka \
  oci://harbor.zkjgy.online/library/write-to-kafka \
  --namespace <namespace> \
  -f values.yaml
```

### 自定义配置

编辑 `values.yaml` 中的 `env` 字段后执行 helm upgrade：

```bash
helm upgrade --install write-to-kafka ./helm/write-to-kafka \
  --namespace <namespace> \
  -f my-values.yaml
```

### 卸载

```bash
helm uninstall write-to-kafka -n <namespace>
```

## CI/CD（GitHub Actions）

`.github/workflows/build-push.yml` 包含四个 Job，在 `main` 分支 push 或 tag 时自动触发：

| Job                  | 说明                                                   |
|----------------------|--------------------------------------------------------|
| `build-amd64`        | 构建并推送 `linux-amd64` 镜像到 Harbor                 |
| `build-arm64`        | 使用 QEMU 构建并推送 `linux-arm64` 镜像到 Harbor       |
| `manifest`           | 合并为多架构 Manifest，打 `latest` 标签                |
| `helm-package-push`  | 打包 Helm Chart 并推送到 Harbor OCI Registry           |

打 tag 时，还会额外：
- 将 amd64/arm64 镜像导出为 `.tar.gz`
- 打包 Helm Chart `.tgz`
- 创建 GitHub Release 并附上上述三个文件

**所需 GitHub Secrets：**

| Secret            | 说明                    |
|-------------------|-------------------------|
| `HARBOR_USERNAME` | Harbor 登录用户名       |
| `HARBOR_PASSWORD` | Harbor 登录密码         |

## 离线部署

从 GitHub Release 下载 `write-to-kafka-<version>-offline.tar.gz`：

```bash
# 1. 解压
tar xzf write-to-kafka-1.0.0-offline.tar.gz

# 2. 上传到目标 Harbor
cd write-to-kafka-1.0.0-offline
bash upload.sh 1.0.0 <your-harbor-url> <username> <password>

# 3. Helm 安装
helm install write-to-kafka ./write-to-kafka-1.0.0.tgz
```

## 调试工具

镜像内置以下调试命令：

```bash
# 进入容器
kubectl exec -it <pod-name> -- sh

# 测试 MQTT 连通性
telnet 192.168.122.231 1883

# 测试 Kafka 连通性
nc -zv 10.81.151.147 21007
```

## 项目文件结构

```
write-to-kafka/
├── main.go                       # 启动入口，配置加载
├── go.mod / go.sum               # Go 模块依赖
├── config/
│   └── config.go                 # 配置结构体
├── models/
│   └── message.go                # SensorValue + KafkaMessage 模型
├── mqtt/
│   └── consumer.go               # MQTT 订阅 + 消息解析 + channel 转发
├── kafka/
│   └── producer.go               # Kafka 批量写入（含 SASL 认证）
├── config.json                   # 本地测试配置文件
├── helm/
│   └── write-to-kafka/
│       ├── Chart.yaml
│       ├── values.yaml           # 环境变量配置
│       └── templates/
│           ├── _helpers.tpl
│           └── deployment.yaml
├── Dockerfile
├── .github/workflows/build-push.yml
├── .gitignore
└── README.md
```

## 网络权限（对侧 Kafka 集群）

根据需求文档，已开通以下网络权限：

| 源 IP        | 目标 IP        | 目标端口   | 用途     |
|-------------|----------------|-----------|----------|
| 10.253.3.93 | 10.81.151.147 | 21005~21024 | Kafka    |
|             | 10.81.151.148 |           |          |
|             | 10.81.151.149 |           |          |

对侧 Kafka 集群地址：`10.81.151.147:21007`（示例）
