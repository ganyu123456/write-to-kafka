# write-to-kafka

订阅 MQTT 主题，将传感器数据实时转发到 Kafka 的 Go 服务。支持多管道模式：不同 MQTT topic 可连接不同 broker，推送到不同 Kafka topic，并支持测点点表过滤。

## 架构

```
MQTT Broker A ──topic──┐
MQTT Broker B ──topic──┤
                       │
          ┌────────────▼──────────────┐
          │   MqttConsumer             │
          │   - 多 broker 多 topic 订阅 │
          │   - 点表过滤 + 测点名映射    │
          │   - 管道路由                │
          └────────────┬──────────────┘
                       │ Channel 传递 (含 KafkaTopic 标记)
          ┌────────────▼──────────────┐
          │   KafkaProducer            │
          │   - 按目标 topic 分发       │
          │   - SASL/Kerberos 认证     │
          └────────────┬──────────────┘
                       │
          ┌────────────▼──────────────┐
          │   Kafka                    │
          │   Topic A / Topic B / ...  │
          └───────────────────────────┘
```

## MQTT 消息格式

MQTT 客户端推送的消息为 **嵌套 JSON**，外层包含消息元信息，测点数据在 `batchData` 字段中：

```json
{
  "timestamp": 1780076839551,
  "deviceId": "sis-collect-dev-dy",
  "batchData": {
    "DDM.SIS.1DCS_10DCS_TDM_01": {"value": 0, "timestamp": 1600397002, "state": 1},
    "DDM.SIS.1DCS_10DCS_TDM_02": {"value": 26, "timestamp": 1600397414, "state": 1}
  }
}
```

### 外层字段

| 字段        | 类型   | 说明                              |
|-------------|--------|-----------------------------------|
| `timestamp` | long   | 消息发送时间，Unix 毫秒           |
| `deviceId`  | string | 设备标识                          |
| `batchData` | object | 测点数据字典，key=测点名          |

### batchData 中单个测点

| 字段        | 类型   | 说明                              |
|-------------|--------|-----------------------------------|
| `value`     | double | 测点值                            |
| `timestamp` | long   | 测点采集时间，Unix **秒**级       |
| `state`     | int    | 质量码：`1` = Good，`0` = Bad    |

## Kafka 消息格式

每条消息对应一个测点，序列化为 JSON：

```json
{
  "tag_name": "DDM.SIS.1DCS_10DCS_TDM_01",
  "device_id": "sis-collect-dev-dy",
  "value": 0,
  "timestamp": 1600397002,
  "state": 1
}
```

- **Key**: `tag_name`（确保同一测点进入同一分区，保持有序）
- **Value**: 上述 JSON

## 管道配置（PIPELINES）

服务核心配置为 `PIPELINES`，一个 JSON 数组，每条管道定义 MQTT → Kafka 的完整映射：

```json
[
  {
    "mqtt_topic": "device/sis/data",
    "kafka_topic": "gyhlw_sd03001",
    "mqtt_broker": "tcp://10.66.87.82",
    "mqtt_port": 11883,
    "mqtt_qos": 1,
    "points_file": "/etc/config/points_dongying.csv"
  }
]
```

### 管道字段

| 字段           | 必填 | 说明 |
|---------------|------|------|
| `mqtt_topic`  | 是   | 要订阅的 MQTT 主题 |
| `kafka_topic` | 是   | 推送数据的 Kafka 主题（命名规范 `gyhlw_{plant_code}`） |
| `points_file` | 否   | 点表 CSV 文件路径，用于过滤和重命名测点 |
| `mqtt_broker` | 否   | 覆盖全局 MQTT broker（为空则使用 `MQTT__BROKER`）|
| `mqtt_port`   | 否   | 覆盖全局 MQTT 端口（为 0 则使用 `MQTT__PORT`）|
| `mqtt_username`| 否  | 覆盖全局 MQTT 用户名 |
| `mqtt_password`| 否  | 覆盖全局 MQTT 密码 |
| `mqtt_client_id`| 否 | 覆盖全局 MQTT Client ID |
| `mqtt_qos`    | 否   | 覆盖全局 MQTT QoS（为 0 则使用 `MQTT__QOS`）|

每条管道可连接不同的 MQTT broker，同一 broker 下的多个 topic 共享一个 MQTT 连接。

## 点表（Points Table）

点表 CSV 用于过滤测点和映射名称，格式为两列无表头：

```
MQTT测点名,Kafka测点名
YHJZZJ.DPU1002_SH0026_BALM1_PV,YHJZZJ.DPU1002_SH0026_BALM1_PV
```

- **第 1 列**：MQTT 消息中的测点名
- **第 2 列**：写入 Kafka 时的测点名

只保留点表中存在的测点，其余丢弃。两列名称相同时仅做白名单过滤。

## 配置说明

### 环境变量

| 环境变量                               | 默认值                   | 说明                              |
|---------------------------------------|-------------------------|-----------------------------------|
| `PIPELINES`                           | -                       | 管道配置（JSON 数组，**推荐使用**） |
| `MQTT__BROKER`                        | `tcp://127.0.0.1`       | MQTT Broker 地址（管道未指定时使用）|
| `MQTT__PORT`                          | `1883`                  | MQTT 端口（管道未指定时使用）       |
| `MQTT__CLIENTID`                      | `write-to-kafka`        | MQTT 客户端 ID                     |
| `MQTT__USERNAME`                      | `""`                    | MQTT 用户名                        |
| `MQTT__PASSWORD`                      | `""`                    | MQTT 密码                          |
| `MQTT__QOS`                           | `1`                     | MQTT QoS（管道未指定时使用）        |
| `MQTT__TOPICS`                        | `["sensors/batch"]`     | 兼容模式：订阅的 MQTT 主题（PIPELINES 为空时生效）|
| `KAFKA__BROKERS`                      | `["127.0.0.1:9092"]`    | Kafka Broker 列表（JSON 数组）       |
| `KAFKA__TOPIC`                        | `sensor-data`           | 兼容模式：Kafka 目标主题（PIPELINES 为空时生效）|
| `KAFKA__SASL_MECHANISM`               | `"none"`                | SASL 机制：`none`, `plain`, `gssapi` |
| `KAFKA__SASL_USER`                    | `""`                    | SASL 用户名 / Kerberos principal  |
| `KAFKA__SASL_PASSWORD`                | `""`                    | SASL 密码 / Kerberos 密码         |
| `KAFKA__SASL_GSSAPI_AUTH_TYPE`        | `"ccache"`              | Kerberos 认证方式：`password`, `keytab`, `ccache` |
| `KAFKA__SASL_GSSAPI_REALM`            | `""`                    | Kerberos Realm                   |
| `KAFKA__SASL_GSSAPI_SERVICE_NAME`     | `"kafka"`               | Kerberos Service Name            |
| `KAFKA__SASL_GSSAPI_DOMAIN_NAME`      | `""`                    | Kerberos 域名 |
| `KAFKA__SASL_GSSAPI_KEYTAB_PATH`      | `""`                    | Keytab 文件路径 |
| `KAFKA__KRB5_CONFIG`                  | `"/etc/krb5.conf"`      | krb5.conf 配置文件路径 |

### JSON 配置文件（可选）

环境变量优先级高于 `config.json`：

```json
{
  "mqtt": {
    "broker": "tcp://127.0.0.1",
    "port": 1883,
    "client_id": "write-to-kafka",
    "qos": 1
  },
  "kafka": {
    "brokers": ["10.81.151.147:21007"],
    "sasl": {
      "mechanism": "gssapi",
      "username": "gyhlw",
      "password": "Huawei12#$%",
      "gssapi_auth_type": "password",
      "gssapi_realm": "HADOOP.COM",
      "gssapi_service_name": "kafka",
      "krb5_config_path": "/etc/krb5.conf"
    }
  },
  "pipelines": [
    {
      "mqtt_topic": "device/sis/data",
      "kafka_topic": "gyhlw_sd03001",
      "points_file": "docs/points.csv"
    }
  ]
}
```

## Kafka Topic 命名规范

遵循 `gyhlw_{plant_code}` 格式，全小写，下划线连接：

| 电厂 | 编码 | Topic 名称 |
|------|------|-----------|
| 黄岛电厂 | sd02001 | gyhlw_sd02001 |
| 东营电厂 | sd03001 | gyhlw_sd03001 |
| 郓城电厂 | sd04001 | gyhlw_sd04001 |
| 鲁北电厂 | sd05001 | gyhlw_sd05001 |
| 滨州电厂 | sd06001 | gyhlw_sd06001 |
| 临清电厂 | sd07001 | gyhlw_sd07001 |

## Kerberos (GSSAPI) 认证

### 方式一：用户名密码方式（推荐）

程序自动调用 Kerberos KDC 获取票据：

```bash
KAFKA__SASL_MECHANISM=gssapi \
KAFKA__SASL_USER=gyhlw \
KAFKA__SASL_PASSWORD=Huawei12#$% \
KAFKA__SASL_GSSAPI_AUTH_TYPE=password \
KAFKA__SASL_GSSAPI_REALM=HADOOP.COM \
KAFKA__KRB5_CONFIG=/etc/krb5.conf
```

### 方式二：缓存票据方式（需提前 kinit）

```bash
kinit gyhlw
KAFKA__SASL_MECHANISM=gssapi \
KAFKA__SASL_USER=gyhlw \
KAFKA__SASL_GSSAPI_AUTH_TYPE=ccache \
go run .
```

### 方式三：Keytab 文件方式

```bash
KAFKA__SASL_MECHANISM=gssapi \
KAFKA__SASL_USER=gyhlw \
KAFKA__SASL_GSSAPI_AUTH_TYPE=keytab \
KAFKA__SASL_GSSAPI_KEYTAB_PATH=/etc/gyhlw.keytab \
go run .
```

### SASL/PLAIN 认证

```bash
KAFKA__SASL_MECHANISM=plain \
KAFKA__SASL_USER=gyhlw \
KAFKA__SASL_PASSWORD=Huawei12#$% \
go run .
```

## 本地运行

```bash
# 单管道
PIPELINES='[{"mqtt_topic":"device/sis/data","kafka_topic":"gyhlw_sd03001","mqtt_broker":"tcp://10.66.87.82","mqtt_port":11883}]' \
KAFKA__BROKERS='["10.81.151.147:21007"]' \
KAFKA__SASL_MECHANISM=gssapi \
KAFKA__SASL_USER=gyhlw \
KAFKA__SASL_PASSWORD=Huawei12#$% \
go run .

# 多管道（多电厂）
PIPELINES='[
  {"mqtt_topic":"device/sis/huangdao","kafka_topic":"gyhlw_sd02001","mqtt_broker":"tcp://10.0.1.1","mqtt_port":1883,"points_file":"docs/points_huangdao.csv"},
  {"mqtt_topic":"device/sis/dongying","kafka_topic":"gyhlw_sd03001","mqtt_broker":"tcp://10.0.2.1","mqtt_port":1883,"points_file":"docs/points_dongying.csv"}
]' go run .
```

## Docker 构建

```bash
docker build -t write-to-kafka:latest .

docker run -d \
  -e PIPELINES='[{"mqtt_topic":"device/sis/data","kafka_topic":"gyhlw_sd03001","mqtt_broker":"tcp://10.66.87.82","mqtt_port":11883,"points_file":"/etc/config/points.csv"}]' \
  -e KAFKA__BROKERS='["10.81.151.147:21007"]' \
  -v /data/points.csv:/etc/config/points.csv \
  --name write-to-kafka \
  write-to-kafka:latest
```

## Helm 部署

### 安装 / 升级

```bash
helm upgrade --install write-to-kafka ./helm/write-to-kafka --namespace <namespace>
```

### 自定义配置

编辑 `values.yaml` 中的 `env.PIPELINES` 和 `pointsFiles`：

```yaml
env:
  PIPELINES: '[{"mqtt_topic":"device/sis/data","kafka_topic":"gyhlw_sd03001","mqtt_broker":"tcp://10.66.87.82","mqtt_port":11883,"mqtt_qos":1,"points_file":"/etc/config/points_dongying.csv"}]'

pointsFiles:
  - name: points-dongying
    hostPath: /data/write-ro-kafka/points/points_dongying.csv
    mountPath: /etc/config/points_dongying.csv
```

```bash
helm upgrade --install write-to-kafka ./helm/write-to-kafka --namespace <namespace> -f my-values.yaml
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
tar xzf write-to-kafka-1.0.0-offline.tar.gz
cd write-to-kafka-1.0.0-offline
bash upload.sh 1.0.0 <your-harbor-url> <username> <password>
helm install write-to-kafka ./write-to-kafka-1.0.0.tgz
```

## 调试工具

```bash
kubectl exec -it <pod-name> -- sh

# 测试 MQTT 连通性
telnet 10.66.87.82 11883

# 测试 Kafka 连通性
nc -zv 10.81.151.147 21007
```

## 项目文件结构

```
write-to-kafka/
├── main.go                       # 启动入口，配置加载，管道构建
├── go.mod / go.sum               # Go 模块依赖
├── config/
│   └── config.go                 # 配置结构体（含 PipelineConfig）
├── models/
│   ├── message.go                # SensorValue + KafkaMessage 模型
│   └── points.go                 # 点表加载、过滤、重命名
├── mqtt/
│   └── consumer.go               # MQTT 多 broker 订阅 + 管道路由
├── kafka/
│   └── producer.go               # Kafka 写入（含 SASL 认证）
├── config.json                   # 本地测试配置文件
├── docs/
│   ├── points.csv                # 点表示例
│   └── 关于kafka-topic命名规范.md
├── helm/
│   └── write-to-kafka/
│       ├── Chart.yaml
│       ├── values.yaml           # Helm 配置（PIPELINES + pointsFiles）
│       └── templates/
│           ├── _helpers.tpl
│           └── deployment.yaml
├── Dockerfile
├── .github/workflows/build-push.yml
├── .gitignore
└── README.md
```

## 网络权限

已开通以下网络权限：

| 源 IP        | 目标 IP        | 目标端口   | 用途     |
|-------------|----------------|-----------|----------|
| 10.253.3.93 | 10.81.151.147 | 21005~21024 | Kafka    |
|             | 10.81.151.148 |           |          |
|             | 10.81.151.149 |           |          |
