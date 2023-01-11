# Telegraf

## 介绍 Telegraf

[README.md](./README.telegraf.md)

## 介绍改动

### Parser

增加 Parser 插件 OpenTSDB 和 OpenTSDB-Telnet，用于采集 OpenTSDB 的写入请求。

**OpenTSDB**

通过使用 Input 插件 http_listener_v2 并配置 `data_format` 为 `"opentsdb"`，将能够解析 OpenTSDB 格式的写入请求。

```toml
[[inputs.http_listener_v2]]
service_address = ":8080"
paths = ["/api/put"]
methods = ["POST", "PUT"]
data_format = "opentsdb"
```

**OpenTSDB-Telnet**

通过使用 Input 插件 socket_listener，并配置 `data_format` 为 opentsdbtelnet，将能够解析 OpenTSDB-Telnet 格式的写入请求。

```toml
[[inputs.socket_listener]]
service_address = "tcp://:8081"
data_format = "opentsdbtelnet"
```

### Output

增加 Output 插件 CnosDBG，用于将指标输出到 CnosDB。

```toml
[[outputs.cnosdb]]
url = "localhost:31006"
user = "user"
password = "pass"
database = "telegraf"
```

**配置介绍**

| 参数       | 说明               |
|----------|------------------|
| url      | CnosDB GRpc 服务地址 |
| user     | 用户名              |
| password | 密码               |
| database | CnosDB 数据库       |

### Input

增加配置参数 high_priority_io，用于开启端到端模式。

当设置为 true 时，写入的数据将立即发送到 Output 插件，并根据 Output 插件的返回参数来决定返回值。

```toml
[[inputs.http_listener_v2]]
service_address = ":8080"
paths = ["/api/put"]
methods = ["POST", "PUT"]
data_format = "opentsdb"
high_priority_io = true
```

以上配置与在 [Output](#output) 章节中的配置相比，增加了 `high_priority_io = true` 配置项。