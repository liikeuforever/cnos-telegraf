# Cnos-Telegraf

CnosDB-Telegraf 基于 Telegraf 进行开发，增加了一些功能与插件。

## 原版 Telegraf 文档

[README.md](./README.telegraf.md)

## Cnos-Telegraf 的改动说明

### Parser Plugin

增加 Parser 插件 OpenTSDB 和 OpenTSDB-Telnet，用于采集 OpenTSDB 的写入请求。

- **OpenTSDB**

  通过使用 Input 插件 http_listener_v2 并配置 `data_format` 为 `"opentsdb"`，将能够解析 OpenTSDB 格式的写入请求。

- Linux kernel version 2.6.23 or later
- Windows 7 or later
- FreeBSD 11.2 or later
- MacOS 10.11 El Capitan or later

[minimum requirements]: https://github.com/golang/go/wiki/MinimumRequirements#minimum-requirements

## Obtaining Telegraf

View the [changelog](/CHANGELOG.md) for the latest updates and changes by version.

### Binary Downloads

Binary downloads are available from the [InfluxData downloads](https://www.influxdata.com/downloads)
page or from each [GitHub Releases](https://github.com/influxdata/telegraf/releases) page.

### Package Repository

InfluxData also provides a package repo that contains both DEB and RPM downloads.

For deb-based platforms (e.g. Ubuntu and Debian) run the following to add the
repo key and setup a new sources.list entry:

```shell
# influxdata-archive_compat.key GPG fingerprint:
#     9D53 9D90 D332 8DC7 D6C8 D3B9 D8FF 8E1F 7DF8 B07E
wget -q https://repos.influxdata.com/influxdata-archive_compat.key
echo '393e8779c89ac8d958f81f942f9ad7fb82a25e133faddaf92e15b16e6ac9ce4c influxdata-archive_compat.key' | sha256sum -c && cat influxdata-archive_compat.key | gpg --dearmor | sudo tee /etc/apt/trusted.gpg.d/influxdata-archive_compat.gpg > /dev/null
echo 'deb [signed-by=/etc/apt/trusted.gpg.d/influxdata-archive_compat.gpg] https://repos.influxdata.com/debian stable main' | sudo tee /etc/apt/sources.list.d/influxdata.list
sudo apt-get update && sudo apt-get install telegraf
```toml
[[inputs.http_listener_v2]]
service_address = ":8080"
paths = ["/api/put"]
methods = ["POST", "PUT"]
data_format = "opentsdb"
```
   ```toml
   [[inputs.http_listener_v2]]
   service_address = ":8080"
   paths = ["/api/put"]
   methods = ["POST", "PUT"]
   data_format = "opentsdb"
   ```

- **OpenTSDB-Telnet**

```shell
# influxdata-archive_compat.key GPG fingerprint:
#     9D53 9D90 D332 8DC7 D6C8 D3B9 D8FF 8E1F 7DF8 B07E
cat <<EOF | sudo tee /etc/yum.repos.d/influxdata.repo
[influxdata]
name = InfluxData Repository - Stable
baseurl = https://repos.influxdata.com/stable/\$basearch/main
enabled = 1
gpgcheck = 1
gpgkey = https://repos.influxdata.com/influxdata-archive_compat.key
EOF
sudo yum install telegraf
通过使用 Input 插件 socket_listener，并配置 `data_format` 为 opentsdbtelnet，将能够解析 OpenTSDB-Telnet 格式的写入请求。
通过使用 Input 插件 socket_listener，并配置 `data_format` 为 `"opentsdbtelnet"`，将能够解析 OpenTSDB-Telnet 格式的写入请求。
  通过使用 Input 插件 socket_listener，并配置 `data_format` 为 `"opentsdbtelnet"`，将能够解析 OpenTSDB-Telnet 格式的写入请求。

   ```toml
   [[inputs.socket_listener]]
   service_address = "tcp://:8081"
   data_format = "opentsdbtelnet"
   ```

### Output Plugin

Telegraf requires Go version 1.20 or newer and the Makefile requires GNU make.

On Windows, the makefile requires the use of a bash terminal to support all makefile targets.
An easy option to get bash for windows is using the version that comes with [git for windows](https://gitforwindows.org/).

1. [Install Go](https://golang.org/doc/install)
2. Clone the Telegraf repository:

   ```shell
   git clone https://github.com/influxdata/telegraf.git
   ```

3. Run `make build` from the source directory

   ```shell
   cd telegraf
   make build
   ```

### Nightly Builds

[Nightly](/docs/NIGHTLIES.md) builds are available, generated from the master branch.

### 3rd Party Builds

Builds for other platforms or package formats are provided by members of theTelegraf community.
These packages are not built, tested, or supported by the Telegraf project or InfluxData. Please
get in touch with the package author if support is needed:

- [Ansible Role](https://github.com/rossmcdonald/telegraf)
- [Chocolatey](https://chocolatey.org/packages/telegraf) by [ripclawffb](https://chocolatey.org/profiles/ripclawffb)
- [Scoop](https://github.com/ScoopInstaller/Main/blob/master/bucket/telegraf.json)
- [Snap](https://snapcraft.io/telegraf) by Laurent Sesquès (sajoupa)
- [Homebrew](https://formulae.brew.sh/formula/telegraf#default)

## Getting Started

See usage with:

```shell
telegraf --help
增加 Output 插件 CnosDBG，用于将指标输出到 CnosDB。
增加 Output 插件 CnosDB，用于将指标输出到 CnosDB。

```toml
[[outputs.cnosdb]]
url = "localhost:31006"
user = "user"
password = "pass"
database = "telegraf"
```

- **配置介绍**

| 参数       | 说明               |
|----------|------------------|
| url      | CnosDB GRpc 服务地址 |
| user     | 用户名              |
| password | 密码               |
| database | CnosDB 数据库       |

### Input Plugin

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

## 构建

1. [安装 Go](https://golang.org/doc/install) >=1.18 (推荐 1.18.0 版本)
2. 从 Github 克隆仓库:

   ```shell
   git clone https://github.com/cnosdb/cnos-telegraf.git
   ```

3. 在仓库目录下执行 `make build`

   ```shell
   cd cnos-telegraf
   make build
   ```

## 启动

执行以下指令，查看用例:

```shell
telegraf --help
```

### 生成一份标准的 telegraf 配置文件

```shell
telegraf config > telegraf.conf
```

### 生成一份 telegraf 配置文件，仅包含 cpu 指标采集 & influxdb 输出两个插件

```shell
telegraf config --section-filter agent:inputs:outputs --input-filter cpu --output-filter influxdb
```

### 运行 telegraf 但是将采集指标输出到标准输出

```shell
telegraf --config telegraf.conf --test
```

### 运行 telegraf 并通过配置文件来管理加载的插件

```shell
telegraf --config telegraf.conf
```

### 运行 telegraf，仅加载 cpu & memory 指标采集，和 influxdb 输出插件

```shell
telegraf --config telegraf.conf --input-filter cpu:mem --output-filter influxdb
```
