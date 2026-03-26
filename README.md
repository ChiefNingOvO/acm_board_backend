# ACM Board Backend

一个面向 ACM/算法竞赛榜单展示场景的轻量级后端服务。

它会定时从 PTA/Pintia 拉取最新提交记录，做本地去重、首刀统计、用户信息映射，并把提交事件和公告事件投递到 Kafka；同时提供排行榜、首刀列表、统计信息和管理接口，方便前端展示和运维。

## 功能特性

- 每隔固定时间自动拉取 PTA 最新提交
- 使用 SQLite 持久化去重状态和运行数据
- 将提交事件、公告事件发送到 Kafka
- 维护题目首刀记录，保存首刀人和最早提交时间
- 提供排行榜、首刀列表、提交统计等查询接口
- 提供简易管理页，可发送公告、清空数据库
- 支持 `.env` 配置化部署，适合服务器直接上线

## 技术栈

- Python 3.11+
- FastAPI
- SQLite
- kafka-python
- requests
- uvicorn

## 项目结构

```text
.
├── app.py                              # FastAPI 入口、业务逻辑、数据库初始化
├── get_data_utils.py                   # PTA/Pintia 数据抓取工具
├── clear_db.py                         # 清库脚本
├── migrate_to_sqlite.py                # 历史 CSV 迁移脚本
├── admin.html                          # 简易管理页
├── .env.example                        # 环境变量模板
└── deploy/systemd/acm-board-backend.service
                                       # systemd 部署示例
```

## 工作流程

1. 服务启动后初始化 SQLite 和内存状态
2. 后台轮询器按 `SUBMISSION_POLL_INTERVAL_SECONDS` 周期抓取 PTA 提交
3. 对提交记录按 `submission_id` 去重
4. 处理用户信息、通过记录、首刀记录
5. 将新事件发送到 Kafka
6. 前端按需查询排行榜、首刀、统计信息

## 快速开始

### 1. 安装依赖

```bash
pip install -e .
```

如果你不用 editable 安装，也可以直接：

```bash
pip install fastapi kafka-python requests "uvicorn[standard]" python-dotenv
```

### 2. 配置环境变量

复制配置模板：

```bash
cp .env.example .env
```

然后按实际环境填写 `.env`，尤其是：

- PTA 相关 URL、Cookie、请求头
- Kafka 地址和 Topic
- 服务监听地址和端口
- 首刀题目列表
- SQLite 数据库文件名

### 3. 启动服务

```bash
uvicorn app:app --host 0.0.0.0 --port 8090
```

如果你已经在 `.env` 中配置了 `APP_HOST` 和 `APP_PORT`，也可以直接：

```bash
python app.py
```

### 4. 访问管理页

默认管理页地址：

```text
http://127.0.0.1:8090/admin
```

## 环境变量说明

下面列出最关键的配置项，完整模板见 [`.env.example`](.env.example)。

### PTA 配置

- `PTA_SUBMIT_URL`：提交记录接口
- `PTA_PROBLEM_URL`：题目标签接口
- `PTA_RANK_URL`：排行榜接口
- `PTA_REFERER_BASE_URL`：PTA 页面基础地址
- `PTA_COOKIE`：登录态 Cookie
- `PTA_X_LOLLIPOP`、`PTA_X_MARSHMALLOW`：请求校验头

### Kafka 配置

- `KAFKA_BOOTSTRAP_SERVERS`：Kafka broker 地址，多个用逗号分隔
- `KAFKA_SUBMISSION_TOPIC`：提交事件 Topic
- `KAFKA_BROADCAST_TOPIC`：公告事件 Topic
- `KAFKA_PRODUCER_ACKS`：Kafka acks 配置
- `KAFKA_PRODUCER_RETRIES`：Kafka 重试次数
- `KAFKA_SEND_TIMEOUT_SECONDS`：发送超时时间

### 服务配置

- `APP_TITLE`：FastAPI 标题
- `APP_SOURCE_NAME`：Kafka 消息来源标识
- `APP_HOST`：服务监听地址
- `APP_PORT`：服务监听端口
- `APP_RELOAD`：是否开启热重载
- `ADMIN_HTML_FILE`：管理页文件名

### 轮询与首刀配置

- `SUBMISSION_POLL_INTERVAL_SECONDS`：提交抓取轮询间隔，默认可设为 `5`
- `FIRST_BLOOD_PROBLEM_IDS`：参与首刀统计的题号列表
- `FIRST_BLOOD_EMPTY_VALUE`：未产生首刀时的占位值，通常为 `-1`

### 数据库与迁移配置

- `DB_FILE`：SQLite 数据库文件路径
- `MIGRATION_*`：历史 CSV 迁移脚本使用的文件名

## 对外接口

### `GET /api/get_rank_list`

返回当前排行榜数据。

示例响应：

```json
[
  {
    "rank": 1,
    "name": "张三"
  },
  {
    "rank": 2,
    "name": "李四"
  }
]
```

### `GET /api/get_first_blood`

返回所有题目的首刀信息。

示例响应：

```json
[
  {
    "problem_id": "A",
    "user_id": "张三 20230001",
    "judge_time": "2026-03-26T10:15:30Z"
  },
  {
    "problem_id": "B",
    "user_id": "-1",
    "judge_time": null
  }
]
```

### `GET /api/get_submission_stats`

返回当前总提交数和累计 `ACCEPTED` 提交次数。

示例响应：

```json
{
  "total_submissions": 1234,
  "accepted_submissions": 567
}
```

### `GET /api/send_message`

发送公告事件到 Kafka。

请求参数：

- `content`：公告内容
- `duration`：前端展示时长，单位毫秒

示例响应：

```json
{
  "status": "success",
  "topic": "acm.board.submission.events",
  "event_key": "broadcast:1711440000000"
}
```

### `POST /api/clear_db`

清空数据库并重新初始化运行状态。

### `GET /admin`

返回简易管理页面。

## Kafka 消息格式

提交事件和公告事件都会以统一结构发送到 Kafka：

```json
{
  "event_type": "message",
  "source": "acm_board_backend",
  "payload": {
    "type": "message",
    "user_id": "20230001",
    "problem_id": "A",
    "name": "张三",
    "label": "A",
    "status": "ACCEPTED",
    "judge_time": "2026-03-26T10:15:30Z",
    "event_key": "submission:20230001:A:ACCEPTED:2026-03-26T10:15:30Z"
  }
}
```

公告事件示例：

```json
{
  "event_type": "broadcast",
  "source": "acm_board_backend",
  "payload": {
    "type": "broadcast",
    "content": "比赛开始",
    "duration": 5000,
    "created_at": 1711440000000,
    "event_key": "broadcast:1711440000000"
  }
}
```

## 部署建议

### 服务器配置

这个项目本身比较轻量，常规 `2C2G` 云服务器通常可以部署和运行，但前提是：

- PTA 接口可访问
- Kafka broker 可访问
- SQLite 文件所在磁盘可写
- 日志和数据库文件有基本备份策略

### 使用 systemd 部署

仓库中提供了示例文件：

- [`deploy/systemd/acm-board-backend.service`](deploy/systemd/acm-board-backend.service)

常见部署流程：

1. 将项目上传到服务器，例如 `/opt/acm_board_backend2`
2. 创建虚拟环境并安装依赖
3. 准备 `.env`
4. 修改 `systemd` 文件中的项目路径和 Python 路径
5. 启动服务并设置开机自启

## 数据存储说明

SQLite 中会保存以下数据：

- 已处理提交 ID
- 用户已通过题目记录
- 所有 `ACCEPTED` 提交 ID
- 首刀记录
- 题目标签
- 用户信息
- 学号映射信息

这样即使服务重启，核心状态也可以恢复。

## 注意事项

- `.env` 中包含 PTA 和 Kafka 的敏感配置，上传 GitHub 前不要提交真实 `.env`
- 建议将数据库文件和日志文件加入忽略列表
- 如果 Kafka 报 `NoBrokersAvailable`，优先检查网络连通性和 `advertised.listeners`
- 如果 PTA 接口请求失败，优先检查 Cookie 和请求头是否过期

## 后续可改进方向

- 补充测试用例和 CI
- 将业务逻辑从 `app.py` 中拆分为模块
- 增加结构化日志和监控
- 为排行榜和公告消费端补充更明确的协议文档

## License

如果你准备开源，建议补一个明确的许可证，例如 `MIT`。
