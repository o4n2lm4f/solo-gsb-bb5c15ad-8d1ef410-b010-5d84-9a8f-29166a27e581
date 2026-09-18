# Webhook 事件接收与重放 API

基于 FastAPI + SQLite 的 Webhook 事件接收服务:按 `event_id` 幂等接收、记录首次接收顺序,支持时间范围分页查询与批量重放。

## 启动

```bash
docker compose up --build
```

服务监听 **8000** 端口(SQLite 数据持久化在 named volume `webhook-data`)。

不用 compose 也可以:

```bash
docker build -t webhook-replay . && docker run -p 8000:8000 webhook-replay
```

本地开发:`pip install -r requirements.txt && uvicorn app.main:app --port 8000`

## 示例 curl

### 1. 接收事件(幂等)

```bash
curl -X POST http://localhost:8000/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"evt-1","occurred_at":"2026-09-18T09:00:00Z","payload":{"a":1}}'
# 首次: 201 {"status":"created","event_id":"evt-1"}
# 重复: 200 {"status":"duplicate","event_id":"evt-1"}  —— 不产生重复记录
```

### 2. 按时间范围分页查询

```bash
curl 'http://localhost:8000/events?from=2026-09-18T09:30:00Z&to=2026-09-18T11:30:00Z&page=1&page_size=50'
```

返回 `total / page / page_size / items`,每条含 `seq`(首次接收顺序号)、`event_id`、`occurred_at`、`payload`、`received_at`。`from`/`to` 均可省略。

### 3. 批量重放

```bash
curl -X POST http://localhost:8000/replay \
  -H 'Content-Type: application/json' \
  -d '{"event_ids":["evt-3","evt-1","evt-404"]}'
```

```json
{
  "replayed": 2,
  "skipped": 1,
  "results": [
    {"event_id": "evt-1", "status": "replayed", "event": {"seq": 1, "...": "..."}},
    {"event_id": "evt-3", "status": "replayed", "event": {"seq": 2, "...": "..."}},
    {"event_id": "evt-404", "status": "skipped", "reason": "not_found"}
  ]
}
```

命中的事件**按首次接收顺序(`seq`)输出**,与请求顺序无关;未命中的标记 `skipped`;请求中重复的 `event_id` 只输出一次。

### 4. 完整性校验

```bash
curl http://localhost:8000/integrity
```

```json
{"verified": true, "checked": 3, "first_break": null}
```

从最早记录开始逐条校验哈希链:**序号连续性**(相邻 `seq` 逐一递增)、**前后哈希衔接**(每条 `prev_hash` 等于上一条的 `hash`,首条 `prev_hash` 为空串)、**事件字段**(按固定 JSON 规范重算哈希比对)。空库返回 `verified=true`、`checked=0`。

发现篡改时返回首个断点的 `seq`、`event_id` 和原因(`seq_gap` / `prev_hash_mismatch` / `hash_mismatch`),`checked` 为断点前已通过校验的记录数:

```json
{"verified": false, "checked": 1, "first_break": {"seq": 2, "event_id": "evt-2", "reason": "hash_mismatch"}}
```

## 哈希链规范

每条记录写入时在同一事务内计算并存储两个字段:

- `prev_hash`:上一条记录的 `hash`(首条为空串 `""`)
- `hash`:对以下文档做**固定 JSON 规范化**(键排序、紧凑分隔符 `,`/`:`、非 ASCII 不转义)后的 SHA-256 十六进制:

```json
{"seq":1,"event_id":"evt-1","occurred_at":"...","payload":{...},"received_at":"...","prev_hash":""}
```

重复 `event_id` 的投递走幂等返回,不会写入记录,链保持不变。任何对历史记录的字段篡改、删除或重排都会被 `/integrity` 在首个断点处检出。

**旧库迁移**:启动时自动为已有 SQLite 数据补加 `prev_hash`/`hash` 列并按首次接收顺序回填整条链,无需手工操作;迁移可重复执行(幂等)。

## 设计要点

- **幂等**:`event_id` 上有 `UNIQUE` 约束,重复插入触发 `IntegrityError` 后返回 200 `duplicate`,并发下也不会产生重复记录。
- **接收顺序**:自增主键 `id` 即首次接收顺序,重放按它排序。
- **完整性哈希链**:每条记录引用上一条哈希,写入与链字段更新在同一事务完成(`BEGIN IMMEDIATE` 保证并发下链头一致);`GET /integrity` 全链校验。
- **时间比较**:`occurred_at` 归一化为 UTC ISO8601 字符串存储,范围查询走字典序比较,并建有索引。
- **配置**:数据库路径由环境变量 `DB_PATH` 控制(容器内默认 `/data/events.db`)。

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/events` | 接收单个事件(幂等) |
| GET | `/events` | 时间范围分页查询(`from`/`to`/`page`/`page_size`) |
| POST | `/replay` | 批量重放,按首次接收顺序输出 |
| GET | `/integrity` | 校验完整性哈希链,返回 `verified`/`checked`/`first_break` |
| GET | `/health` | 健康检查 |

交互式文档:启动后访问 `http://localhost:8000/docs`。
