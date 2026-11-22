# 教室 IPTV 换订转发器

按**源 IP** 把上联 UDP 组播流转发到订阅该源的教室端口。换订（整批更换教室
订阅表）时保证教室一直有信号：旧源持续转发到新源实际可用，再统一切换，最后
把上联放行范围收窄到新表。

- 转发器：FastAPI + asyncio 单收单发套接字，Swagger 在 `/docs`
- 上联模拟器：从多个源 IP 向同一组播组发包，可整组放行或精确放行最多两个源，
  可注入「拒绝改表」与「改成后丢回执」故障
- 编排：Docker Compose（uplink / relay / classroom 观察器）

## 快速开始

```bash
docker compose up --build
# Swagger
open http://127.0.0.1:8000/docs
# 观察各教室端口正在收哪个源
docker compose logs -f classroom
```

Compose 使用 `network_mode: host`：组播不会跨默认 Docker bridge 转发，模拟器也
需要 `127.0.0.11/12/13` 这些回环别名来模拟不同源 IP。生产网络里上联口与教室
口是分开的，只有转发器需要组播权限。

不用 Docker 时（本仓库的实测方式）：

```bash
pip install -r requirements.txt
SIM_GROUP=239.255.42.10 SIM_PORT=5004 SIM_INTERFACE=127.0.0.1 \
SIM_SOURCES=127.0.0.11,127.0.0.12,127.0.0.13 SIM_INITIAL_MODE=allow \
SIM_INITIAL_SOURCES=127.0.0.11 python3 -m uvicorn simulator.main:app --port 9100 &

RELAY_UPLINK_GROUP=239.255.42.10 RELAY_UPLINK_PORT=5004 \
RELAY_UPLINK_INTERFACE=127.0.0.1 RELAY_MULTICAST_GROUP=239.255.42.10 \
RELAY_MULTICAST_INTERFACE=127.0.0.1 RELAY_UPLINK_URL=http://127.0.0.1:9100 \
RELAY_INITIAL_TABLE='16001=127.0.0.11;16002=127.0.0.11;16003=127.0.0.11' \
python3 -m uvicorn app.main:app --port 8000 &

python3 -m scripts.classroom_watcher       # 观察教室端口
python3 -m scripts.demo_switch             # 用 API 演示全部场景
```

`scripts/demo_switch.py` 依次演示：正常换订、准备失败、收窄失败+暂停换订+
重试成功、丢回执仍判成功。同样的调用也可以直接在 Swagger 里点。

## 换订协议

1. **准备**：把上联放行范围扩大到「旧表源 ∪ 新表源」。教室表不动。
   - 集合 ≤ 2 个源：精确放行；> 2 个源：模拟器只支持整组或最多两个源，
     因此整组放行（`mode=all`）。
   - 失败（被拒绝 / 实际未放行）→ **按旧表照旧发包**，并尝试把上联恢复到旧表
     所需的源；返回 `failed / prepare_failed`。
2. **切换前复核**：重新读取模拟器**实际生效**的过滤；只有确认覆盖两边源才切换。
3. **统一切换**：一次性替换不可变订阅表快照（数据面原子读取，不阻塞）。
4. **收窄**：把上联收窄到完全等于新表源。
   - 失败 → 新表继续转发（教室不断流），报告上联**实际**放行范围，
     `gate.blocked=true`，**暂停新的换订**，直到重试成功。

关键约束：**每个版本的订阅表最多两个源**；一次换订涉及的两个版本合计也最多
两个源（由 `SubscriptionTable` 与迁移请求校验）。需要三个源时，协议用「整组
放行」的短暂窗口完成过渡，收窄后仍回到每版 ≤2 源。

### 丢回执的判定

每次改表后都重新 `GET /sim/filter` 读取实际状态；**不信任回执本身**：

- 回执丢失 + 实际已生效 → 判成功（`ack=lost, verified_by_actual=true`）
- 回执丢失 + 实际未变 → 判失败
- 回执成功 + 实际不符 → 判失败并复核，不标记完成
- 完全读不到实际状态时，只有明确 ACK 才接受，并标记 `verified_by_actual=false`

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/healthz` | 健康检查 |
| GET | `/state` | 当前表、门禁、事件 |
| GET | `/stats` | 收包/转发/按源/按教室端口统计 |
| POST | `/migrations` | 整批换订：`{"table": {"16001": "127.0.0.12"}, "note": "..."}` |
| GET | `/migrations` | 换订历史 |
| GET | `/migrations/{id}` | 单次换订详情（含每步实际过滤） |
| POST | `/migrations/{id}/retry-narrow` | 重试收窄；成功才恢复换订 |
| POST | `/demo/reset` | 演示用：重置教室表和模拟器 |
| GET/POST | `/demo/uplink/filter` | 查看/直接设置模拟器过滤 |
| POST | `/demo/uplink/faults` | 注入故障，见下 |

故障注入（模拟器 `POST /sim/faults`，转发器有同名代理）：

```json
{"refuse_changes": 1}
{"drop_receipts": 2}
{"refuse_changes": 1, "refuse_only_sources": ["127.0.0.12"]}
{"drop_receipts": 1, "drop_only_sources": ["127.0.0.12"]}
{"reset": true}
```

`*_only_sources` 只对精确放行到该源集合的请求生效，用来单独命中「收窄」这一步。

## 配置

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `RELAY_UPLINK_GROUP` / `RELAY_UPLINK_PORT` | `239.255.42.10` / `5004` | 上联组播地址 |
| `RELAY_UPLINK_INTERFACE` | `127.0.0.1` | 收流网卡/IP |
| `RELAY_MULTICAST_GROUP` | `239.255.42.10` | 教室侧输出组（默认沿用原组） |
| `RELAY_MULTICAST_INTERFACE` | `127.0.0.1` | 发流网卡/IP |
| `RELAY_CLASSROOM_PORT_MIN/MAX` | `16000` / `16999` | 允许的教室端口范围 |
| `RELAY_UPLINK_URL` | `http://127.0.0.1:9100` | 模拟器地址；置空=不管理过滤 |
| `RELAY_UPLINK_REQUIRED` | `false` | 启动时模拟器不可达则失败 |
| `RELAY_INITIAL_TABLE` | 空 | `16001=127.0.0.11;16002=127.0.0.11` |
| `RELAY_ALLOWED_SOURCES` | 空 | 可选的本地源白名单（逗号分隔） |

模拟器侧：`SIM_GROUP`、`SIM_PORT`、`SIM_INTERFACE`、`SIM_SOURCES`、
`SIM_INTERVAL`、`SIM_INITIAL_MODE/SOURCES`。

## 测试

```bash
pip install -r requirements-dev.txt
python3 -m pytest -q
```

- `tests/test_table.py`、`tests/test_relay.py`：每版 ≤2 源、按源分发、回环自激抑制
- `tests/test_migrations.py`：以 mock 模拟器覆盖成功/准备失败/收窄失败/重试/丢回执
- `tests/test_e2e_udp.py`、`tests/test_e2e_signal_continuity.py`：真实进程 + 真实
  UDP 套接字，验证教室端口实际收到的源、切换无断流、丢回执后仍完成

## 已知限制

- 订阅表保存在进程内存里，重启回到 `RELAY_INITIAL_TABLE`；多副本之间不同步。
- 「暂停换订」只在单个 relay 实例内生效。
- 上联过滤由模拟器的 HTTP 接口管理；真实上联若没有可读回的过滤接口，只能依
  赖 ACK，此时 `verified_by_actual=false`，丢回执会被当作失败。
- 组播回环可能让转发器收到自己发出的包：仅当教室组与上联组相同且端口范围覆盖
  上联端口时按负载内容做短期去重（`EchoGuard`）；这不是通用去重机制。
- Compose 依赖 host 网络与 Linux 回环别名，macOS/Windows 上无法直接演示。
