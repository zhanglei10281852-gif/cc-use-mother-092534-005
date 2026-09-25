# 连接器最小权限会话代理

办公协作平台的连接器会话代理：把外部账号授权拆成**资源、动作、数据范围、有效时间**四个维度，
任务提交时先计算最小能力集合，扩权必须资源所有者重新确认；调用前后双重校验会话版本，
分页读取途中授权收缩时已取数据保留审计、未取页立即停止；写操作凭幂等键去重并持久化外部回执，
本地重试与崩溃恢复都不会造成重复发送。管理员可单点隔离/撤销而不切断其他合法任务，
并能查看请求范围、实际使用、拒绝原因与剩余额度。

仓库同时提供领域资料（合同、策略、事件样例）与可运行的服务实现，全部基于 Python 标准库，
持久化使用 SQLite（内存或文件），无需其他服务。

## 目录

- `domain/contract.json`：实体、四维能力、状态、事件类型和关键业务规则。
- `domain/policies.json`：可被程序读取的策略样例（P-05-01 … P-05-12）。
- `examples/events.json`：按业务发生时间排列的事件样例。
- `broker/`：会话代理服务实现。
  - `models.py`：`Capability` 四维值对象与最小集合 `minimum_set`。
  - `errors.py`：异常与拒绝原因编码（如 `scope_reduced`、`quota_exhausted`）。
  - `storage.py`：SQLite 表结构、只追加的事件日志与状态/回执/额度读写。
  - `fakes.py`：外部连接器模拟器（分页读取、写超时、服务端幂等去重）。
  - `service.py`：`Broker` 领域服务（授权、校验、分页、幂等写、对账、管理视图）。
- `tests/test_broker.py`：20 个端到端行为测试。
- `examples/walkthrough.py`：主管场景的完整走读（含文件库重启后对账）。
- `tools/validate_contract.py`：离线校验领域资料一致性。

## 核心规则如何落地

| 主管诉求 | 实现位置 |
| --- | --- |
| 四维授权，任务先算最小集合 | `submit_task` + `minimum_set`，授权恰好等于需求去重后的集合 |
| 扩权必须资源所有者重新确认 | `request_expansion` 只登记；`confirm_expansion` 校验 `owner_id` 后才生效并升版本 |
| 调用前后校验会话版本 | 每页 `_pre_check`；返回后按“该能力是否仍 granted 且在有效期内”复核（不只比对版本号） |
| 分页途中收缩：已取留档、未取停止 | 收缩当页写入 `pages_audit(delivered=0)` 但不交付，调用置 `stopped`，后续页直接拒绝 |
| 写操作幂等、回执持久化、重试不重发 | 先落 `receipts(status=pending)` 再发外部；同键返回原回执；外部服务端按键去重 |
| 服务恢复后继续对账 | 重新打开同一 SQLite 文件后 `recover()`，会话进 `reconciling`，凭原键核销回执 |
| 隔离单项能力 / 撤销单项资源 | `isolate_capability` / `revoke_capability` / `revoke_resource`，其他能力状态不变 |
| 请求范围/实际使用/拒绝原因/剩余额度 | `session_view()` 一个视图聚合返回 |

## 构建与测试

```bash
python3 -m compileall -q .
python3 -m unittest discover -s tests -v
python3 tools/validate_contract.py
```

## 端到端走读

```bash
python3 examples/walkthrough.py
```

依次演示：只授权日历 → 顺带读邮件被拒、扩权须所有者重认 → 分页第 3 页途中收缩、
已取两页留档 → 写邮件回执超时、本地重试只产生一次外部副作用 → 进程重启后待核销回执继续对账
→ 管理视图展示范围/用量/拒绝原因/额度 → 单点隔离邮件外发而不影响其他能力。

所有命令都在项目根目录执行；走读脚本使用临时目录下的 SQLite 文件，不依赖外部服务。
