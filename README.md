# 连接器最小权限会话代理

连接器能力授权、资源投影、审批会话与调用回执的完整参考实现。外部办公平台
（日历、邮件、云盘、通讯录）的授权不再一次性打包，而是拆成四个维度：

- **资源（resource）**：calendar / mail / drive / contacts；
- **动作（action）**：read / write；
- **数据范围（data_scope）**：按资源属性做集合匹配，例如只允许 `kind=event`；
- **有效时间（valid_until）**：带时区的截止时刻，到期自动失效。

## 核心保证

1. **最小能力集合**：任务提交时由需求合并出最小授权；所有者确认时多给
   （`grant_not_minimal`）或少给（`capability_missing`）都会被拒绝。
2. **扩权必须重新确认**：任务追加需求时，任何扩大（新资源/动作/数据类别/
   更晚有效期）进入待确认清单；未由资源所有者重新确认前，相关调用以
   `resource_owner_reconfirm_required` 拒绝。
3. **调用前后双校验**：每次调用都检查调用方持有的会话版本、能力状态、
   数据范围与有效期；收缩只允许变小，借收缩扩大范围同样被拒。
4. **分页收缩安全**：分页读取途中授权收缩时，连接器已返回的该页数据只写入
   审计（`page.retained`），不交给任务；尚未领取的页立即停止，不再请求。
5. **写操作幂等**：幂等键在“租户 + 连接器”范围内唯一；先落
   `write.requested` 预留点再调用外部。结果未知时禁止重发，只能通过
   `reconcile_writes` 向连接器核对并核销回执，本地重试不会造成重复发送；
   同键不同体直接拒绝（`DuplicateDeliveryError`）。
6. **细粒度处置**：管理员可隔离单个能力（`isolate_capability`）或撤销单项
   资源（`revoke_capability`），同会话其他合法任务继续运行。
7. **可观测**：管理员接口给出请求范围、实际使用量、拒绝原因、剩余额度、
   待核销写操作与留存审计。
8. **崩溃恢复**：所有状态变更以事件追加到 SQLite；服务重启后重放事件流，
   进行中的会话与待核销回执自动恢复并继续对账。

## 目录

- `domain/contract.json`：实体、状态、事件类型和关键业务规则。
- `domain/policies.json`：可被程序读取的策略样例。
- `examples/events.json`：按业务发生时间排列的事件样例。
- `examples/walkthrough.py`：完整业务故事线的可执行演练。
- `broker/`：会话代理参考实现（仅依赖 Python 标准库）。
  - `catalog.py`：四维能力模型与最小集合计算。
  - `service.py`：会话状态机、授权校验、分页读取器、写操作与对账、管理员接口。
  - `storage.py`：SQLite 追加式事件存储。
  - `connectors.py`：连接器接口与离线测试桩（含故障/响应丢失注入）。
  - `errors.py`：拒绝原因枚举与异常。
- `tools/validate_contract.py`：使用 Python 标准库和 SQLite 内存表验证资料一致性。
- `tests/`：领域资料校验与 25 项端到端行为测试。

## 构建

```bash
python3 -m compileall -q .
```

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 业务演练

```bash
python3 examples/walkthrough.py
```

## 资料校验

```bash
python3 tools/validate_contract.py
```

所有命令都在项目根目录执行，不需要另行启动数据库、缓存或其他服务；
持久化演练使用临时 SQLite 文件，连接器使用内存桩。
