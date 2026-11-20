# 本切片实际语义

## 可用范围

MQTT 3.1.1 TCP、clean session=true、QoS0/1。默认协议 Keep Alive=0，显式整秒
1..65535 可设；非整秒和溢出值拒绝，不截断。0 不关闭 TCP keepalive，也不是后台保活。
rumqttc 0.25.1 原始 v5 setter 对小于 5 秒 panic；v3 允许0，eventloop 对0不创建心跳定时器。
测试包含实际 CONNECT 字段0、1，无默认 PING 的1.4秒观察，以及显式1秒 PINGREQ。
v5 一律返回 typed Unsupported(6)，没有 fork、patch 或暗改 keepalive。

## 完成与排队

- connect：CONNACK；失败不自动重试。connect Future 超时中止本次原生句柄，后续可重新创建。
- subscribe/unsubscribe：对应 SUBACK/UNSUBACK；SUBACK 透出 broker 授予 QoS。
- publish：本地接收 Queued、引擎处理 Outgoing Publish 后 Sent、PUBACK 后 Acknowledged。
  QoS0 没有 ACK；Sent 不代表远端收到，ACK 不代表应用处理。
- 串行处理一个 publish/sub/unsub/disconnect 协议操作，另外最多排32个命令。
  这是验证切片的吞吐限制，不是最终并发设计。
- ticket 的 Future.timeout 不撤回消息，迟到 PUBACK 更新仍可到达。
- subscribe/unsubscribe 的等待超时不会撤回 broker 已执行的操作。
- 进站消息先进入原生有界队列，再手动发 QoS1 协议 ACK。不等 Dart 应用处理。
- QoS1 可能重复，无磁盘 outbox、无跨进程恢复保证。

## lifecycle（当前与目标的差异）

- 正常 disconnect 等引擎的 DISCONNECT 事件；之后可再 connect。不保证 broker 必然收到。
- **当前 close 是 force-close**：中止 actor、拒绝 pending、释放句柄；没有优雅排空。
  Dart `close()` 已删除无效的 timeout 参数，不可将此 API 当 drain close。
- Dart disconnect 等待超时会 force-stop 此次 transport，并回到可再次 connect 的
  disconnected 状态；不保证 broker 收到 DISCONNECT。并发调用共享第一调用的 deadline。
- 无自动重连；持久会话与订阅期望集未实现，不能从 reconnect 成功推断这些能力已完成。
- shutdownRuntime 必须由客户端拥有方协调；每 isolate 的 init/shutdown 引用配对，
  最后一次 shutdown 拒绝仍有 native client lease 的情况，并拒绝 runtime worker 上调用。

## 状态码（实验 ABI）

0 OK；1 InvalidArgument；2 NotInitialized；3 Closed；4 Busy；5 QueueFull；6 Unsupported；
7 Network；8 Refused；9 BufferTooSmall；10 Empty；11 Internal；12 Overflow；13 Timeout（预留，
Dart 等待超时当前是 TimeoutException）。C poll 不等待网络或事件，但内部互斥锁可能争用；不是 lock-free / wait-free。无日志口令、私钥或 payload。

## 缺失不是降级

TLS/WS/V5/重连/credentials 等尚未暴露在当前 Options 中，不能误认为这些选项会被忽略
并降级。最终 API 尚未冻结。任何移动端能力必须另跑交叉构建、模拟器、真机验证后再陈述。

## 生命周期

仍为 Linux host 实验切片。没有 drain、回调握手、自动重连、
磁盘 outbox 或跨进程恢复；本地等待者的一次结算不等于 MQTT exactly-once delivery。

### 状态转移

| 当前状态 | 调用/事件 | 后续状态与行为 |
| --- | --- | --- |
| disconnected | connect | connecting；只有 CONNACK 才完成并转 connected |
| connecting/connected/disconnecting | 再 connect | 拒绝 StateError；不会新建第二条 transport |
| connected | disconnect | disconnecting；拒绝新 publish/sub/unsub（native Busy）；收到 Outgoing DISCONNECT 才完成 |
| disconnecting | Dart 重复/并发/reentrant disconnect | 共享同一尝试及第一调用的 deadline；各 caller 可独立取消等待，不重发 DISCONNECT |
| disconnected | disconnect | Dart 直接成功；native 对每个新单调 op 发 Disconnected completion |
| connecting | Dart disconnect | force-stop 本次连接尝试；pending connect 以 StateError 失败；客户端仍可再 connect |
| connecting | native disconnect | Busy；原始 C 调用方可用 close 永久中止，或继续等 CONNACK；不新增 C reset 导出 |
| disconnecting | Dart timeout/提交失败 | force-stop transport、fail pending、回 disconnected；后续可再 connect |
| connecting | connect timeout | 原生 close/free 此次 handle；回 disconnected，后续 connect 用新 handle |
| connected | SUBACK/UNSUBACK 等待超时 | 只移除对应 waiter，连接状态不变；迟到 ACK 不能结算其他 op |
| 任意非 closed | cancel(op_id) | 仅终结本地 waiter/票据尚未完成阶段；不会撤销 MQTT 包或状态转移 |
| 任意非 closed | close() | 立即转 closed；stop poll → native close/free → free buffers；fail pending；无 drain |
| closed | close() | 幂等成功；其余合法操作拒绝；native poll 为 Closed(3)，绝不 Empty/旧消息 |
| 任意 | shutdownRuntime | 不改变 client 状态；进程级引用配对；最后一次有 live lease 或在 worker 上调用时 Busy |

native 同一连接最多容纳 32 个 disconnect completion ids（含首个）。重复请求必须遵守
op_id 单调规则；C 不接受把同一个旧 id 当作新请求复用。已有命令仍串行处理，原请求队列
容量 32 不变；coalescing 只发一条 DISCONNECT。每个被接受的 id 获自己的完成，或遇致命
Failure/Overflow 时按该终态失败；超额返回 QueueFull，不增设无界等待队列。

### 每种操作的本地结果

| 操作 | 成功点 | 失败/超时 | cancel | close |
| --- | --- | --- | --- | --- |
| connect | CONNACK | native MqttException；超时 TimeoutException + force-stop transport | waiter 为 MqttOperationCancelled；尚在进行的连接仍可能随后 connected | 尚未完成 Future 失败 |
| subscribe | SUBACK | native MqttException；超时 TimeoutException，删除 waiter | 只停止本地等待，broker 仍可能订阅成功 | 尚未完成 Future 失败 |
| unsubscribe | UNSUBACK | 同上；迟到 ACK 不影响新操作 | 只停止本地等待，broker 仍可能取消成功 | 尚未完成 Future 失败 |
| publish ticket | Queued / Sent / Acknowledged 分阶段 | native 失败/断开仅失败未完成阶段；调用者 Future.timeout 不改变原阶段 Future | 所有尚未完成阶段失败；之后 progress 仍可走 publishUpdates | 所有尚未完成阶段失败 |
| disconnect | 本地 Outgoing DISCONNECT；不是远端确认 | 默认 10 秒；失败/超时 force-stop 后可重连 | 本地等待失败，协议断开仍可继续；实际状态仍由事件决定 | 尚未完成 Future 失败 |
| close | force-stop 与释放，无 drain | 不接受 timeout；native 错误仍抛出 | 已终态，无待取消操作 | 重复调用成功 |
| shutdownRuntime | 配对释放 init 引用 | native Busy(4)，或 Dart isolate 内 live wrapper 时 StateError | 无操作 id，不适用 | 先 close 各客户端再 shutdown |

QoS0 `whenAcknowledged == null`，不伪造 ACK。成功的 Queued/Sent 不会因 cancel/close
被改写；cancel 后 ticket 终态 Future 重复 await 仍失败；迟到 PUBACK 仅更新流，不会
再次完成已失败 Future。超时/取消/ACK/close 都至多结算一次本地 waiter；共享 disconnect 的物理 deadline 不因 caller 取消而删除。

### 错误映射与取消接口

实验 ABI 状态码仍是上文 0–13，既有 event/option layout 不变。新增 ownership helper 见 ownership.md。native 非零业务错误 →
`MqttException(code)`；Dart 自有 deadline 仍为 `TimeoutException`（不冒用 native 预留13）；
已关闭/不当状态/本地断开结算仍保留既有 `StateError`；本地取消用其子类
`MqttOperationCancelled(operationId)`，不增加 native Cancelled code。统一改成全套
MqttException/Closed/Timeout/Cancelled 的统一映射尚未提供。

`cancel(int operationId)` 仅在 Dart waiter 层实现，不增加 C cancel 导出。原有错误分类
保持不变；不统一错误类型，也不增加 native Cancelled 状态码。

### 公共操作句柄

提供 additive `MqttOperation<T>`，包含立即可用的 `operationId`、`future`、
`cancel()`。`beginConnect` / `beginSubscribe` / `beginUnsubscribe` / `beginDisconnect`
同步返回句柄；客户端已经 close 时同步抛 StateError，不返回可用句柄。
原有 `connect` / `subscribe` / `unsubscribe` / `disconnect` 仍返回 Future，委托 begin 方法。
不需猜 id；publish 继续用已有 ticket.operationId 与 client.cancel(id)。

```dart
await client.connect(); // 保留原用法；CONNACK 完成
final op = client.beginSubscribe('sensors/#');
print(op.operationId); // begin 返回后即能取得
await op.cancel();     // 或 await client.cancel(op.operationId)
try {
  await op.future;
} on MqttOperationCancelled {
  // 只放弃本地等待，broker 仍可能执行订阅。
}
```

- handle.cancel 幂等；已取消、完成、超时或 close 后均不改变结果。直接 client.cancel(id)
  仍保留已关闭时 StateError 的既有语义。ACK/同步 native 拒绝/deadline 已提交后不会被
  紧随的 cancel 改写；控制结果提交先于同步 listener，使 listener close 不覆盖已提交结果。
- 立即取消 connect（包括 native 创建前）仍只放弃等待；已启动连接可随后 connected。
  close 才永久失效；未取消的 connect 等待超时仍 force-stop 本次 transport。
- 每次 beginDisconnect 都有独立本地 id/Future；只发一个共享 DISCONNECT，第一调用
  deadline 不变。取消首个/加入者/所有 callers 都不取消其他人的等待或物理 deadline。
  共享原生终态为已经注册的未取消 callers 提交结果；cancelled caller 不再被 late event 改写。
- Dart 每个共享尝试最多保留32个 caller join continuation（含已取消者，直到共享尝试终结）。
  超额句柄的 future 为 MqttException(5/QueueFull)；取消不能循环重入来积累无界 continuation。
  这与 native 对直接 C 调用者最多32个 disconnect completion ids 的限制是不同层次。
- subscribe/unsubscribe 句柄 id 与成功 Ack.operationId 一致。超时/取消后的 late ACK 不结算
  新操作；本地 exactly-once settlement 不是 MQTT exactly-once delivery。

不提供统一错误类型或 C cancel/reset 导出；ABI 尚未冻结。
C 调用者仍自行提供 op_id，在自己的等待层放弃等待。

## 所有权与退出

显式 close 优先。NativeFinalizer 作 GC/isolate-group 退出托底；单个 owner 的退出通知
只向 native port 发送整数，native 自行回收 client、匹配 allocator 的缓冲与 runtime ref，
不调用 Dart cleanup、不等待 Dart 握手，不改变 caller-owned polling 数据路径。

已完成初始化/连接后的受控 Isolate.run、自然静默退出及两种 VM kill 优先级在 Linux
通过。注册/连接分配中途强杀、进程退出/VM crash、热重启/移动端不保证；活跃 Timer
可以使 isolate 继续存活。GC 不保证及时性。native shutdown 对退出通知竞争最多等待2秒，
再应用既有 shutdown/live-lease guard；这不是整个 shutdown API 的总耗时上限。
Tokio blocking/DNS 工作可能晚于 background shutdown 返回，不承诺即时线程/RSS回收。

caller buffers、retiring quota、背压/Overflow 表和完整支持矩阵见 [ownership](ownership.md)。
