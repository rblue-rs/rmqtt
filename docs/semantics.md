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
  Dart close 的 timeout 参数目前不控制排空，不可将此 API 当最终 drain close。
- disconnect 等待超时不会回滚 native 操作；调用方目前应接着显式 close。
  自动超时转关闭态、幂等并发 disconnect 与完整关闭协议仍属 T12 余项。
- 无自动重连；持久会话与订阅期望集未实现，不能从 reconnect 成功推断这些能力已完成。
- shutdownRuntime 必须由客户端拥有方协调；每 isolate 的 init/shutdown 引用配对，
  最后一次 shutdown 拒绝仍有 native client lease 的情况，并拒绝 runtime worker 上调用。

## 状态码（实验 ABI）

0 OK；1 InvalidArgument；2 NotInitialized；3 Closed；4 Busy；5 QueueFull；6 Unsupported；
7 Network；8 Refused；9 BufferTooSmall；10 Empty；11 Internal；12 Overflow；13 Timeout（预留，
Dart 等待超时当前是 TimeoutException）。C poll 本身从不阻塞。无日志口令、私钥或 payload。

## 缺失不是降级

TLS/WS/V5/重连/credentials 等尚未暴露在当前 Options 中，不能误认为这些选项会被忽略
并降级。最终 API 尚未冻结。任何移动端能力必须另跑交叉构建、模拟器、真机验证后再陈述。
