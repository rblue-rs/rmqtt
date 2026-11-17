# FFI 安全发现：异步 listener 不能使用回调返回即失效的借用指针

原计划 T13：回调期间借出 buffer，回调返回后原生释放。
原计划 T16：使用 Dart `NativeCallable.listener`，在 Dart 回调里复制。
这两项组合存在 use-after-free：listener 的原生 trampoline 返回，不表示 Dart
回调已经执行。不能等 Dart 手动运行复制以后，假装原借用期仍然有效。

官方文档（本轮打开核对）：
https://api.dart.dev/dart-ffi/NativeCallable/NativeCallable.listener.html
明确写明 native 不等待回调，Dart callback 在未来运行；指针资源必须保持有效直到调用完成。

## 本切片的安全替代（实验 ABI，尚未冻结）

- 只暴露 `mqtt_client_poll`：Dart isolate 定时主动拉取，原生不调用 Dart。
- `MqttEvent` 只含标量与长度；topic/payload 复制到调用方提供的有效缓冲。
- 小缓冲返回 9，填写 required lengths，不弹出事件，不修改 topic/payload。
- 调用成功前 input 数据已复制；poll 返回后 native 不保留 output 指针。
- 句柄为单调递增 u64，通过受锁表查找，不是裸 Rust 指针；重复 free 安全。
- free 前必须 close；runtime 最后一次 shutdown 在有句柄 lease 时返回 Busy。
- Dart 关闭先停 timer，再原生 close/free，再释放 caller buffers；无回调可在 close 后到达。
- 正常应用必须显式 close；NativeFinalizer 托底尚未实现。
- 原生 32 个待办命令 + 1 个协议操作在途；事件 128 + 单独保留的终态 fatal 槽。
  队列满则断线，已有事件仍保留，fatal 不重复插入。payload 上限 64KiB。
- Dart 每轮最多 poll 64 个事件，每个流最多 16 个 listener。任一 listener pause 时
  停止整个 client poll，防止 Dart paused subscription 形成无界队列；可能触发原生 Overflow。

## 后续可选最终 ABI（需审阅）

A. 保留 pull poll，并提供可选的**仅标量**唤醒通知；数据仍由 Dart 拉取。
B. owned event + 显式 event_release；计数有界，Dart ACK release 后才能释放，
   close 必须协调未消费 listener 消息与 NativeCallable.close 的先后。

不能直接把当前轮询 ABI 宣称为已实现原 T13/T16/T15。不要通过线程阻塞等待
Dart callback 返回修补所有权，这会引入 close/shutdown 死锁。

## panic 与 C 指针限制

所有导出函数均在 catch_unwind 边界内将 unwind 转成 INTERNAL(11)，主机有测试。
panic=abort 以及无效 C 指针导致的 UB/segfault 不会被捕获。调用方仍须满足头文件的
长度、可读/可写、对齐与不重叠前提；null 与错误长度可检出不表示能验证任意地址。
