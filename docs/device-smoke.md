# 移动端验证门槛（本轮未执行）

| 层级 | Android | iOS |
|---|---|---|
| Rust 目标交叉构建 | NOT RUN（未准备 NDK） | NOT RUN（Linux box 无 Xcode） |
| Flutter plugin 集成/模拟器 | NOT RUN（未安装 Flutter/Android SDK） | NOT RUN（无 macOS/Xcode） |
| 真机 TLS 连接、订阅、QoS1发布/收消息 | NOT RUN（未使用用户设备） | NOT RUN（未使用设备/签名） |

后续必须分别记录目标 ABI、工具链/SDK 版本、设备型号/OS、签名方式、broker 配置、
CA 信任与主机名校验、前后台切换、断网恢复、close 结果。不能把 Linux 测试绿、
.so 构建成功、无签名 XCFramework 或模拟器成功写成真机已支持。

当前无 TLS 实现，因此还不能进行“真机 TLS 冒烟”。iOS 无后台 MQTT 常连承诺。
