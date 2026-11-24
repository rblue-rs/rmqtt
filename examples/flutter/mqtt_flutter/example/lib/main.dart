import 'package:flutter/material.dart';
import 'package:rmqtt_flutter/rmqtt_flutter.dart';

void main() {
  runApp(const MqttSkeletonApp());
}

/// Minimal sample. It does not connect to a broker and does not bundle
/// credentials, certificates, or private keys. Mobile builds are
/// "buildable" evidence only until a later physical-device TLS smoke run.
class MqttSkeletonApp extends StatelessWidget {
  const MqttSkeletonApp({super.key});

  @override
  Widget build(BuildContext context) {
    return const MaterialApp(
      title: 'rmqtt experimental sample',
      home: _Home(),
    );
  }
}

class _Home extends StatelessWidget {
  const _Home();

  @override
  Widget build(BuildContext context) {
    const options = MqttConnectOptions();
    return Scaffold(
      appBar: AppBar(title: const Text('rmqtt experimental sample')),
      body: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            const Text(
              'This is a packaging sample for the experimental Flutter FFI '
              'plugin skeleton. It is not a production MQTT client.',
            ),
            const SizedBox(height: 12),
            const Text(
              'Native mobile libraries are built with panic=abort: a Rust panic '
              'terminates the process and is not converted into a Dart exception. '
              'iOS does not promise an always-on background MQTT connection. '
              'A successful APK or unsigned iOS build is not physical-device '
              'TLS verification.',
            ),
            const SizedBox(height: 12),
            Text(
              'Default keepAlivePeriod is ${options.keepAlivePeriod}, which is '
              'encoded as zero in CONNECT. This screen does not open a socket.',
            ),
          ],
        ),
      ),
    );
  }
}
