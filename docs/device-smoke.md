# Mobile validation gates — not run

| Layer | Android | iOS |
| --- | --- | --- |
| Rust cross-build | NOT RUN; requires NDK | NOT RUN; requires Apple toolchain |
| Flutter plugin / emulator / simulator | NOT RUN | NOT RUN |
| Physical-device TLS, subscribe, QoS1 publish/receive | NOT RUN | NOT RUN |

Future evidence must record target ABI, SDK/toolchain, OS/device, signing, broker,
CA trust and hostname checks, foreground/background transitions, network loss and
close behavior. A Linux test, shared-library build, unsigned XCFramework or
simulator result is not proof of physical-device support. TLS is not implemented
in this slice; no iOS always-on background MQTT promise is made.
