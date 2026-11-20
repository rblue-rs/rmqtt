#
# Flutter FFI plugin shell for the experimental mqtt-c Rust static library.
# The iOS device slice is aarch64-apple-ios (arm64). This file is project
# scaffolding; a Linux host cannot produce the .a. See tools/build-libs.sh
# and docs/device-smoke.md. Not a physical-device or background-always-on
# result. No application password or private key is bundled.
#
Pod::Spec.new do |s|
  s.name             = 'mqtt_flutter'
  s.version          = '0.1.0'
  s.summary          = 'Experimental Rust MQTT FFI plugin skeleton.'
  s.description      = <<-DESC
Experimental Flutter FFI plugin shell that links a prebuilt mqtt-c static library.
                       DESC
  s.homepage         = 'https://github.com/rblue-rs/rmqtt'
  s.license          = { :type => 'Apache-2.0' }
  s.author           = 'rmqtt'
  s.source           = { :path => '.' }
  s.source_files     = 'Classes/**/*'
  s.public_header_files = 'Classes/**/*.h'
  s.vendored_libraries = 'Frameworks/libmqtt_c.a'
  s.dependency 'Flutter'
  s.platform = :ios, '15.0'
  s.pod_target_xcconfig = {
    'DEFINES_MODULE' => 'YES',
    'EXCLUDED_ARCHS[sdk=iphonesimulator*]' => 'i386 x86_64',
    'OTHER_LDFLAGS' => '-force_load $(PODS_TARGET_SRCROOT)/Frameworks/libmqtt_c.a'
  }
  s.swift_version = '5.0'
end
