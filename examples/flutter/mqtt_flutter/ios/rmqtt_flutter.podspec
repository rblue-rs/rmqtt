#
# Flutter FFI plugin shell for the experimental mqtt-c Rust static library.
# The iOS device slice is aarch64-apple-ios (arm64). This file is project
# scaffolding; a Linux host cannot produce the .a. See ../tool/build-native.sh
# and docs/device-smoke.md. Not a physical-device or background-always-on
# result. No application password or private key is bundled.
#
Pod::Spec.new do |s|
  s.name             = 'rmqtt_flutter'
  s.version          = '0.1.0'
  s.summary          = 'Experimental Rust MQTT FFI plugin skeleton.'
  s.description      = <<-DESC
Experimental Flutter FFI plugin shell that builds bundled mqtt-c Rust source for iOS arm64 devices.
                       DESC
  s.homepage         = 'https://github.com/rblue-rs/rmqtt'
  s.license          = { :type => 'Apache-2.0', :file => '../LICENSE' }
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
  # Package-local source build; orchestration only until independently validated on iOS.
  s.script_phase = {
    :name => 'Build bundled Rust source',
    :execution_position => :before_compile,
    :script => 'set -eu; bash "${PODS_TARGET_SRCROOT}/../tool/build-native.sh" ios-arm64',
    :output_files => ['${PODS_TARGET_SRCROOT}/Frameworks/libmqtt_c.a']
  }
  s.swift_version = '5.0'
end
