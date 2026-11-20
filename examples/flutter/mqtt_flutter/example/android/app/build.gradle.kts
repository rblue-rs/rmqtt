plugins {
    id("com.android.application")
    // The Flutter Gradle Plugin must be applied after the Android and Kotlin Gradle plugins.
    id("dev.flutter.flutter-gradle-plugin")
}

android {
    namespace = "rs.rmqtt.mqtt_flutter_example"
    compileSdk = flutter.compileSdkVersion
    ndkVersion = flutter.ndkVersion

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    defaultConfig {
        // TODO: Specify your own unique Application ID (https://developer.android.com/studio/build/application-id.html).
        applicationId = "rs.rmqtt.mqtt_flutter_example"
        // You can update the following values to match your application needs.
        // For more information, see: https://flutter.dev/to/review-gradle-config.
        minSdk = flutter.minSdkVersion
        targetSdk = flutter.targetSdkVersion
        // Uses the version code from pubspec.yaml. When using split APKs, 1000 * ABI_VERSION
        // is added automatically by Flutter. (https://developer.android.com/studio/build/configure-apk-splits#configure-APK-versions)
        // You can force using the value of versionCode by specifying the `-P force-version-code-ignoring-abi=true`
        // flag during build.
        versionCode = flutter.versionCode
        versionName = flutter.versionName
        ndk {
            // Default ABI for this skeleton. Other ABIs are not packaged.
            abiFilters += listOf("arm64-v8a")
        }
    }

    buildTypes {
        release {
            // TODO: Add your own signing config for the release build.
            // Signing with the debug keys for now, so `flutter run --release` works.
            signingConfig = signingConfigs.getByName("debug")
        }
    }
}

kotlin {
    compilerOptions {
        jvmTarget = org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17
    }
}

flutter {
    source = "../.."
}

// If libmqtt_c.so has not been staged yet, build the arm64-v8a artifact.
// The Flutter FFI plugin also packages jniLibs from the plugin project.
val rustLib = file("${project.projectDir}/../../../android/src/main/jniLibs/arm64-v8a/libmqtt_c.so")
val rustScript = file("${project.projectDir}/../../../../tools/build-libs.sh")
tasks.register<Exec>("buildMqttRustAndroidArm64") {
    onlyIf { rustScript.exists() && !rustLib.exists() }
    workingDir = rustScript.parentFile
    commandLine("bash", rustScript.absolutePath, "android-arm64")
}
tasks.named("preBuild") {
    dependsOn("buildMqttRustAndroidArm64")
}
