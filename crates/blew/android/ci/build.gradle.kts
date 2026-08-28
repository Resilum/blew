plugins {
    id("com.android.library")
    id("org.jetbrains.kotlin.android")
}

android {
    namespace = "org.jakebot.blew"
    compileSdk = 36

    defaultConfig {
        minSdk = 24
    }

    // Compile the real sources in place rather than copying them.
    sourceSets["main"].java.srcDirs("../src/main/java")
    sourceSets["main"].manifest.srcFile("../src/main/AndroidManifest.xml")

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
}

kotlin {
    compilerOptions {
        jvmTarget = org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17
    }
}

dependencies {
    // Kept in step with ../build.gradle.kts by hand; the two lists diverging
    // shows up here as a compile error, which is the point.
    implementation("androidx.core:core-ktx:1.9.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.7.3")
    implementation(project(":tauri-android"))
}
