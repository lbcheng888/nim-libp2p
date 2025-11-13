plugins {
    alias(libs.plugins.android.application)
    alias(libs.plugins.kotlin.android)
    alias(libs.plugins.kotlin.compose)
}

fun String.escapeForBuildConfig(): String =
    this.replace("\\", "\\\\").replace("\"", "\\\"")

val bootstrapPeers =
    (project.findProperty("libp2p_bootstrap_peers") as String? ?: "")
val relayPeers =
    (project.findProperty("libp2p_relay_peers") as String? ?: "")

android {
    namespace = "com.example.libp2psmoke"
    compileSdk = 35
    ndkVersion = "26.1.10909125"

    defaultConfig {
        applicationId = "com.example.libp2psmoke"
        minSdk = 24
        targetSdk = 35
        versionCode = 1
        versionName = "1.0"

        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"

        externalNativeBuild {
            cmake {
                arguments("-DANDROID_STL=c++_shared")
            }
        }

        ndk {
            abiFilters += listOf("arm64-v8a")
        }

        buildConfigField(
            "String",
            "LIBP2P_BOOTSTRAP_PEERS",
            "\"${bootstrapPeers.escapeForBuildConfig()}\""
        )
        buildConfigField(
            "String",
            "LIBP2P_RELAY_PEERS",
            "\"${relayPeers.escapeForBuildConfig()}\""
        )
        buildConfigField(
            "String",
            "DEX_SETTLEMENT_RPCS",
            "\"{\\\"btcTestnet\\\":\\\"http://127.0.0.1:18332\\\",\\\"bscTestnet\\\":\\\"https://bsc-testnet-dataseed.bnbchain.org\\\"}\""
        )
        buildConfigField(
            "String",
            "DEX_USDC_CONTRACTS",
            "\"{\\\"bscTestnet\\\":\\\"0xE4140d73e9F09C5f783eC2BD8976cd8256A69AD0\\\"}\""
        )
    }

    buildTypes {
        release {
            isMinifyEnabled = false
            proguardFiles(
                getDefaultProguardFile("proguard-android-optimize.txt"),
                "proguard-rules.pro"
            )
        }
    }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
        isCoreLibraryDesugaringEnabled = true
    }
    kotlinOptions {
        jvmTarget = "11"
    }
    buildFeatures {
        compose = true
        buildConfig = true
    }

    externalNativeBuild {
        cmake {
            path = file("src/main/cpp/CMakeLists.txt")
        }
    }

    sourceSets["main"].jniLibs.srcDirs("src/main/jniLibs")

    packaging {
        jniLibs {
            pickFirsts += "lib/**/libc++_shared.so"
        }
        resources {
            excludes += "/META-INF/{AL2.0,LGPL2.1}"
        }
    }
}

dependencies {

    implementation(libs.androidx.core.ktx)
    implementation(libs.androidx.lifecycle.runtime.ktx)
    implementation(libs.androidx.lifecycle.viewmodel.compose)
    implementation(libs.androidx.activity.compose)
    implementation(platform(libs.androidx.compose.bom))
    implementation(libs.androidx.ui)
    implementation(libs.androidx.ui.graphics)
    implementation(libs.androidx.ui.tooling.preview)
    implementation(libs.androidx.material3)
    implementation(libs.androidxMaterialIconsExtended)
    implementation(libs.okhttp)
    implementation(libs.web3j.core)
    coreLibraryDesugaring(libs.desugar.jdk.libs)
    testImplementation(libs.junit)
    androidTestImplementation(libs.androidx.junit)
    androidTestImplementation(libs.androidx.espresso.core)
    androidTestImplementation(platform(libs.androidx.compose.bom))
    androidTestImplementation(libs.androidx.ui.test.junit4)
    debugImplementation(libs.androidx.ui.tooling)
    debugImplementation(libs.androidx.ui.test.manifest)
}
