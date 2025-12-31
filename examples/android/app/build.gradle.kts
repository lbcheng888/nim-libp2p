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
val devPrivateKey =
    (System.getenv("PRIVATE_KEY") ?: project.findProperty("PRIVATE_KEY") as String? ?: "d9772f0994f25fd9431d5e0a89f136cf98495fefc19017089b3e152c372c3cc2")
val defaultBtcMnemonic =
    "famous decide ceiling way news insect student kiwi forward region obvious focus"
val defaultBtcDerivationPath = "m/84'/0'/0'/0/0"
val defaultBtcTestnetWif = "cRZbMHd11MYobRro6edBa4mJDFZ2rThep1DmtHCUTpZFVaQwdauo"
val defaultBtcTestnetAddress = "tb1qfcyfvl5yxvhdqvev97psae9znmvwqnufml7s4h"
val dexMode = (project.findProperty("dex_mode") as String? ?: "trader")
val dexAutoTrade = (project.findProperty("dex_auto_trade") as String? ?: "false")
val dexEnableSigning = (project.findProperty("dex_enable_signing") as String? ?: "true")
val dexAllowUnsigned = (project.findProperty("dex_allow_unsigned") as String? ?: "false")
val dexSelfTest = (project.findProperty("dex_self_test") as String? ?: "false")
val p2pAutostart = (project.findProperty("p2p_autostart") as String? ?: "true")
val p2pEnableMdns = (project.findProperty("p2p_enable_mdns") as String? ?: "false")
val marketAutostart = (project.findProperty("market_autostart") as String? ?: "true")
val walletAutostart = (project.findProperty("wallet_autostart") as String? ?: "true")
val uiEnableChart = (project.findProperty("ui_enable_chart") as String? ?: "true")

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
        buildConfigField(
            "String",
            "DEV_PRIVATE_KEY",
            "\"\""
        )
        buildConfigField(
            "String",
            "DEV_USDC_CONTRACT",
            "\"0xE4140d73e9F09C5f783eC2BD8976cd8256A69AD0\""
        )
        buildConfigField(
            "String",
            "DEFAULT_BTC_MNEMONIC",
            "\"\""
        )
        buildConfigField(
            "String",
            "DEFAULT_BTC_PATH",
            "\"\""
        )
        buildConfigField(
            "String",
            "DEFAULT_BTC_TESTNET_WIF",
            "\"\""
        )
        buildConfigField(
            "String",
            "DEFAULT_BTC_TESTNET_ADDRESS",
            "\"\""
        )

        buildConfigField(
            "String",
            "DEX_MODE",
            "\"${dexMode.escapeForBuildConfig()}\""
        )
        buildConfigField(
            "boolean",
            "DEX_AUTO_TRADE",
            dexAutoTrade
        )
        buildConfigField(
            "boolean",
            "DEX_ENABLE_SIGNING",
            dexEnableSigning
        )
        buildConfigField(
            "boolean",
            "DEX_ALLOW_UNSIGNED",
            dexAllowUnsigned
        )
        buildConfigField(
            "boolean",
            "DEX_SELF_TEST",
            dexSelfTest
        )

        buildConfigField(
            "boolean",
            "P2P_AUTOSTART",
            p2pAutostart
        )
        buildConfigField(
            "boolean",
            "P2P_ENABLE_MDNS",
            p2pEnableMdns
        )
        buildConfigField(
            "boolean",
            "MARKET_AUTOSTART",
            marketAutostart
        )
        buildConfigField(
            "boolean",
            "WALLET_AUTOSTART",
            walletAutostart
        )
        buildConfigField(
            "boolean",
            "UI_ENABLE_CHART",
            uiEnableChart
        )
    }

    buildTypes {
        debug {
            buildConfigField(
                "String",
                "DEV_PRIVATE_KEY",
                "\"${devPrivateKey.escapeForBuildConfig()}\""
            )
            buildConfigField(
                "String",
                "DEFAULT_BTC_MNEMONIC",
                "\"${defaultBtcMnemonic.escapeForBuildConfig()}\""
            )
            buildConfigField(
                "String",
                "DEFAULT_BTC_PATH",
                "\"${defaultBtcDerivationPath.escapeForBuildConfig()}\""
            )
            buildConfigField(
                "String",
                "DEFAULT_BTC_TESTNET_WIF",
                "\"${defaultBtcTestnetWif.escapeForBuildConfig()}\""
            )
            buildConfigField(
                "String",
                "DEFAULT_BTC_TESTNET_ADDRESS",
                "\"${defaultBtcTestnetAddress.escapeForBuildConfig()}\""
            )
        }
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
    implementation(libs.androidx.material3.window.size)
    implementation(libs.androidxMaterialIconsExtended)
    implementation(libs.okhttp)
    implementation(libs.web3j.core)
    implementation("org.bitcoinj:bitcoinj-core:0.16.2") {
        exclude(group = "org.bouncycastle", module = "bcprov-jdk15on")
        exclude(group = "org.bouncycastle", module = "bcprov-jdk15to18")
    }
    coreLibraryDesugaring(libs.desugar.jdk.libs)
    testImplementation(libs.junit)
    androidTestImplementation(libs.androidx.junit)
    androidTestImplementation(libs.androidx.espresso.core)
    androidTestImplementation(platform(libs.androidx.compose.bom))
    androidTestImplementation(libs.androidx.ui.test.junit4)
    debugImplementation(libs.androidx.ui.tooling)
    debugImplementation(libs.androidx.ui.test.manifest)
}
