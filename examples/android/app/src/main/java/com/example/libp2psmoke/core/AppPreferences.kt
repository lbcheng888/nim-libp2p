package com.example.libp2psmoke.core

import android.content.Context
import com.example.libp2psmoke.BuildConfig

/**
 * Runtime configuration store for the Android example app.
 *
 * Production builds should avoid build-time flags for connectivity; instead, allow
 * editing multiaddrs and feature toggles at runtime.
 */
class AppPreferences(context: Context) {
    private val appContext = context.applicationContext
    private val prefs = appContext.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)

    fun getBootstrapPeersRaw(): String =
        prefs.getString(KEY_BOOTSTRAP_PEERS_RAW, null) ?: BuildConfig.LIBP2P_BOOTSTRAP_PEERS

    fun setBootstrapPeersRaw(value: String) {
        prefs.edit().putString(KEY_BOOTSTRAP_PEERS_RAW, value).apply()
    }

    fun clearBootstrapPeersRaw() {
        prefs.edit().remove(KEY_BOOTSTRAP_PEERS_RAW).apply()
    }

    fun getRelayPeersRaw(): String =
        prefs.getString(KEY_RELAY_PEERS_RAW, null) ?: BuildConfig.LIBP2P_RELAY_PEERS

    fun setRelayPeersRaw(value: String) {
        prefs.edit().putString(KEY_RELAY_PEERS_RAW, value).apply()
    }

    fun clearRelayPeersRaw() {
        prefs.edit().remove(KEY_RELAY_PEERS_RAW).apply()
    }

    fun isMarketEnabled(): Boolean =
        prefs.getBoolean(KEY_MARKET_ENABLED, BuildConfig.MARKET_AUTOSTART)

    fun setMarketEnabled(enabled: Boolean) {
        prefs.edit().putBoolean(KEY_MARKET_ENABLED, enabled).apply()
    }

    fun getMarketInterval(defaultInterval: String = "1m"): String =
        prefs.getString(KEY_MARKET_INTERVAL, null) ?: defaultInterval

    fun setMarketInterval(interval: String) {
        prefs.edit().putString(KEY_MARKET_INTERVAL, interval).apply()
    }

    fun clearAll() {
        prefs.edit().clear().apply()
    }

    companion object {
        private const val PREFS_NAME = "app_prefs"
        private const val KEY_BOOTSTRAP_PEERS_RAW = "bootstrap_peers_raw"
        private const val KEY_RELAY_PEERS_RAW = "relay_peers_raw"
        private const val KEY_MARKET_ENABLED = "market_enabled"
        private const val KEY_MARKET_INTERVAL = "market_interval"
    }
}

