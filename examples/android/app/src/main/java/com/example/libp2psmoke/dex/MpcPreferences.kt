package com.example.libp2psmoke.dex

import android.content.Context
import android.content.SharedPreferences

class MpcPreferences(context: Context) {
    private val prefs: SharedPreferences = context.getSharedPreferences("mpc_prefs", Context.MODE_PRIVATE)

    companion object {
        private const val KEY_LOCAL_SECRET = "local_secret"
        private const val KEY_LOCAL_NONCE = "local_nonce" // Optional, often ephemeral
        private const val KEY_REMOTE_PUB = "remote_pub"
        private const val KEY_JOINT_ADDRESS = "joint_address"
        private const val KEY_REMOTE_SECRET = "remote_secret" // For Simulation ONLY
        private const val KEY_JOINT_PUB = "joint_pub"
        private const val KEY_LOCAL_PUB = "local_pub"
    }

    fun saveLocalKeys(secret: String, pub: String, remotePub: String, jointAddress: String, jointPub: String) {
        prefs.edit()
            .putString(KEY_LOCAL_SECRET, secret)
            .putString(KEY_LOCAL_PUB, pub)
            .putString(KEY_REMOTE_PUB, remotePub)
            .putString(KEY_JOINT_ADDRESS, jointAddress)
            .putString(KEY_JOINT_PUB, jointPub)
            .apply()
    }

    fun getLocalSecret(): String? = prefs.getString(KEY_LOCAL_SECRET, null)
    fun getLocalPub(): String? = prefs.getString(KEY_LOCAL_PUB, null)
    fun getRemotePub(): String? = prefs.getString(KEY_REMOTE_PUB, null)
    fun getJointAddress(): String? = prefs.getString(KEY_JOINT_ADDRESS, null)
    fun getJointPub(): String? = prefs.getString(KEY_JOINT_PUB, null)

    // Simulation Only
    fun saveRemoteSecret(secret: String) {
        prefs.edit().putString(KEY_REMOTE_SECRET, secret).apply()
    }

    fun getRemoteSecret(): String? = prefs.getString(KEY_REMOTE_SECRET, null)

    fun clear() {
        prefs.edit().clear().apply()
    }
}
