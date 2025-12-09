package com.example.libp2psmoke.core

import android.content.Context
import android.security.keystore.KeyGenParameterSpec
import android.security.keystore.KeyProperties
import android.util.Base64
import java.security.KeyStore
import javax.crypto.Cipher
import javax.crypto.KeyGenerator
import javax.crypto.SecretKey
import javax.crypto.spec.GCMParameterSpec

/**
 * 安全密钥存储
 * 使用 Android KeyStore 安全存储和加密敏感数据
 */
class SecureKeyStore(private val context: Context) {
    
    companion object {
        private const val KEYSTORE_PROVIDER = "AndroidKeyStore"
        private const val MASTER_KEY_ALIAS = "dex_master_key"
        private const val TRANSFORMATION = "AES/GCM/NoPadding"
        private const val GCM_TAG_LENGTH = 128
        private const val PREFS_NAME = "secure_storage"
        private const val TAG = "SecureKeyStore"
    }
    
    private val keyStore: KeyStore = KeyStore.getInstance(KEYSTORE_PROVIDER).apply {
        load(null)
    }
    
    private val prefs by lazy {
        context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
    }
    
    /**
     * 安全存储私钥
     * @param alias 密钥别名
     * @param privateKey 私钥数据 (会在存储后清零)
     */
    fun storePrivateKey(alias: String, privateKey: CharArray): Boolean {
        return try {
            // Convert CharArray to ByteArray directly without creating a String
            val charBuffer = java.nio.CharBuffer.wrap(privateKey)
            val byteBuffer = java.nio.charset.StandardCharsets.UTF_8.encode(charBuffer)
            val data = ByteArray(byteBuffer.remaining())
            byteBuffer.get(data)
            
            try {
                val encrypted = encrypt(data)
                
                // 存储加密后的数据
                val encoded = Base64.encodeToString(encrypted, Base64.NO_WRAP)
                prefs.edit().putString("pk_$alias", encoded).apply()
                
                SecureLogger.d(TAG, "Private key stored securely: ${SecureLogger.maskAddress(alias)}")
                true
            } finally {
                // 清零原始数据
                data.fill(0)
                if (byteBuffer.hasArray()) {
                    byteBuffer.array().fill(0)
                }
                // Note: privateKey clearing is left to the caller or we can do it here if agreed
                privateKey.fill('\u0000')
            }
        } catch (e: Exception) {
            SecureLogger.e(TAG, "Failed to store private key", e)
            false
        }
    }
    
    /**
     * 读取私钥
     * @param alias 密钥别名
     * @return 私钥数据 (调用方负责清零)
     */
    fun retrievePrivateKey(alias: String): CharArray? {
        return try {
            val encoded = prefs.getString("pk_$alias", null) ?: return null
            val encrypted = Base64.decode(encoded, Base64.NO_WRAP)
            val decrypted = decrypt(encrypted)
            
            try {
                val byteBuffer = java.nio.ByteBuffer.wrap(decrypted)
                val charBuffer = java.nio.charset.StandardCharsets.UTF_8.decode(byteBuffer)
                val result = CharArray(charBuffer.remaining())
                charBuffer.get(result)
                
                // Clear the char buffer if possible (though it's managed by CharBuffer)
                if (charBuffer.hasArray()) {
                    charBuffer.array().fill('\u0000')
                }
                
                result
            } finally {
                decrypted.fill(0)
            }
        } catch (e: Exception) {
            SecureLogger.e(TAG, "Failed to retrieve private key", e)
            null
        }
    }
    
    /**
     * 删除私钥
     */
    fun deletePrivateKey(alias: String): Boolean {
        return try {
            prefs.edit().remove("pk_$alias").apply()
            true
        } catch (e: Exception) {
            SecureLogger.e(TAG, "Failed to delete private key", e)
            false
        }
    }
    
    /**
     * 检查私钥是否存在
     */
    fun hasPrivateKey(alias: String): Boolean {
        return prefs.contains("pk_$alias")
    }
    
    /**
     * 安全存储任意字符串
     */
    fun storeString(key: String, value: String): Boolean {
        return try {
            val encrypted = encrypt(value.toByteArray(Charsets.UTF_8))
            val encoded = Base64.encodeToString(encrypted, Base64.NO_WRAP)
            prefs.edit().putString(key, encoded).apply()
            true
        } catch (e: Exception) {
            SecureLogger.e(TAG, "Failed to store string", e)
            false
        }
    }
    
    /**
     * 读取加密字符串
     */
    fun retrieveString(key: String): String? {
        return try {
            val encoded = prefs.getString(key, null) ?: return null
            val encrypted = Base64.decode(encoded, Base64.NO_WRAP)
            String(decrypt(encrypted), Charsets.UTF_8)
        } catch (e: Exception) {
            SecureLogger.e(TAG, "Failed to retrieve string", e)
            null
        }
    }
    
    /**
     * 删除存储的值
     */
    fun delete(key: String): Boolean {
        return try {
            prefs.edit().remove(key).apply()
            true
        } catch (e: Exception) {
            false
        }
    }
    
    /**
     * 清除所有安全存储
     */
    fun clearAll() {
        try {
            prefs.edit().clear().apply()
            if (keyStore.containsAlias(MASTER_KEY_ALIAS)) {
                keyStore.deleteEntry(MASTER_KEY_ALIAS)
            }
        } catch (e: Exception) {
            SecureLogger.e(TAG, "Failed to clear secure storage", e)
        }
    }
    
    // ═══════════════════════════════════════════════════════════════════
    // 私有加密方法
    // ═══════════════════════════════════════════════════════════════════
    
    private fun getOrCreateMasterKey(): SecretKey {
        if (!keyStore.containsAlias(MASTER_KEY_ALIAS)) {
            val keyGenerator = KeyGenerator.getInstance(
                KeyProperties.KEY_ALGORITHM_AES,
                KEYSTORE_PROVIDER
            )
            
            val spec = KeyGenParameterSpec.Builder(
                MASTER_KEY_ALIAS,
                KeyProperties.PURPOSE_ENCRYPT or KeyProperties.PURPOSE_DECRYPT
            )
                .setBlockModes(KeyProperties.BLOCK_MODE_GCM)
                .setEncryptionPaddings(KeyProperties.ENCRYPTION_PADDING_NONE)
                .setKeySize(256)
                .setUserAuthenticationRequired(false) // 不需要用户认证
                .setRandomizedEncryptionRequired(true)
                .build()
            
            keyGenerator.init(spec)
            keyGenerator.generateKey()
        }
        
        return keyStore.getKey(MASTER_KEY_ALIAS, null) as SecretKey
    }
    
    private fun encrypt(data: ByteArray): ByteArray {
        val cipher = Cipher.getInstance(TRANSFORMATION)
        cipher.init(Cipher.ENCRYPT_MODE, getOrCreateMasterKey())
        
        val iv = cipher.iv
        val encrypted = cipher.doFinal(data)
        
        // 格式: [IV长度(1字节)] + [IV] + [加密数据]
        return ByteArray(1 + iv.size + encrypted.size).apply {
            this[0] = iv.size.toByte()
            System.arraycopy(iv, 0, this, 1, iv.size)
            System.arraycopy(encrypted, 0, this, 1 + iv.size, encrypted.size)
        }
    }
    
    private fun decrypt(data: ByteArray): ByteArray {
        val ivLength = data[0].toInt()
        val iv = data.copyOfRange(1, 1 + ivLength)
        val encrypted = data.copyOfRange(1 + ivLength, data.size)
        
        val cipher = Cipher.getInstance(TRANSFORMATION)
        val spec = GCMParameterSpec(GCM_TAG_LENGTH, iv)
        cipher.init(Cipher.DECRYPT_MODE, getOrCreateMasterKey(), spec)
        
        return cipher.doFinal(encrypted)
    }
}

/**
 * 安全清零 CharArray
 */
fun CharArray.secureErase() {
    this.fill('\u0000')
}

/**
 * 安全清零 ByteArray
 */
fun ByteArray.secureErase() {
    this.fill(0)
}





