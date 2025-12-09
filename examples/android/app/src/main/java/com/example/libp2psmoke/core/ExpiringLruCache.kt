package com.example.libp2psmoke.core

import android.os.SystemClock
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * 带过期时间的 LRU 缓存
 * 线程安全，支持自动过期清理
 *
 * @param maxSize 最大条目数
 * @param expireAfterMs 过期时间 (毫秒)，-1 表示永不过期
 */
class ExpiringLruCache<K, V>(
    private val maxSize: Int,
    private val expireAfterMs: Long = -1
) {
    
    private data class CacheEntry<V>(
        val value: V,
        val createdAt: Long = SystemClock.elapsedRealtime(),
        var lastAccessedAt: Long = createdAt
    ) {
        fun isExpired(expireAfterMs: Long): Boolean {
            if (expireAfterMs < 0) return false
            return SystemClock.elapsedRealtime() - createdAt > expireAfterMs
        }
    }
    
    private val cache = LinkedHashMap<K, CacheEntry<V>>(maxSize, 0.75f, true)
    private val lock = ReentrantReadWriteLock()
    
    /** 当前缓存大小 */
    val size: Int get() = lock.read { cache.size }
    
    /**
     * 放入缓存
     */
    fun put(key: K, value: V) {
        lock.write {
            // 先清理过期条目
            cleanupExpired()
            
            // 移除旧值
            cache.remove(key)
            
            // 如果已满，移除最旧的条目
            while (cache.size >= maxSize) {
                val oldest = cache.entries.firstOrNull() ?: break
                cache.remove(oldest.key)
            }
            
            cache[key] = CacheEntry(value)
        }
    }
    
    /**
     * 获取缓存值
     * @return 值，如果不存在或已过期则返回 null
     */
    fun get(key: K): V? {
        return lock.read {
            val entry = cache[key] ?: return@read null
            
            // 检查是否过期
            if (entry.isExpired(expireAfterMs)) {
                // 升级为写锁移除
                lock.read { } // 释放读锁
                lock.write { cache.remove(key) }
                return@read null
            }
            
            entry.lastAccessedAt = SystemClock.elapsedRealtime()
            entry.value
        }
    }
    
    /**
     * 获取或计算值
     */
    fun getOrPut(key: K, defaultValue: () -> V): V {
        get(key)?.let { return it }
        
        val value = defaultValue()
        put(key, value)
        return value
    }
    
    /**
     * 移除缓存
     */
    fun remove(key: K): V? {
        return lock.write {
            cache.remove(key)?.value
        }
    }
    
    /**
     * 检查是否包含 key
     */
    fun contains(key: K): Boolean {
        return get(key) != null
    }
    
    /**
     * 清空缓存
     */
    fun clear() {
        lock.write {
            cache.clear()
        }
    }
    
    /**
     * 获取所有未过期的 key
     */
    fun keys(): Set<K> {
        return lock.read {
            cache.filterValues { !it.isExpired(expireAfterMs) }.keys.toSet()
        }
    }
    
    /**
     * 获取所有未过期的值
     */
    fun values(): List<V> {
        return lock.read {
            cache.values
                .filter { !it.isExpired(expireAfterMs) }
                .map { it.value }
        }
    }
    
    /**
     * 手动触发过期清理
     */
    fun cleanup() {
        lock.write {
            cleanupExpired()
        }
    }
    
    /**
     * 获取缓存统计信息
     */
    fun stats(): CacheStats {
        return lock.read {
            val now = SystemClock.elapsedRealtime()
            val entries = cache.values.toList()
            val expired = entries.count { it.isExpired(expireAfterMs) }
            val oldest = entries.minOfOrNull { it.createdAt }
            val newest = entries.maxOfOrNull { it.createdAt }
            
            CacheStats(
                size = entries.size,
                maxSize = maxSize,
                expiredCount = expired,
                oldestAgeMs = oldest?.let { now - it },
                newestAgeMs = newest?.let { now - it }
            )
        }
    }
    
    private fun cleanupExpired() {
        if (expireAfterMs < 0) return
        
        val iterator = cache.entries.iterator()
        while (iterator.hasNext()) {
            val entry = iterator.next()
            if (entry.value.isExpired(expireAfterMs)) {
                iterator.remove()
            }
        }
    }
    
    data class CacheStats(
        val size: Int,
        val maxSize: Int,
        val expiredCount: Int,
        val oldestAgeMs: Long?,
        val newestAgeMs: Long?
    ) {
        val utilizationPercent: Float
            get() = if (maxSize > 0) (size.toFloat() / maxSize) * 100 else 0f
    }
}

/**
 * 简化版：仅 LRU，无过期
 */
fun <K, V> lruCache(maxSize: Int): ExpiringLruCache<K, V> = ExpiringLruCache(maxSize, -1)

/**
 * 创建带过期的缓存
 */
fun <K, V> expiringCache(
    maxSize: Int,
    expireAfterMinutes: Long
): ExpiringLruCache<K, V> = ExpiringLruCache(maxSize, expireAfterMinutes * 60 * 1000)

