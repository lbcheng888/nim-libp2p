package com.example.libp2psmoke.core

import java.math.BigDecimal
import java.math.BigInteger
import java.util.concurrent.TimeUnit

/**
 * 应用级常量定义
 * 集中管理所有魔法数字和配置值
 */
object Constants {
    
    // ═══════════════════════════════════════════════════════════════════
    // 网络配置
    // ═══════════════════════════════════════════════════════════════════
    
    object Network {
        /** HTTP 连接超时 (秒) */
        const val CONNECT_TIMEOUT_SECONDS = 15L
        
        /** HTTP 读取超时 (秒) */
        const val READ_TIMEOUT_SECONDS = 30L
        
        /** HTTP 写入超时 (秒) */
        const val WRITE_TIMEOUT_SECONDS = 30L
        
        /** WebSocket Ping 间隔 (秒) */
        const val WS_PING_INTERVAL_SECONDS = 20L
        
        /** 最大重试次数 */
        const val MAX_RETRY_COUNT = 3
        
        /** 初始重连延迟 (毫秒) */
        const val INITIAL_RECONNECT_DELAY_MS = 1000L
        
        /** 最大重连延迟 (毫秒) */
        const val MAX_RECONNECT_DELAY_MS = 30000L
        
        /** 请求超时 (毫秒) */
        const val REQUEST_TIMEOUT_MS = 30000L
    }
    
    // ═══════════════════════════════════════════════════════════════════
    // BSC/EVM 链配置
    // ═══════════════════════════════════════════════════════════════════
    
    object Bsc {
        /** 默认 Gas Limit */
        val DEFAULT_GAS_LIMIT: BigInteger = BigInteger.valueOf(120_000L)
        
        /** 最小 Gas Price (1 Gwei) */
        val MIN_GAS_PRICE: BigInteger = BigInteger.valueOf(1_000_000_000L)
        
        /** USDC 代币精度 */
        const val USDC_DECIMALS = 18
        
        /** BSC 测试网 Chain ID */
        const val TESTNET_CHAIN_ID = 97L
        
        /** 交易确认等待次数 */
        const val TX_CONFIRMATION_ATTEMPTS = 12
        
        /** 每次确认等待间隔 (毫秒) */
        const val TX_CONFIRMATION_INTERVAL_MS = 1500L
        
        /** 私钥长度 (Hex 字符) */
        const val PRIVATE_KEY_HEX_LENGTH = 64

        /** BSC 测试网 RPC 节点列表 */
        val TESTNET_RPC_URLS = listOf(
            "https://bsc-testnet.publicnode.com",
            "https://bsc-testnet.public.blastapi.io",
            "https://data-seed-prebsc-1-s1.binance.org:8545",
            "https://data-seed-prebsc-2-s1.binance.org:8545",
            "https://data-seed-prebsc-1-s2.binance.org:8545",
            "https://data-seed-prebsc-2-s2.binance.org:8545",
            "https://data-seed-prebsc-1-s3.binance.org:8545",
            "https://data-seed-prebsc-2-s3.binance.org:8545"
        )
    }
    
    // ═══════════════════════════════════════════════════════════════════
    // BTC 配置
    // ═══════════════════════════════════════════════════════════════════
    
    object Btc {
        /** Dust Limit (聪) - 低于此值的输出会被节点拒绝 */
        const val DUST_LIMIT_SATS = 546L
        
        /** 每个输入的预估手续费 (聪) */
        const val FEE_PER_INPUT_SATS = 1000L
        
        /** 1 BTC = 10^8 聪 */
        const val SATS_PER_BTC = 100_000_000L
        
        /** 默认参考价格 (用于估算) */
        val REFERENCE_PRICE_USD: BigDecimal = BigDecimal("68000")
        
        /** BTC 精度 (小数位) */
        const val DECIMAL_PLACES = 8
    }
    
    // ═══════════════════════════════════════════════════════════════════
    // 缓存配置
    // ═══════════════════════════════════════════════════════════════════
    
    object Cache {
        /** K线缓存最大条目数 */
        const val MAX_KLINE_ENTRIES = 10
        
        /** K线缓存过期时间 (分钟) */
        const val KLINE_EXPIRE_MINUTES = 5L
        
        /** 最大 K线数据点数 */
        const val MAX_KLINE_DATA_POINTS = 720
        
        /** 订单簿缓存深度 */
        const val ORDER_BOOK_DEPTH = 20
        
        /** Partial Fill 过期时间 (分钟) */
        const val PARTIAL_FILL_EXPIRE_MINUTES = 30L
    }
    
    // ═══════════════════════════════════════════════════════════════════
    // UI 配置
    // ═══════════════════════════════════════════════════════════════════
    
    object Ui {
        /** 最大消息数/每个 Peer */
        const val MAX_MESSAGES_PER_PEER = 200
        
        /** 最大 Feed 条目数 */
        const val MAX_FEED_ITEMS = 128
        
        /** 最大直播帧数 */
        const val MAX_LIVE_FRAMES = 128
        
        /** 最大交易记录数 */
        const val MAX_SWAP_HISTORY = 40
        
        /** 深度刷新间隔 (毫秒) */
        const val DEPTH_REFRESH_INTERVAL_MS = 15000L
    }
    
    // ═══════════════════════════════════════════════════════════════════
    // P2P 配置
    // ═══════════════════════════════════════════════════════════════════
    
    object P2P {
        /** 最大入站连接数 */
        const val MAX_INBOUND_CONNECTIONS = 16
        
        /** 最大出站连接数 */
        const val MAX_OUTBOUND_CONNECTIONS = 16
        
        /** mDNS 探测超时 (毫秒) */
        const val MDNS_PROBE_TIMEOUT_MS = 5000L
        
        /** 连接尝试冷却时间 (毫秒) */
        const val CONNECTION_COOLDOWN_MS = 30000L
    }
    
    // ═══════════════════════════════════════════════════════════════════
    // DEX 订单配置
    // ═══════════════════════════════════════════════════════════════════
    
    object Dex {
        /** 默认交易对 */
        const val DEFAULT_SYMBOL = "BTC-USDC"
        
        /** 订单默认 TTL (毫秒) - 1小时 */
        const val ORDER_TTL_MS = 3600000L
        
        /** 订单签名版本 */
        const val SIGNATURE_VERSION = 1
        
        /** 最小交易金额 (USDC) */
        val MIN_TRADE_AMOUNT_USDC: BigDecimal = BigDecimal("10")
        
        /** 默认滑点容忍度 */
        val DEFAULT_SLIPPAGE: BigDecimal = BigDecimal("0.005") // 0.5%
    }
    
    // ═══════════════════════════════════════════════════════════════════
    // Mixer 配置
    // ═══════════════════════════════════════════════════════════════════
    
    object Mixer {
        /** 默认 Onion 跳数 */
        const val DEFAULT_HOP_COUNT = 3
        
        /** 最小跳数 */
        const val MIN_HOP_COUNT = 1
        
        /** 最大跳数 */
        const val MAX_HOP_COUNT = 5
        
        /** 最小参与者数量 */
        const val MIN_PARTICIPANTS = 3
    }
}



