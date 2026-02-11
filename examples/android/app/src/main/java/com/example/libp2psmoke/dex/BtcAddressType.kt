package com.example.libp2psmoke.dex

/**
 * BTC 地址类型，用于控制签名与脚本格式
 */
enum class BtcAddressType {
    /** 传统 P2PKH，地址以 1 / m 开头 */
    P2PKH,
    /** 原生 SegWit P2WPKH，bech32 地址以 bc1 / tb1 开头 */
    P2WPKH;

    val isSegwit: Boolean
        get() = this == P2WPKH
}
