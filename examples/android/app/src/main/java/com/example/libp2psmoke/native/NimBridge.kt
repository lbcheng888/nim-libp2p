package com.example.libp2psmoke.native

import android.util.Log

object NimBridge {
    private const val LIB_CRYPTO_COMPAT = "crypto_compat"
    private const val LIB_NIM = "nimlibp2p"
    private const val LIB_BRIDGE = "nimbridge"
    private var librariesLoaded = false

    @Synchronized
    private fun ensureLoaded() {
        if (librariesLoaded) return
        try {
            System.loadLibrary("c++_shared")
        } catch (_: UnsatisfiedLinkError) {
            // c++_shared is bundled by the NDK; ignore when already loaded.
        }
        try {
            System.loadLibrary(LIB_CRYPTO_COMPAT)
        } catch (_: UnsatisfiedLinkError) {
            // Compatibility shim is optional when running against a patched OpenSSL.
        }
        System.loadLibrary(LIB_BRIDGE)
        System.loadLibrary(LIB_NIM)
        librariesLoaded = true
    }

    fun initNode(configJson: String): Long {
        ensureLoaded()
        return nativeInit(configJson)
    }

    fun startNode(handle: Long): Int {
        ensureLoaded()
        return nativeStart(handle)
    }

    fun stopNode(handle: Long): Int = nativeStop(handle)

    fun freeNode(handle: Long) = nativeFree(handle)

    fun isStarted(handle: Long): Boolean = nativeIsStarted(handle)

    fun pollEvents(handle: Long, maxEvents: Int = 128): String? =
        nativePollEvents(handle, maxEvents)

    fun lanEndpoints(handle: Long): String? = nativeGetLanEndpoints(handle)

    fun listenAddresses(handle: Long): String? = nativeGetListenAddresses(handle)

    fun dialableAddresses(handle: Long): String? = nativeGetDialableAddresses(handle)

    fun connectedPeers(handle: Long): String? = nativeGetConnectedPeers(handle)

    fun connectedPeersInfo(handle: Long): String? = nativeGetConnectedPeersInfo(handle)

    fun localPeerId(handle: Long): String? {
        val value = nativeGetLocalPeerId(handle)
        Log.d("NimBridge", "localPeerId(handle=$handle) -> $value")
        return value
    }

    fun setMdnsEnabled(handle: Long, enabled: Boolean): Boolean =
        nativeSetMdnsEnabled(handle, enabled)

    fun setMdnsInterface(handle: Long, ipv4: String): Boolean =
        nativeSetMdnsInterface(handle, ipv4)

    fun mdnsProbe(handle: Long): Boolean = nativeMdnsProbe(handle)

    fun initializeConnectionEvents(handle: Long): Boolean =
        nativeInitializeConnEvents(handle)

    fun connectPeer(handle: Long, peerId: String): Int =
        nativeConnectPeer(handle, peerId)

    fun connectMultiaddr(handle: Long, multiaddr: String): Int =
        nativeConnectMultiaddr(handle, multiaddr)

    fun sendDirect(handle: Long, peerId: String, payload: ByteArray): Boolean =
        nativeSendDirect(handle, peerId, payload)

    fun publishFeedEntry(handle: Long, jsonPayload: String): Boolean =
        nativePublishFeed(handle, jsonPayload)

    fun fetchFeedSnapshot(handle: Long): String? =
        nativeFetchFeedSnapshot(handle)

    fun registerPeerHints(handle: Long, peerId: String, addressesJson: String, source: String? = null): Boolean =
        nativeRegisterPeerHints(handle, peerId, addressesJson, source)

    fun addExternalAddress(handle: Long, multiaddr: String): Boolean =
        nativeAddExternalAddress(handle, multiaddr)

    fun isPeerConnected(handle: Long, peerId: String): Boolean =
        nativeIsPeerConnected(handle, peerId)

    fun upsertLivestream(handle: Long, streamKey: String, configJson: String): Boolean =
        nativeUpsertLivestream(handle, streamKey, configJson)

    fun publishLivestreamFrame(handle: Long, streamKey: String, payload: ByteArray): Boolean =
        nativePublishLivestreamFrame(handle, streamKey, payload)

    fun submitBtcSwap(
        handle: Long,
        orderId: String,
        rpcUrl: String,
        username: String?,
        password: String?,
        amountSats: Long,
        targetAddress: String
    ): Boolean =
        nativeSubmitBtcSwap(handle, orderId, rpcUrl, username, password, amountSats, targetAddress)

    fun submitBscUsdcTransfer(
        handle: Long,
        orderId: String,
        rpcUrl: String,
        contract: String,
        privateKey: String,
        toAddress: String,
        amount: String
    ): Boolean =
        nativeSubmitBscTransfer(handle, orderId, rpcUrl, contract, privateKey, toAddress, amount)

    fun lastDirectError(handle: Long): String? = nativeGetLastDirectError(handle)

    fun lastInitError(): String? = nativeGetLastInitError()

    fun identityFromSeed(seed: ByteArray): String? {
        ensureLoaded()
        return nativeIdentityFromSeed(seed)
    }

    private external fun nativeInit(configJson: String?): Long
    private external fun nativeStart(handle: Long): Int
    private external fun nativeStop(handle: Long): Int
    private external fun nativeFree(handle: Long)
    private external fun nativeIsStarted(handle: Long): Boolean
    private external fun nativePollEvents(handle: Long, maxEvents: Int): String?
    private external fun nativeGetLanEndpoints(handle: Long): String?
    private external fun nativeGetListenAddresses(handle: Long): String?
    private external fun nativeGetDialableAddresses(handle: Long): String?
    private external fun nativeGetConnectedPeers(handle: Long): String?
    private external fun nativeGetConnectedPeersInfo(handle: Long): String?
    private external fun nativeGetLocalPeerId(handle: Long): String?
    private external fun nativeSetMdnsEnabled(handle: Long, enabled: Boolean): Boolean
    private external fun nativeSetMdnsInterface(handle: Long, ipv4: String?): Boolean
    private external fun nativeMdnsProbe(handle: Long): Boolean
    private external fun nativeInitializeConnEvents(handle: Long): Boolean
    private external fun nativeConnectPeer(handle: Long, peerId: String?): Int
    private external fun nativeConnectMultiaddr(handle: Long, multiaddr: String?): Int
    private external fun nativeSendDirect(handle: Long, peerId: String?, payload: ByteArray?): Boolean
    private external fun nativePublishFeed(handle: Long, jsonPayload: String?): Boolean
    private external fun nativeFetchFeedSnapshot(handle: Long): String?
    private external fun nativeUpsertLivestream(handle: Long, streamKey: String?, configJson: String?): Boolean
    private external fun nativePublishLivestreamFrame(handle: Long, streamKey: String?, payload: ByteArray?): Boolean
    private external fun nativeGetLastDirectError(handle: Long): String?
    private external fun nativeGetLastInitError(): String?
    private external fun nativeIdentityFromSeed(seed: ByteArray?): String?
    private external fun nativeRegisterPeerHints(handle: Long, peerId: String?, addressesJson: String?, source: String?): Boolean
    private external fun nativeAddExternalAddress(handle: Long, multiaddr: String?): Boolean
    private external fun nativeIsPeerConnected(handle: Long, peerId: String?): Boolean
    private external fun nativeSubmitBtcSwap(handle: Long, orderId: String?, rpcUrl: String?, username: String?, password: String?, amountSats: Long, address: String?): Boolean
    private external fun nativeSubmitBscTransfer(handle: Long, orderId: String?, rpcUrl: String?, contract: String?, privateKey: String?, toAddress: String?, amount: String?): Boolean
}
