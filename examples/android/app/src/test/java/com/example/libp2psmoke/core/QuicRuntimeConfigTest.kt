package com.example.libp2psmoke.core

import com.example.libp2psmoke.model.QuicRuntimePreferenceOption
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class QuicRuntimeConfigTest {
    @Test
    fun `requestedConfig writes runtime preference and path`() {
        val config = QuicRuntimeJson.requestedConfig(
            preference = QuicRuntimePreferenceOption.BUILTIN_ONLY,
            libraryPath = " /data/local/tmp/libmsquic.so "
        )

        assertEquals("builtin_only", config.preferenceWireValue)
        assertEquals("/data/local/tmp/libmsquic.so", config.libraryPath)
    }

    @Test
    fun `requestedConfig removes empty runtime path`() {
        val config = QuicRuntimeJson.requestedConfig(
            preference = QuicRuntimePreferenceOption.AUTO,
            libraryPath = "   "
        )

        assertEquals("auto", config.preferenceWireValue)
        assertNull(config.libraryPath)
    }

    @Test
    fun `parseStatusFields prefers diagnostics payload`() {
        val status = QuicRuntimeJson.parseStatusFields(
            fields = mapOf(
                "kind" to "builtin",
                "implementation" to "nim-quic",
                "path" to "",
                "requestedPreference" to "builtin_preferred",
                "requestedLibraryPath" to "/vendor/lib/libmsquic.so"
            ),
            requestedPreference = QuicRuntimePreferenceOption.NATIVE_ONLY,
            requestedLibraryPath = "",
            pureNim = true,
            loaded = true
        )

        assertEquals(QuicRuntimePreferenceOption.BUILTIN_PREFERRED, status.requestedPreference)
        assertEquals("/vendor/lib/libmsquic.so", status.requestedLibraryPath)
        assertEquals("builtin", status.activeKind)
        assertEquals("nim-quic", status.implementation)
        assertTrue(status.pureNim)
        assertTrue(status.loaded)
    }

    @Test
    fun `parseStatusFields falls back to requested config when diagnostics missing`() {
        val status = QuicRuntimeJson.parseStatusFields(
            fields = emptyMap(),
            requestedPreference = QuicRuntimePreferenceOption.BUILTIN_ONLY,
            requestedLibraryPath = "/tmp/libmsquic.so"
        )

        assertEquals(QuicRuntimePreferenceOption.BUILTIN_ONLY, status.requestedPreference)
        assertEquals("/tmp/libmsquic.so", status.requestedLibraryPath)
        assertEquals("unavailable", status.activeKind)
        assertTrue(!status.loaded)
    }
}
