package com.example.libp2psmoke.core

import com.example.libp2psmoke.model.QuicRuntimePreferenceOption
import com.example.libp2psmoke.model.QuicRuntimeStatus
import org.json.JSONObject

data class RequestedQuicRuntimeConfig(
    val preferenceWireValue: String,
    val libraryPath: String?
)

object QuicRuntimeJson {
    fun requestedConfig(
        preference: QuicRuntimePreferenceOption,
        libraryPath: String
    ): RequestedQuicRuntimeConfig {
        val trimmedPath = libraryPath.trim().ifEmpty { null }
        return RequestedQuicRuntimeConfig(
            preferenceWireValue = preference.wireValue,
            libraryPath = trimmedPath
        )
    }

    fun applyRequestedConfig(
        extra: JSONObject,
        preference: QuicRuntimePreferenceOption,
        libraryPath: String
    ) {
        val config = requestedConfig(preference, libraryPath)
        extra.put("quicRuntimePreference", config.preferenceWireValue)
        if (config.libraryPath != null) {
            extra.put("quicRuntimeLibraryPath", config.libraryPath)
        } else {
            extra.remove("quicRuntimeLibraryPath")
        }
    }

    fun parseStatusFields(
        fields: Map<String, String?>,
        requestedPreference: QuicRuntimePreferenceOption,
        requestedLibraryPath: String,
        pureNim: Boolean = false,
        loaded: Boolean = false
    ): QuicRuntimeStatus {
        val resolvedPreference =
            QuicRuntimePreferenceOption.fromStoredValue(fields["requestedPreference"])
                ?: requestedPreference
        val resolvedLibraryPath =
            fields["requestedLibraryPath"]
                ?.takeIf { !it.isNullOrBlank() }
                ?: requestedLibraryPath.trim()
        val activeKind = fields["kind"].orEmpty().ifBlank { "unavailable" }
        val implementation = fields["implementation"].orEmpty().ifBlank { activeKind }
        return QuicRuntimeStatus(
            requestedPreference = resolvedPreference,
            requestedLibraryPath = resolvedLibraryPath,
            activeKind = activeKind,
            implementation = implementation,
            activePath = fields["path"].orEmpty().trim(),
            pureNim = pureNim,
            loaded = loaded
        )
    }

    fun parseStatus(
        diagnostics: JSONObject?,
        requestedPreference: QuicRuntimePreferenceOption,
        requestedLibraryPath: String
    ): QuicRuntimeStatus {
        val runtime = diagnostics?.optJSONObject("quicRuntime")
        if (runtime == null) {
            return parseStatusFields(
                fields = emptyMap(),
                requestedPreference = requestedPreference,
                requestedLibraryPath = requestedLibraryPath
            )
        }
        return parseStatusFields(
            fields = mapOf(
                "requestedPreference" to runtime.optString("requestedPreference"),
                "requestedLibraryPath" to runtime.optString("requestedLibraryPath"),
                "kind" to runtime.optString("kind"),
                "implementation" to runtime.optString("implementation"),
                "path" to runtime.optString("path")
            ),
            requestedPreference = requestedPreference,
            requestedLibraryPath = requestedLibraryPath,
            pureNim = runtime.optBoolean("pureNim", false),
            loaded = runtime.optBoolean("loaded", false)
        )
    }
}
