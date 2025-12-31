package com.example.libp2psmoke.core

object MultiaddrListParser {
    /**
     * Accepts either:
     * - a JSON array: `["/ip4/1.2.3.4/tcp/9001/p2p/12D3...", "..."]`
     * - or a whitespace/comma separated list.
     */
    fun parse(raw: String?): List<String> {
        val trimmed = raw?.trim().orEmpty()
        if (trimmed.isEmpty()) return emptyList()
        if (trimmed.startsWith("[")) {
            val parsed = parseJsonStringArray(trimmed)
            if (parsed != null) {
                return parsed.map { it.trim() }.filter { it.isNotEmpty() }
            }
        }
        return trimmed
            .split(',', '\n', '\t', ' ')
            .map { it.trim() }
            .filter { it.isNotEmpty() }
    }

    private fun parseJsonStringArray(raw: String): List<String>? {
        val s = raw.trim()
        if (!s.startsWith("[") || !s.endsWith("]")) return null
        val out = mutableListOf<String>()
        var i = 1

        fun skipSeparators() {
            while (i < s.length - 1) {
                val ch = s[i]
                if (ch.isWhitespace() || ch == ',') {
                    i++
                    continue
                }
                break
            }
        }

        while (i < s.length - 1) {
            skipSeparators()
            if (i >= s.length - 1) break
            if (s[i] == ']') break
            if (s[i] != '"') return null
            i++

            val sb = StringBuilder()
            var closed = false
            while (i < s.length) {
                val ch = s[i++]
                when (ch) {
                    '\\' -> {
                        if (i >= s.length) return null
                        val esc = s[i++]
                        when (esc) {
                            '"', '\\', '/' -> sb.append(esc)
                            'b' -> sb.append('\b')
                            'f' -> sb.append('\u000C')
                            'n' -> sb.append('\n')
                            'r' -> sb.append('\r')
                            't' -> sb.append('\t')
                            'u' -> {
                                if (i + 4 > s.length) return null
                                val hex = s.substring(i, i + 4)
                                val code = hex.toIntOrNull(16) ?: return null
                                sb.append(code.toChar())
                                i += 4
                            }
                            else -> sb.append(esc)
                        }
                    }
                    '"' -> {
                        closed = true
                        break
                    }
                    else -> sb.append(ch)
                }
            }
            if (!closed) return null
            out.add(sb.toString())
        }

        return out
    }
}
