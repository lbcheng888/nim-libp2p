package com.example.libp2psmoke.core

import org.junit.Assert.assertEquals
import org.junit.Test

class MultiaddrListParserTest {
    @Test
    fun parsesEmpty() {
        assertEquals(emptyList<String>(), MultiaddrListParser.parse(""))
        assertEquals(emptyList<String>(), MultiaddrListParser.parse("   "))
        assertEquals(emptyList<String>(), MultiaddrListParser.parse(null))
    }

    @Test
    fun parsesJsonArray() {
        val raw =
            """
            [
              " /ip4/1.2.3.4/tcp/9001/p2p/12D3KooW...",
              "/ip4/5.6.7.8/tcp/9001/p2p/12D3KooW..."
            ]
            """.trimIndent()
        assertEquals(
            listOf(
                "/ip4/1.2.3.4/tcp/9001/p2p/12D3KooW...",
                "/ip4/5.6.7.8/tcp/9001/p2p/12D3KooW..."
            ),
            MultiaddrListParser.parse(raw)
        )
    }

    @Test
    fun parsesDelimitedList() {
        val raw = " /dnsaddr/bootstrap.libp2p.io,  /ip4/1.2.3.4/tcp/9001/p2p/12D3KooW...\n/ip4/5.6.7.8/tcp/9001 "
        assertEquals(
            listOf(
                "/dnsaddr/bootstrap.libp2p.io",
                "/ip4/1.2.3.4/tcp/9001/p2p/12D3KooW...",
                "/ip4/5.6.7.8/tcp/9001"
            ),
            MultiaddrListParser.parse(raw)
        )
    }
}

