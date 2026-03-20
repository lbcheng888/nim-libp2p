package com.example.libp2psmoke.ui

import android.view.Choreographer

class NimUiFrameClock(
    private val onFrame: (frameTimeNanos: Long, deltaNanos: Long) -> Unit
) : Choreographer.FrameCallback {
    private val choreographer = Choreographer.getInstance()
    private var running = false
    private var lastFrameTimeNanos = 0L

    fun start() {
        if (running) return
        running = true
        lastFrameTimeNanos = 0L
        choreographer.postFrameCallback(this)
    }

    fun stop() {
        if (!running) return
        running = false
        lastFrameTimeNanos = 0L
        choreographer.removeFrameCallback(this)
    }

    override fun doFrame(frameTimeNanos: Long) {
        if (!running) return
        val deltaNanos =
            if (lastFrameTimeNanos <= 0L) {
                0L
            } else {
                (frameTimeNanos - lastFrameTimeNanos)
                    .coerceAtLeast(MinFrameDeltaNanos)
                    .coerceAtMost(MaxFrameDeltaNanos)
            }
        lastFrameTimeNanos = frameTimeNanos
        onFrame(frameTimeNanos, deltaNanos)
        if (running) {
            choreographer.postFrameCallback(this)
        }
    }

    private companion object {
        const val MinFrameDeltaNanos = 4_166_666L
        const val MaxFrameDeltaNanos = 66_666_666L
    }
}
