package com.example.libp2psmoke.ui.components

import androidx.compose.animation.*
import androidx.compose.animation.core.*
import androidx.compose.foundation.layout.Box
import androidx.compose.runtime.*
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.scale
import androidx.compose.ui.graphics.graphicsLayer

// ═══════════════════════════════════════════════════════════════════
// 动画常量
// ═══════════════════════════════════════════════════════════════════
object DexAnimations {
    const val FAST = 200
    const val NORMAL = 300
    const val SLOW = 500
    
    val DefaultEasing = FastOutSlowInEasing
    val BounceEasing = { fraction: Float ->
        val n1 = 7.5625f
        val d1 = 2.75f
        when {
            fraction < 1f / d1 -> n1 * fraction * fraction
            fraction < 2f / d1 -> {
                val f = fraction - 1.5f / d1
                n1 * f * f + 0.75f
            }
            fraction < 2.5f / d1 -> {
                val f = fraction - 2.25f / d1
                n1 * f * f + 0.9375f
            }
            else -> {
                val f = fraction - 2.625f / d1
                n1 * f * f + 0.984375f
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════
// 入场动画
// ═══════════════════════════════════════════════════════════════════

@Composable
fun FadeInOnMount(
    modifier: Modifier = Modifier,
    delay: Int = 0,
    duration: Int = DexAnimations.NORMAL,
    content: @Composable () -> Unit
) {
    var visible by remember { mutableStateOf(false) }
    
    LaunchedEffect(Unit) {
        kotlinx.coroutines.delay(delay.toLong())
        visible = true
    }
    
    AnimatedVisibility(
        visible = visible,
        enter = fadeIn(animationSpec = tween(duration, easing = DexAnimations.DefaultEasing)),
        modifier = modifier
    ) {
        content()
    }
}

@Composable
fun SlideInFromBottom(
    modifier: Modifier = Modifier,
    delay: Int = 0,
    duration: Int = DexAnimations.NORMAL,
    content: @Composable () -> Unit
) {
    var visible by remember { mutableStateOf(false) }
    
    LaunchedEffect(Unit) {
        kotlinx.coroutines.delay(delay.toLong())
        visible = true
    }
    
    AnimatedVisibility(
        visible = visible,
        enter = slideInVertically(
            initialOffsetY = { it / 2 },
            animationSpec = tween(duration, easing = DexAnimations.DefaultEasing)
        ) + fadeIn(animationSpec = tween(duration)),
        modifier = modifier
    ) {
        content()
    }
}

@Composable
fun ScaleInOnMount(
    modifier: Modifier = Modifier,
    delay: Int = 0,
    duration: Int = DexAnimations.NORMAL,
    initialScale: Float = 0.8f,
    content: @Composable () -> Unit
) {
    var visible by remember { mutableStateOf(false) }
    
    LaunchedEffect(Unit) {
        kotlinx.coroutines.delay(delay.toLong())
        visible = true
    }
    
    AnimatedVisibility(
        visible = visible,
        enter = scaleIn(
            initialScale = initialScale,
            animationSpec = tween(duration, easing = DexAnimations.DefaultEasing)
        ) + fadeIn(animationSpec = tween(duration)),
        modifier = modifier
    ) {
        content()
    }
}

// ═══════════════════════════════════════════════════════════════════
// 交互动画
// ═══════════════════════════════════════════════════════════════════

@Composable
fun PressableScale(
    pressed: Boolean,
    modifier: Modifier = Modifier,
    content: @Composable () -> Unit
) {
    val scale by animateFloatAsState(
        targetValue = if (pressed) 0.95f else 1f,
        animationSpec = spring(
            dampingRatio = Spring.DampingRatioMediumBouncy,
            stiffness = Spring.StiffnessLow
        ),
        label = "pressScale"
    )
    
    Box(modifier = modifier.scale(scale)) {
        content()
    }
}

// ═══════════════════════════════════════════════════════════════════
// 脉冲动画
// ═══════════════════════════════════════════════════════════════════

@Composable
fun PulsingElement(
    modifier: Modifier = Modifier,
    pulseFraction: Float = 0.05f,
    duration: Int = 1500,
    content: @Composable () -> Unit
) {
    val infiniteTransition = rememberInfiniteTransition(label = "pulse")
    val scale by infiniteTransition.animateFloat(
        initialValue = 1f,
        targetValue = 1f + pulseFraction,
        animationSpec = infiniteRepeatable(
            animation = tween(duration, easing = FastOutSlowInEasing),
            repeatMode = RepeatMode.Reverse
        ),
        label = "pulseScale"
    )
    
    Box(modifier = modifier.scale(scale)) {
        content()
    }
}

// ═══════════════════════════════════════════════════════════════════
// 闪烁动画
// ═══════════════════════════════════════════════════════════════════

@Composable
fun ShimmerEffect(
    modifier: Modifier = Modifier,
    content: @Composable () -> Unit
) {
    val infiniteTransition = rememberInfiniteTransition(label = "shimmer")
    val alpha by infiniteTransition.animateFloat(
        initialValue = 0.3f,
        targetValue = 1f,
        animationSpec = infiniteRepeatable(
            animation = tween(1000, easing = LinearEasing),
            repeatMode = RepeatMode.Reverse
        ),
        label = "shimmerAlpha"
    )
    
    Box(modifier = modifier.graphicsLayer { this.alpha = alpha }) {
        content()
    }
}

// ═══════════════════════════════════════════════════════════════════
// 数值动画
// ═══════════════════════════════════════════════════════════════════

@Composable
fun animateIntAsState(
    targetValue: Int,
    animationSpec: AnimationSpec<Int> = tween(DexAnimations.NORMAL)
): State<Int> {
    return animateIntAsState(
        targetValue = targetValue,
        animationSpec = animationSpec,
        label = "intAnimation"
    )
}

// ═══════════════════════════════════════════════════════════════════
// 交错动画入场
// ═══════════════════════════════════════════════════════════════════

@Composable
fun StaggeredAnimationColumn(
    items: List<@Composable () -> Unit>,
    modifier: Modifier = Modifier,
    staggerDelay: Int = 50
) {
    androidx.compose.foundation.layout.Column(modifier = modifier) {
        items.forEachIndexed { index, item ->
            SlideInFromBottom(delay = index * staggerDelay) {
                item()
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════
// 页面过渡规格
// ═══════════════════════════════════════════════════════════════════

fun dexEnterTransition(): EnterTransition {
    return fadeIn(animationSpec = tween(DexAnimations.NORMAL)) + 
           slideInVertically(
               initialOffsetY = { it / 10 },
               animationSpec = tween(DexAnimations.NORMAL)
           )
}

fun dexExitTransition(): ExitTransition {
    return fadeOut(animationSpec = tween(DexAnimations.FAST)) + 
           slideOutVertically(
               targetOffsetY = { -it / 10 },
               animationSpec = tween(DexAnimations.FAST)
           )
}

fun dexPopEnterTransition(): EnterTransition {
    return fadeIn(animationSpec = tween(DexAnimations.FAST)) + 
           slideInVertically(
               initialOffsetY = { -it / 10 },
               animationSpec = tween(DexAnimations.FAST)
           )
}

fun dexPopExitTransition(): ExitTransition {
    return fadeOut(animationSpec = tween(DexAnimations.NORMAL)) + 
           slideOutVertically(
               targetOffsetY = { it / 10 },
               animationSpec = tween(DexAnimations.NORMAL)
           )
}





