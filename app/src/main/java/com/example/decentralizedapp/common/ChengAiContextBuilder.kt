package com.example.decentralizedapp.common

data class ChengAiContext(
    val insights: List<String>,
    val promptContext: String,
    val suggestedTags: List<String>,
    val sourceContentIds: List<String>
) {
    companion object {
        fun empty(): ChengAiContext = ChengAiContext(
            insights = emptyList(),
            promptContext = "",
            suggestedTags = emptyList(),
            sourceContentIds = emptyList()
        )
    }
}

object ChengAiContextBuilder {
    fun build(
        items: Collection<ContentManager.ContentItem>,
        authorId: String,
        limit: Int = 12,
        tagLimit: Int = 8
    ): ChengAiContext {
        val normalizedAuthor = authorId.trim()
        if (normalizedAuthor.isBlank() || items.isEmpty() || limit <= 0 || tagLimit <= 0) {
            return ChengAiContext.empty()
        }

        val candidates = items.asSequence()
            .filter { it.authorId == normalizedAuthor && it.deletedAt == null }
            .filter { hasHighlights(it) }
            .sortedByDescending { it.aiContributionUpdatedAt ?: it.createdAt }
            .toList()

        if (candidates.isEmpty()) {
            return ChengAiContext.empty()
        }

        val insights = LinkedHashSet<String>()
        val sourceContentIds = mutableListOf<String>()
        for (item in candidates) {
            val before = insights.size
            val lines = buildList {
                if (item.aiLearningHighlights.isNotEmpty() ||
                    item.aiKnowledgeHighlights.isNotEmpty() ||
                    item.aiTrainingHighlights.isNotEmpty()
                ) {
                    addAll(item.aiLearningHighlights)
                    addAll(item.aiKnowledgeHighlights)
                    addAll(item.aiTrainingHighlights)
                } else {
                    addAll(item.aiContributionExplanations)
                }
            }
            for (line in lines) {
                val cleaned = line.trim()
                if (cleaned.isNotEmpty()) {
                    insights.add(cleaned)
                }
                if (insights.size >= limit) {
                    break
                }
            }
            if (insights.size > before) {
                sourceContentIds += item.id
            }
            if (insights.size >= limit) {
                break
            }
        }

        val insightList = insights.toList()
        val promptContext = if (insightList.isEmpty()) {
            ""
        } else {
            insightList.joinToString("\n") { "- $it" }
        }

        val suggestedTags = collectSuggestedTags(candidates, tagLimit)

        return ChengAiContext(
            insights = insightList,
            promptContext = promptContext,
            suggestedTags = suggestedTags,
            sourceContentIds = sourceContentIds
        )
    }

    private fun hasHighlights(item: ContentManager.ContentItem): Boolean {
        return item.aiLearningHighlights.isNotEmpty() ||
            item.aiKnowledgeHighlights.isNotEmpty() ||
            item.aiTrainingHighlights.isNotEmpty() ||
            item.aiContributionExplanations.isNotEmpty()
    }

    private fun collectSuggestedTags(
        items: List<ContentManager.ContentItem>,
        tagLimit: Int
    ): List<String> {
        val tagCounts = LinkedHashMap<String, Int>()
        val tagOrder = LinkedHashMap<String, Int>()
        var index = 0
        for (item in items) {
            val candidates = ArrayList<String>()
            candidates.addAll(item.tags)
            candidates.addAll(item.aiSuggestedTags)
            for (tag in candidates) {
                val normalized = tag.trim().trimStart('#')
                if (normalized.isBlank()) continue
                if (!tagCounts.containsKey(normalized)) {
                    tagCounts[normalized] = 0
                    tagOrder[normalized] = index++
                }
                tagCounts[normalized] = (tagCounts[normalized] ?: 0) + 1
            }
        }
        if (tagCounts.isEmpty()) {
            return emptyList()
        }
        return tagCounts.entries
            .sortedWith { a, b ->
                val byCount = b.value.compareTo(a.value)
                if (byCount != 0) return@sortedWith byCount
                val orderA = tagOrder[a.key] ?: 0
                val orderB = tagOrder[b.key] ?: 0
                orderA.compareTo(orderB)
            }
            .take(tagLimit)
            .map { it.key }
    }
}
