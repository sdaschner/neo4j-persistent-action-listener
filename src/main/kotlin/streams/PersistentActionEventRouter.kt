package streams

import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.logging.Log
import org.neo4j.logging.internal.LogService
import streams.events.NodePayload
import streams.events.OperationType.created
import streams.events.RelationshipPayload
import streams.events.StreamsEvent
import streams.events.StreamsTransactionEvent
import streams.extensions.quote
import streams.serialization.JSONUtils

class PersistentActionEventRouter(logService: LogService) {

    private val ignoredLabels: List<String> = listOf("Action")

    private val log: Log = logService.getUserLog(PersistentActionEventRouter::class.java)

    fun start() {
        log.info("Initialising persistent router")
    }

    fun stop() {
        log.info("Stopping persistent router")
    }

    fun sendEvents(transactionEvents: List<StreamsEvent>, db: GraphDatabaseService) {
        sendTxEvents(transactionEvents.filterIsInstance<StreamsTransactionEvent>(), db)
    }

    private fun sendTxEvents(events: List<StreamsTransactionEvent>, db: GraphDatabaseService) {
        val actionEvent = events.find { isCreateActionEvent(it) }
        if (actionEvent != null) {
            val actionId = actionEvent.payload.after?.properties.orEmpty()["actionId"] as String?
            if (actionId != null)
                persistChangeJson(events, actionEvent, actionId, db)
        }
    }

    private fun persistChangeJson(events: List<StreamsTransactionEvent>, actionEvent: StreamsTransactionEvent, actionId: String, db: GraphDatabaseService) {
        val json = JSONUtils.writeValueAsString(events.filter { !isIgnoredEvent(it) })
        val labelString = getLabelsAsString((actionEvent.payload as NodePayload).after?.labels.orEmpty())
        db.executeTransactionally("MERGE (n$labelString {actionId: \$actionId}) SET n.json = \$json", mapOf("actionId" to actionId, "json" to json))
    }

    private fun isCreateActionEvent(it: StreamsTransactionEvent) = it.meta.operation == created && it.payload is NodePayload && it.payload.after?.labels.orEmpty().contains("Action")

    private fun isIgnoredEvent(event: StreamsTransactionEvent) =
            when (event.payload) {
                is NodePayload -> nodeContainsIgnoredLabels(event.payload)
                is RelationshipPayload -> relNodesContainIgnoredLabels(event.payload)
                else -> false
            };

    private fun relNodesContainIgnoredLabels(payload: RelationshipPayload) = (payload.start.labels.orEmpty().any(ignoredLabels::contains)
            || payload.end.labels.orEmpty().any(ignoredLabels::contains))

    private fun nodeContainsIgnoredLabels(payload: NodePayload) = (payload.before?.labels.orEmpty().any(ignoredLabels::contains)
            || payload.after?.labels.orEmpty().any(ignoredLabels::contains))

    private fun getLabelsAsString(labels: Collection<String>): String = labels
            .joinToString(":") { it.quote() }
            .let { if (it.isNotBlank()) ":$it" else it }

}
