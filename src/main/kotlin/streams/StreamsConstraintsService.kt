package streams

import kotlinx.coroutines.*
import org.neo4j.graphdb.DatabaseShutdownException
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType
import streams.events.Constraint
import streams.utils.SystemUtils
import java.io.Closeable
import java.util.*
import java.util.concurrent.ConcurrentHashMap

class StreamsConstraintsService(private val db: GraphDatabaseService) : Closeable {

    private val schemaPollingInterval = 300000L;

    private val nodeConstraints = ConcurrentHashMap<String, Set<Constraint>>()
    private val relConstraints = ConcurrentHashMap<String, Set<Constraint>>()

    private lateinit var job: Job

    override fun close() {
        SystemUtils.ignoreExceptions({ runBlocking { job.cancelAndJoin() } }, UninitializedPropertyAccessException::class.java)
    }

    fun start() {
        job = GlobalScope.launch(Dispatchers.IO) {
            while (isActive) {
                if (!db.isAvailable(5000)) return@launch
                SystemUtils.ignoreExceptions({
                    db.beginTx().use {
                        val constraints = it.schema().constraints
                        constraints
                                .filter { it.isNodeConstraint() }
                                .groupBy { it.label.name() }
                                .forEach { label, c ->
                                    nodeConstraints[label] = c
                                            .map { Constraint(label, it.propertyKeys.toSet(), it.streamsConstraintType()) }
                                            .toSet()
                                }
                        constraints
                                .filter { it.isRelationshipConstraint() }
                                .groupBy { it.relationshipType.name() }
                                .forEach { relationshipType, c ->
                                    relConstraints[relationshipType] = c
                                            .map { Constraint(relationshipType, it.propertyKeys.toSet(), it.streamsConstraintType()) }
                                            .toSet()
                                }
                    }
                }, DatabaseShutdownException::class.java)
                delay(schemaPollingInterval)
            }
        }
    }

    fun forLabel(label: Label): Set<Constraint> {
        return nodeConstraints[label.name()] ?: emptySet()
    }

    fun forRelationshipType(relationshipType: RelationshipType): Set<Constraint> {
        return relConstraints[relationshipType.name()] ?: emptySet()
    }

    fun allForLabels(): Map<String, Set<Constraint>> {
        return Collections.unmodifiableMap(nodeConstraints)
    }

    fun allForRelationshipType(): Map<String, Set<Constraint>> {
        return Collections.unmodifiableMap(relConstraints)
    }


}