package streams.extensions

import org.neo4j.graphdb.Node
import javax.lang.model.SourceVersion

fun Node.labelNames(): List<String> {
    return this.labels.map { it.name() }
}

fun String.quote(): String = if (SourceVersion.isIdentifier(this)) this else "`$this`"
