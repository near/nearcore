// This Kotlin/Native utility takes /proc/<pid>/maps file to stdin and prints regions
// in the descending sorted order.
import kotlin.text.Regex
import platform.posix.*
import kotlinx.cinterop.*

fun humanReadable(value: ULong) = when {
    value > 1_000_000_000UL -> (value / (1024UL * 1024UL)).toString() + "MB"
    value > 1_000_000UL -> (value / 1024UL).toString() + "KB"
    else -> value.toString()
}

fun main() = memScoped {
    val regex = Regex("^([0123456789abcdef]+)-([0123456789abcdef]+).*")
    val bufferLength = 64 * 1024
    val buffer = allocArray<ByteVar>(bufferLength)
    val lines = mutableMapOf<String, ULong>()

    while (true) {
        val line = fgets(buffer, bufferLength, stdin)?.toKString()?.trim()
        if (line == null) break
        regex.matchEntire(line) ?.let {
            val start = it.groups[1]!!.value.toULong(16)
            val end = it.groups[2]!!.value.toULong(16)
            lines[line] = end - start
        }
    }
    lines.keys.sortedByDescending {
        key -> lines[key]!!
    }.forEach {
        println("${humanReadable(lines[it]!!)}: $it")
    }
}