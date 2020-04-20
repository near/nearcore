// This Kotlin/Native utility takes two cost tables and compares the difference.
import kotlin.text.Regex
import platform.posix.*
import kotlinx.cinterop.*

fun format2(d: Double): String {
    val dmul = (d * 100.0).toInt()
    return "${dmul/100}.${dmul%100}"
}

fun readCosts(name: String): Map<String, ULong> = memScoped {
    val regex = Regex("\\s*([\\p{Alnum}_]+): ([\\p{Digit}]+).*")
    val result = mutableMapOf<String, ULong>()
    val bufferLength = 64 * 1024
    val buffer = allocArray<ByteVar>(bufferLength)
    val file = fopen(name, "r") ?: throw Error("no file $name")
    while (true) {
        val line = fgets(buffer, bufferLength, file)?.toKString()?.trim()
        if (line == null) break
        regex.matchEntire(line) ?.let {
            result[it.groups[1]!!.value] = it.groups[2]!!.value.toULong(10)
        }
    }
    fclose(file)
    result
}

fun main(args: Array<String>) {
    val costs1 = readCosts(args[0])
    val costs2 = readCosts(args[1])

    costs1.keys.forEach {
        val c1 = costs1[it]!!
        val c2 = costs2[it]!!
        println("$it: time=$c1 insn=$c2 insn/time=${format2(c2.toDouble()/c1.toDouble())}")
    }
}