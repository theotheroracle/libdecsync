/**
 * libdecsync - DecsyncV2.kt
 *
 * Copyright (C) 2019 Aldo Gunsing
 *
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, see <http://www.gnu.org/licenses/>.
 */

package org.decsync.library

import kotlinx.serialization.json.*

@ExperimentalStdlibApi
internal class DecsyncV2<T>(
        override val decsyncDir: NativeFile,
        override val localDir: DecsyncFile,
        override val syncType: String,
        override val collection: String?,
        override val ownAppId: String
) : DecsyncInst<T>() {
    private val dir = getDecsyncSubdir(decsyncDir, syncType, collection).child("v2")

    init {
        // Create shared directories
        dir.mkdir()
    }

    private fun entriesWithPathToLines(entriesWithPath: Collection<Decsync.EntryWithPath>): List<String> =
            entriesWithPath.map { it.toJson().toString() }

    private fun getSequences(sequencesFile: DecsyncFile): Map<String, Int> {
        val text = sequencesFile.readText() ?: return emptyMap()
        return try {
            val obj = json.parseToJsonElement(text).jsonObject
            obj.mapValues { it.value.jsonPrimitive.int }
        } catch (e: Exception) {
            Log.e(e.message!!)
            emptyMap()
        }
    }

    private fun setSequences(sequencesFile: DecsyncFile, sequences: Map<String, Int>) {
        val obj = sequences.mapValues { JsonPrimitive(it.value) }
        val text = JsonObject(obj).toString()
        sequencesFile.writeText(text)
    }

    private fun getLocalSequences(): Map<String, MutableMap<String, Int>> {
        val file = localDir.child("sequences")
        val text = file.readText() ?: return emptyMap()
        return try {
            val obj = json.parseToJsonElement(text).jsonObject
            obj.mapValues { (_, subSequences) ->
                subSequences.jsonObject.mapValues { it.value.jsonPrimitive.int }.toMutableMap()
            }
        } catch (e: Exception) {
            Log.e(e.message!!)
            emptyMap()
        }
    }

    private fun setLocalSequences(sequences: Map<String, Map<String, Int>>) {
        val file = localDir.child("sequences")
        val obj = sequences.mapValues { (_, subSequences) ->
            JsonObject(subSequences.mapValues { JsonPrimitive(it.value) })
        }
        val text = JsonObject(obj).toString()
        file.writeText(text)
    }

    override fun setEntry(path: List<String>, key: JsonElement, value: JsonElement) =
            setEntries(listOf(Decsync.EntryWithPath(path, key, value)))

    override fun setEntries(entriesWithPath: List<Decsync.EntryWithPath>) {
        val ownDir = dir.child(ownAppId)
        val sequencesFile = ownDir.child("sequences")
        val sequences = getSequences(sequencesFile).toMutableMap()
        entriesWithPath.groupBy { Hash.pathToHash(it.path) }.forEach { (hash, entriesWithPath) ->
            val entriesWithPath = entriesWithPath.toMutableList()
            updateEntries(ownDir.child(hash), entriesWithPath, NoExtra(), true)
            if (entriesWithPath.isNotEmpty()) {
                sequences[hash] = (sequences[hash] ?: 0) + 1
            }
        }
        setSequences(sequencesFile, sequences)
    }

    override fun setEntriesForPath(path: List<String>, entries: List<Decsync.Entry>) =
            setEntries(entries.map { Decsync.EntryWithPath(path, it) })

    override fun executeAllNewEntries(optExtra: OptExtra<T>) {
        dir.resetCache()
        val appIds = dir.listDirectories().filter { it != ownAppId }
        val ownDir = dir.child(ownAppId)
        val localSequences = getLocalSequences().toMutableMap()
        var updatedSequences = false
        for (appId in appIds) {
            val appDir = dir.child(appId)
            val sequences = getSequences(appDir.child("sequences"))
            for ((hash, sequence) in sequences) {
                if (sequence == localSequences[appId]?.get(hash) ?: 0) continue

                try {
                    val appFile = appDir.child(hash)
                    val entriesWithPath = appFile.readLines()
                            .mapNotNull { Decsync.EntryWithPath.fromLine(it) }
                            .toMutableList()
                    val ownFile = ownDir.child(hash)
                    val success = updateEntries(ownFile, entriesWithPath, optExtra)
                    if (success) {
                        updatedSequences = true
                        localSequences.getOrPut(appId) { mutableMapOf() }.let {
                            it[hash] = sequence
                        }
                    }
                    // Do not update the own sequences file,
                    // as other apps will already process the original entries
                } catch (e: Exception) {
                    Log.e(e.message!!)
                }
            }
        }
        if (updatedSequences) {
            setLocalSequences(localSequences)
        }
    }

    private fun executeEntries(entriesWithPath: MutableList<Decsync.EntryWithPath>, extra: T): Boolean {
        var allSuccess = true
        entriesWithPath.groupBy({ it.path }, { it.entry }).forEach { (path, entries) ->
            val entries = entries.toMutableList()
            val success = executeEntriesForPath(path, entries, extra)
            if (!success) {
                allSuccess = false
                entriesWithPath.removeAll {
                    it.path == path && !entries.contains(it.entry)
                }
            }
        }
        return allSuccess
    }

    private fun executeEntriesForPath(path: List<String>, entries: MutableList<Decsync.Entry>, extra: T): Boolean {
        val listener = listeners.firstOrNull { it.matchesPath(path) }
        if (listener == null) {
            Log.e("Unknown action for path $path")
            return true
        }
        return listener.onEntriesUpdate(path, entries, extra)
    }

    private fun updateEntries(
            file: DecsyncFile,
            entriesWithPath: MutableList<Decsync.EntryWithPath>,
            optExtra: OptExtra<T>,
            requireNewValue: Boolean = false
    ): Boolean {
        data class PathAndKey(val path: List<String>, val key: JsonElement) {
            constructor(entryWithPath: Decsync.EntryWithPath) : this(entryWithPath.path, entryWithPath.entry.key)
        }

        // Get a map of the stored entries
        val storedEntriesWithPath = HashMap<PathAndKey, Decsync.EntryWithPath>()
        file.readLines()
                .mapNotNull { Decsync.EntryWithPath.fromLine(it) }
                .forEach {
                    storedEntriesWithPath[PathAndKey(it)] = it
                }

        // Filter out entries which are not newer than the stored one
        val iterator = entriesWithPath.iterator()
        while (iterator.hasNext()) {
            val entryWithPath = iterator.next()
            val storedEntryWithPath = storedEntriesWithPath[PathAndKey(entryWithPath)] ?: continue
            if (entryWithPath.entry.datetime <= storedEntryWithPath.entry.datetime ||
                    (requireNewValue && entryWithPath.entry.value == storedEntryWithPath.entry.value)) {
                iterator.remove()
            }
        }

        // Execute the new entries
        // This also filters out any entries for which the listener fails
        val success = if (optExtra is WithExtra) {
            executeEntries(entriesWithPath, optExtra.value)
        } else {
            true
        }

        // Filter out the stored entries for which a new value is inserted
        var storedEntriesRemoved = false
        for (entryWithPath in entriesWithPath) {
            val storedEntryWithPath = storedEntriesWithPath.remove(PathAndKey(entryWithPath))
            if (storedEntryWithPath != null) {
                storedEntriesRemoved = true
            }
        }

        // Write the new stored entries
        if (storedEntriesRemoved) {
            val lines = entriesWithPathToLines(storedEntriesWithPath.values)
            file.writeLines(lines)
        }
        val lines = entriesWithPathToLines(entriesWithPath)
        file.writeLines(lines, true)

        return success
    }

    override fun executeStoredEntriesForPathExact(
            path: List<String>,
            extra: T,
            keys: List<JsonElement>?
    ): Boolean {
        val hash = Hash.pathToHash(path)
        return executeStoredEntriesForHash(hash, { it == path }, extra, keys)
    }

    override fun executeStoredEntriesForPathPrefix(
            prefix: List<String>,
            extra: T,
            keys: List<JsonElement>?
    ): Boolean {
        val pathPred = { path: List<String> ->
            path.take(prefix.size) == prefix
        }
        var allSuccess = true
        for (hash in Hash.allHashes) {
            allSuccess = allSuccess && executeStoredEntriesForHash(hash, pathPred, extra, keys)
        }
        return allSuccess
    }

    private fun executeStoredEntriesForHash(
            hash: String,
            pathPred: (List<String>) -> Boolean,
            extra: T,
            keys: List<JsonElement>?
    ): Boolean {
        val file = dir.child(ownAppId, hash)
        val entriesWithPath = file.readLines()
                .mapNotNull { Decsync.EntryWithPath.fromLine(it) }
                .filter { pathPred(it.path) }
                .filter { keys == null || it.entry.key in keys }
                .toMutableList()
        return executeEntries(entriesWithPath, extra)
    }

    override fun latestAppId(): String {
        var latestAppId: String? = null
        var latestDatetime: String? = null
        val appIds = dir.listDirectories()
        for (appId in appIds) {
            val appDir = dir.child(appId)
            for (hash in Hash.allHashes) {
                val file = appDir.child(hash)
                val datetime = file.readLines()
                        .mapNotNull { Decsync.EntryWithPath.fromLine(it) }
                        .map { it.entry.datetime }
                        .maxOrNull() ?: continue
                if (latestDatetime == null || datetime > latestDatetime ||
                        appId == ownAppId && datetime == latestDatetime) {
                    latestAppId = appId
                    latestDatetime = datetime
                }
            }
        }
        return latestAppId ?: ownAppId
    }

    override fun deleteOwnEntries() {
        deleteOwnSubdir(dir)
    }

    companion object {
        fun getStaticInfo(
                decsyncDir: NativeFile,
                syncType: String,
                collection: String?,
                info: MutableMap<JsonElement, JsonElement>,
                datetimes: MutableMap<JsonElement, String>
        ) {
            val hash = Hash.pathToHash(listOf("info"))
            val dir = getDecsyncSubdir(decsyncDir, syncType, collection).child("v2")
            val appIds = dir.listDirectories()
            for (appId in appIds) {
                dir.child(appId, hash).readLines()
                        .mapNotNull { Decsync.EntryWithPath.fromLine(it) }
                        .filter { it.path == listOf("info") }
                        .forEach { entryWithPath ->
                            val entry = entryWithPath.entry
                            val oldDatetime = datetimes[entry.key]
                            if (oldDatetime == null || entry.datetime > oldDatetime) {
                                info[entry.key] = entry.value
                                datetimes[entry.key] = entry.datetime
                            }
                        }
            }
        }

        fun getStaticInfo(
                decsyncDir: NativeFile,
                syncType: String,
                collection: String?
        ): Map<JsonElement, JsonElement> {
            val info = mutableMapOf<JsonElement, JsonElement>()
            val datetimes = mutableMapOf<JsonElement, String>()
            getStaticInfo(decsyncDir, syncType, collection, info, datetimes)
            return info
        }

        fun getEntriesCount(
                decsyncDir: NativeFile,
                syncType: String,
                collection: String?,
                prefix: List<String>
        ): Int {
            val values = mutableMapOf<Decsync.StoredEntry, JsonElement>()
            val datetimes = mutableMapOf<Decsync.StoredEntry, String>()
            val dir = getDecsyncSubdir(decsyncDir, syncType, collection).child("v2")
            val appIds = dir.listDirectories()
            for (appId in appIds) {
                for (hash in Hash.allHashes) {
                    dir.child(appId, hash).readLines()
                            .mapNotNull { Decsync.EntryWithPath.fromLine(it) }
                            .filter { it.path.take(prefix.size) == prefix }
                            .forEach { entryWithPath ->
                                val entry = entryWithPath.entry
                                val storedEntry = Decsync.StoredEntry(entryWithPath.path, entry.key)
                                val oldDatetime = datetimes[storedEntry]
                                if (oldDatetime == null || entry.datetime > oldDatetime) {
                                    values[storedEntry] = entry.value
                                    datetimes[storedEntry] = entry.datetime
                                }
                            }
                }
            }
            return values.values.filter { it != JsonNull }.size
        }

        fun getActiveApps(
                decsyncDir: NativeFile,
                syncType: String,
                collection: String?
        ): List<String> {
            val dir = getDecsyncSubdir(decsyncDir, syncType, collection).child("v2")
            return dir.listDirectories()
        }

        fun deleteApp(
                decsyncDir: NativeFile,
                syncType: String,
                collection: String?,
                appId: String
        ) {
            val dir = getDecsyncSubdir(decsyncDir, syncType, collection).child("v2")
            deleteSubdir(dir, appId)
        }
    }
}
