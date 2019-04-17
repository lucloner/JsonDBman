package net.vicp.biggee.kotlin.json

import com.google.gson.JsonObject
import com.google.gson.JsonParser

object DBCache {
    val cache_tableNjson = HashMap<String, String>()
    val cache_keyNjson = HashMap<String, String>()
    val cache_idNlink = HashMap<String, String>()
    val cache_json = HashSet<String>()
    val cache_jsonArray = HashSet<String>()
    private val caches = arrayOf(cache_tableNjson, cache_keyNjson, cache_idNlink, cache_json, cache_jsonArray)

    fun collectCache(json: String, table: String? = null, vararg key: String?) {
        Thread {
            cache_tableNjson.put(table ?: "", json)
            key.iterator().forEach {
                cache_keyNjson.put(it ?: "", json)
            }
            collectJsonElement(json)
        }.start()
    }

    fun collectCacheIfAbsent(json: String, table: String? = null, vararg key: String?) {
        Thread {
            cache_tableNjson.putIfAbsent(table ?: "", json)
            key.iterator().forEach {
                cache_keyNjson.putIfAbsent(it ?: "", json)
            }
            collectJsonElement(json)
        }.start()
    }

    fun collectCacheKeyPair(vararg pair: Pair<String, String>?) {
        Thread {
            pair.iterator().forEach {
                if (it != null) {
                    cache_idNlink.put(it.first, it.second)
                }
            }
        }.start()
    }

    fun collectJsonElement(json: String) {
        Thread {
            val jsonElement = JsonParser().parse(json)
            when {
                jsonElement.isJsonObject -> cache_json.add(json)
                jsonElement.isJsonArray -> cache_jsonArray.add(json)
            }
        }.start()
    }

    fun fillJson(jsonObject: JsonObject, vararg availableKey: String?): JsonObject {
        val filledJsonObject = fillJsonObject(jsonObject)
        if (availableKey.size == 0) {
            return filledJsonObject
        }
        return filterJson(filledJsonObject, availableKey)
    }

    private fun fillJsonObject(jsonObject: JsonObject): JsonObject {
        var returnJsonObject = JsonObject()
        cache_json.iterator().forEach {
            val cachedJsonObject = JsonParser().parse(it).asJsonObject
            returnJsonObject = mergJson(returnJsonObject, cachedJsonObject)
        }
        collectJsonElement(returnJsonObject.toString())
        return returnJsonObject
    }

    fun filterJson(jsonObject: JsonObject, availableKey: Array<out String?>): JsonObject {
        val returnJsonObject = JsonObject()
        jsonObject.keySet().iterator().forEach {
            if (availableKey.contains(it)) {
                returnJsonObject.add(it, jsonObject[it])
                collectCache(returnJsonObject.toString(), null, it)
            }
        }
        return returnJsonObject
    }

    fun mergJson(vararg jsonObject: JsonObject): JsonObject {
        val returnJsonObject = JsonObject()
        jsonObject.iterator().forEach {
            val thisJson = it
            thisJson.keySet().iterator().forEach {
                if (!returnJsonObject.keySet().contains(it)) {
                    returnJsonObject.add(it, thisJson[it])
                }
            }
        }
        return returnJsonObject
    }

    fun flushCache() {
        caches.iterator().forEach {
            when (it) {
                is java.util.Map<*, *> -> it.clear()
                is java.util.Set<*> -> it.clear()
            }
        }
    }
}