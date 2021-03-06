package net.vicp.biggee.kotlin.json

import com.google.gson.Gson
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import com.google.gson.JsonParser

@Deprecated("未完工")
object DBCache : Thread.UncaughtExceptionHandler {
    private var stopflag = false
    val uncaughtExceptionHandler = HashSet<Thread.UncaughtExceptionHandler>()
    val cache_tableNjson = HashMap<String, String>()
    val cache_keyNjson = HashMap<String, String>()
    val cache_idNlink = HashMap<String, String>()
    val cache_json = HashSet<String>()
    val cache_jsonArray = HashSet<String>()
    val cache_loadDBpath = HashMap<String, String>()

    /**
     * Method invoked when the given thread terminates due to the
     * given uncaught exception.
     *
     * Any exception thrown by this method will be ignored by the
     * Java Virtual Machine.
     * @param t the thread
     * @param e the exception
     */
    override fun uncaughtException(t: Thread?, e: Throwable?) {
        uncaughtExceptionHandler.iterator().forEach {
            it.uncaughtException(t, e)
        }
    }

    private val caches =
        arrayOf(cache_tableNjson, cache_keyNjson, cache_idNlink, cache_json, cache_jsonArray, cache_loadDBpath)

    fun stop() {
        stopflag = true
    }

    fun checkStopFlag(): Boolean {
        if (stopflag) {
            stopflag = false
            return true
        }
        return false
    }

    fun collectPath(vararg paths: Pair<String, String>) {
//        Thread {
//            paths.iterator().forEach {
//                if (cache_tableNjson.containsKey(it.first)) {
//                    Thread {
//                        if (cache_loadDBpath[it.first].equals(it.second)) {
//                            throw LinkageError("table name&column catched")
//                        }
//                        throw LinkageError("table name catched")
//                    }.apply {
//                        uncaughtExceptionHandler = this@DBCache
//                        start()
//                    }
//                }
//                cache_loadDBpath.put(it.first, it.second)
//            }
//        }.start()
    }

    fun collectCache(json: String, table: String? = null, vararg key: String?) {
//        Thread {
//            cache_tableNjson.put(table ?: "", json)
//            key.iterator().forEach {
//                cache_keyNjson.put(it ?: "", json)
//            }
//            collectJsonElement(json)
//        }.start()
    }

    fun collectCacheIfAbsent(json: String, table: String? = null, vararg key: String?) {
//        Thread {
//            cache_tableNjson.putIfAbsent(table ?: "", json)
//            key.iterator().forEach {
//                cache_keyNjson.putIfAbsent(it ?: "", json)
//            }
//            collectJsonElement(json)
//        }.start()
    }

    fun collectCacheKeyPair(vararg pair: Pair<String, String>?) {
//        Thread {
//            pair.iterator().forEach {
//                if (it != null) {
//                    cache_idNlink.put(it.first, it.second)
//                }
//            }
//        }.start()
    }

    fun collectJsonElement(json: String) {
//        Thread {
//            val jsonElement = JsonParser().parse(json)
//            when {
//                jsonElement.isJsonObject -> cache_json.add(json)
//                jsonElement.isJsonArray -> cache_jsonArray.add(json)
//            }
//        }.start()
    }

    fun fillJson(jsonObject: JsonObject, vararg availableKey: String?): JsonObject {
        val filledJsonObject = fillJsonObject(jsonObject)
        if (availableKey.size == 0) {
            return filledJsonObject
        }
        return filterJson(filledJsonObject, availableKey)
    }

    private fun fillJsonObject(jsonObject: JsonObject): JsonObject {
        var returnJsonObject = jsonObject
        cache_json.iterator().forEach {
            val cachedJsonObject = JsonParser().parse(it).asJsonObject
            returnJsonObject = mergJson(returnJsonObject, cachedJsonObject)
        }
        collectJsonElement(returnJsonObject.toString())
        return returnJsonObject
    }

    fun filterJson(jsonObject: JsonObject, availableKey: Array<out String?>): JsonObject {
        val returnJsonObject = jsonObject
        returnJsonObject.keySet().iterator().forEach {
            if (!availableKey.contains(it)) {
                returnJsonObject.remove(it)
            } else {
                collectCache(returnJsonObject.toString(), null, it)
            }
        }
        return returnJsonObject
    }

    fun overwriteJson(vararg jsonObject: JsonObject): JsonObject {
        val returnJsonObject = jsonObject[0]
        jsonObject.iterator().forEach {
            val thisJson = it
            thisJson.keySet().iterator().forEach {
                returnJsonObject.add(it, thisJson[it])
            }
        }
        return returnJsonObject
    }

    fun mergJson(vararg jsonObject: JsonObject): JsonObject {
        val returnJsonObject = jsonObject[0]
        jsonObject.iterator().forEach {
            val thisJson = it
            thisJson.keySet().iterator().forEach {
                if (!returnJsonObject.has(it)) {
                    returnJsonObject.add(it, thisJson[it])
                }
            }
        }
        return returnJsonObject
    }

    fun updateJson(originJsonObject: JsonObject, vararg patchJsonObject: JsonObject): Boolean {
        val originJson = originJsonObject.toString()
        val patchedJson = mergJson(originJsonObject, *patchJsonObject).toString()
        return !originJson.equals(patchedJson)
    }

    fun flushCache() {
        caches.iterator().forEach {
            when (it) {
                is MutableMap<*, *> -> it.clear()
                is MutableSet<*> -> it.clear()
            }
        }
    }

    fun locateJson(jsonElement: JsonElement, key: String): JsonElement {
        when {
            jsonElement.isJsonObject -> {
                val jsonObject = jsonElement.asJsonObject
                if (jsonObject.has(key)) {
//                    System.out.println("found jsonObject")
                    return jsonObject
                }
                jsonObject.keySet().iterator().forEach {
                    val found = locateJson(jsonObject[it], key)
                    if (!found.isJsonNull) {
                        return found
                    }
                }
            }
            jsonElement.isJsonArray -> {
                jsonElement.asJsonArray.iterator().forEach {
                    val found = locateJson(it, key)
                    if (!found.isJsonNull) {
                        return found
                    }
                }
            }
            jsonElement.isJsonNull -> {
//                System.out.println("isJsonNull")
            }
            else -> {
                if (jsonElement.toString().contains(key)) {
//                    System.out.println("$key is value")
                    return jsonElement
                }
            }
        }
//        System.out.println("not found $key")
        return JsonParser().parse(Gson().toJson(null))
    }

}