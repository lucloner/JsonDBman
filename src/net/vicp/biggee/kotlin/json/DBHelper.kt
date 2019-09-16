package net.vicp.biggee.kotlin.json

import com.google.gson.JsonArray
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import net.vicp.biggee.kotlin.db.JsonHelper

class DBHelper(val jsonHelper: JsonHelper) {
    fun getLatestJsonArrayFromDB(tableName: String, vararg keys: String): JsonArray {
        val jsonArray = jsonHelper.getJsonArray(tableName)
        val returnArray = getLatestJsonArray(jsonArray, *keys)
        DBCache.collectCache(returnArray.toString(), tableName, *keys)
        return returnArray
    }

    fun getJsonSafe(tableName: String, vararg keys: String): JsonElement {
        DBCache.cache_loadDBpath.clear()
        val jsonArray = JsonArray()
        val t = Thread {
            jsonArray.addAll(getLatestJsonArrayFromDB(tableName, *keys))
        }.apply {
            DBCache.uncaughtExceptionHandler.clear()
            DBCache.uncaughtExceptionHandler.add(
                Thread.UncaughtExceptionHandler { _, e ->
                    //                    System.out.println("getJsonSafe:${t.name}")
                    e.printStackTrace()
                    if (e is LinkageError) {
                        DBCache.stop()
                    }
                }
            )
            isDaemon = true
            start()
        }
        while (t.isAlive) {
            Thread.sleep(100)
        }
        return jsonArray
    }

    //根据关键字,值相同的取最新的一个
    private fun getLatestJsonArray(jsonArray: JsonArray, vararg keys: String): JsonArray {
        val returnArray = JsonArray()
        val latest = HashMap<String, JsonElement>()
        keys.iterator().forEach {
            val key = it
            jsonArray.iterator().forEach {
                when (it) {
                    is JsonObject -> try {
                        val value = it[key].asString
                        latest.put(value, it)
                    } catch (e: Exception) {
                        e.printStackTrace()
//                        System.out.println("没有找到关键字:$keys")
                    }
                    is JsonArray -> {
                        val subArray = getLatestJsonArray(it, key)
                        returnArray.add(subArray)
                    }
                    else -> latest.put("", it)
                }
            }
        }
        latest.values.iterator().forEach {
            returnArray.add(it)
        }
        return returnArray
    }
}