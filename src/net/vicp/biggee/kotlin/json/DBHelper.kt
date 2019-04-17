package net.vicp.biggee.kotlin.json

import com.google.gson.JsonArray
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import net.vicp.biggee.kotlin.db.JsonHelper

object DBHelper {
    fun getLatestJsonArrayFromDB(tableName: String, key: String): JsonArray {
        val jsonArray = JsonHelper.getJsonArray(tableName)
        val returnArray = getLatestJsonArray(jsonArray, key)
        DBCache.collectCache(returnArray.toString(), tableName, key)
        return returnArray
    }

    private fun getLatestJsonArray(jsonArray: JsonArray, key: String): JsonArray {
        val returnArray = JsonArray()
        val latest = HashMap<String, JsonElement>()
        jsonArray.iterator().forEach {
            if (it is JsonObject) {
                try {
                    val value = it[key].asString
                    latest.put(value, it)
                } catch (e: Exception) {
                    e.printStackTrace()
                    System.out.println("没有找到关键字:$key")
                }
            } else if (it is JsonArray) {
                val subArray = getLatestJsonArray(it, key)
                returnArray.add(subArray)
            } else {
                latest.put("", it)
            }
        }
        latest.values.iterator().forEach {
            returnArray.add(it)
        }
        return returnArray
    }
}