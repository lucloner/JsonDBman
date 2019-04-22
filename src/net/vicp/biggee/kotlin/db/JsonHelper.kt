package net.vicp.biggee.kotlin.db

import com.google.gson.JsonArray
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import net.vicp.biggee.kotlin.conf.JsonDBman
import net.vicp.biggee.kotlin.json.DBCache
import java.sql.DriverManager
import java.sql.ResultSet
import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.collections.HashSet

object JsonHelper {
    private val dbConn by lazy {
        Class.forName(JsonDBman.dbdriver)
        DriverManager.getConnection(JsonDBman.connStr)
    }
    private val statement by lazy {
        dbConn.createStatement()
    }

    fun checkDB(connString: String, DBName: String): Boolean {
        var res = false
        try {
            Class.forName(JsonDBman.dbdriver)
            DriverManager.getConnection(connString).createStatement()
                .execute("IF NOT EXISTS (SELECT * FROM [sysdatabases] WHERE [name]='$DBName') CREATE DATABASE [$DBName] COLLATE  Chinese_PRC_CS_AS")
            res = true
        } catch (e: Exception) {
            e.printStackTrace()
        }
        return res
    }

    private fun initTable(tableName: String, jsonElement: JsonElement): List<HashMap<String, HashMap<String, String>>> {
        return praseJsonElement(tableName, HashMap(), tableName, jsonElement)
    }

    private fun getArrayTable(linkID: String? = null): ResultSet {
        var whereSQL = " WHERE"
        if (linkID == null) {
            whereSQL = ""
        } else {
            whereSQL += " [${JsonDBman.dblinks}]='$linkID'"
        }

        return dbConn.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY,
            ResultSet.HOLD_CURSORS_OVER_COMMIT
        ).executeQuery("SELECT * FROM [${JsonDBman.dbarrays}]$whereSQL ")
    }

    private fun praseJsonObject(
        tableName: String,
        jsonObject: JsonObject,
        link: String? = null
    ): List<HashMap<String, HashMap<String, String>>> {
        System.out.println("praseJsonObject${jsonObject.hashCode()}")
        DBCache.collectCache(jsonObject.toString(), tableName, link)
        val primaryKey = getPrimaryKey(tableName)
        val primaryKeyName = primaryKey.first
        val primaryKeyValue = primaryKey.second

        val tableRow = HashMap<String, String>().apply {
            putIfAbsent(primaryKeyName, primaryKeyValue)
            if (link != null) {
//                System.out.println("praseLink:$link")
                putIfAbsent(JsonDBman.dblinks, link)
                DBCache.collectCacheKeyPair(Pair(JsonDBman.dblinks, get(JsonDBman.dblinks) ?: ""))
            }
        }
        val thisRecord = HashMap<String, HashMap<String, String>>().apply {
            put(tableName, tableRow)
        }
        val records = ArrayList<HashMap<String, HashMap<String, String>>>().apply {
            add(thisRecord)
        }
        jsonObject.keySet().iterator().forEach {
            records.addAll(praseJsonElement(tableName, tableRow, it, jsonObject[it], link))
        }
        return records
    }

    private fun praseJsonElement(
        tableName: String,
        tableRow: HashMap<String, String>,
        elementName: String,
        jsonElement: JsonElement,
        link: String? = null
    ): List<HashMap<String, HashMap<String, String>>> {
        System.out.println("praseJsonElement")
        DBCache.collectCacheIfAbsent(jsonElement.toString(), tableName, elementName, link)
        val primaryKey = getPrimaryKey(tableName)
        val primaryKeyName = primaryKey.first
        val primaryKeyValue = if (tableRow.containsKey(primaryKeyName)) tableRow[primaryKeyName] else primaryKey.second
        val primaryKeyPair = Pair(primaryKeyName, primaryKeyValue!!)
        val primaryLink = getLink(elementName)
        val primaryLinkName = if (link == null) elementName else JsonDBman.dblinks
        val primaryLinkValue = if (link == null) primaryLink.second else link
        val primaryLinkPair = Pair(primaryLinkName, primaryLinkValue)

        val records = ArrayList<HashMap<String, HashMap<String, String>>>()

        tableRow.apply {
            put(elementName, primaryLinkValue)
            putIfAbsent(primaryKeyPair.first, primaryKeyPair.second)
            putIfAbsent(primaryLinkPair.first, primaryLinkPair.second)
            DBCache.collectCacheKeyPair(
                Pair(elementName, get(JsonDBman.dblinks) ?: ""),
                Pair(primaryKeyPair.first, get(primaryKeyPair.first) ?: ""),
                Pair(primaryLinkPair.first, get(primaryLinkPair.first) ?: "")
            )
        }

        when {
            jsonElement.isJsonObject -> records.addAll(praseJsonObject(elementName, jsonElement.asJsonObject, link))
            jsonElement.isJsonArray -> records.addAll(
                praseJsonArray(
                    tableName,
                    elementName,
                    jsonElement.asJsonArray,
                    primaryLinkValue,
                    primaryKeyPair
                )
            )
            else -> {
                var value = jsonElement.toString()
                try {
                    value = jsonElement.asString
                } catch (e: Exception) {
                    e.printStackTrace()
                }
                System.out.println("praseJsonValue$value")
                tableRow.put(elementName, value)
            }
        }
        return records
    }

    private fun praseJsonArray(
        tableName: String,
        elementName: String,
        jsonArray: JsonArray,
        link: String? = null,
        key: Pair<String, String>? = null
    ): List<HashMap<String, HashMap<String, String>>> {
        System.out.println("praseJsonArray")
        DBCache.collectCache(jsonArray.toString(), tableName, elementName, link)
        val primaryKey = getPrimaryKey(JsonDBman.dbarrays)
        val primaryKeyName = primaryKey.first
        val primaryKeyValue = primaryKey.second
        val primaryLink = getLink(elementName)
//        val primaryLinkName = JsonDBman.dblinks
        val primaryLinkValue = if (link == null) primaryLink.second else link
        val records = ArrayList<HashMap<String, HashMap<String, String>>>()
        val tableRow = HashMap<String, String>()

        val arrayTableRow = HashMap<String, String>().apply {
            put(primaryKeyName, primaryKeyValue)
            put(JsonDBman.dbprimarykey, key!!.second)
            put(JsonDBman.dblinks, primaryLinkValue)
            put(JsonDBman.dbarrays, elementName)
            DBCache.collectCacheKeyPair(
                key,
                Pair(JsonDBman.dbprimarykey, get(JsonDBman.dbprimarykey) ?: ""),
                Pair(JsonDBman.dblinks, get(JsonDBman.dblinks) ?: ""),
                Pair(JsonDBman.dbarrays, get(JsonDBman.dbarrays) ?: "")
            )
        }
        val arrayTable = HashMap<String, HashMap<String, String>>().apply {
            put(JsonDBman.dbarrays, arrayTableRow)
        }
        records.add(arrayTable)
        jsonArray.iterator().forEach {
            records.addAll(praseJsonElement(tableName, tableRow, elementName, it, link))
        }
        return records
    }

    private fun getKey(tableName: String, mode: String): Pair<String, String> {
        val primaryKey = "${JsonDBman.dbprimarykey}_${tableName}_$mode"
        val primaryKeyValue = "${JsonDBman.dbprimarykeyid}_ID_${UUID.randomUUID()}_${System.currentTimeMillis()}"
        val keyValue = Pair(primaryKey, primaryKeyValue)
        DBCache.collectCacheKeyPair(keyValue)
        return keyValue
    }
    private fun getPrimaryKey(tableName: String) = getKey(tableName, "ID")
    private fun getLink(tableName: String) = getKey(tableName, "LINK")

    fun saveJsonToDB(tableName: String, jsonElement: JsonElement) {
//        System.out.println("saveJsonToDB")
        val missionList = initTable(tableName, jsonElement)
        missionList.iterator().forEach {
            it.iterator().forEach {
                saveToDB(it.key, it.value)
            }
        }
    }

    private fun saveToDB(tableName: String, tableData: HashMap<String, String>) {
        val createTable =
            "IF NOT EXISTS (SELECT * FROM sysobjects WHERE id = object_id('$tableName') and OBJECTPROPERTY(id, 'IsUserTable') = 1) CREATE TABLE [$tableName] ("
        val insertInto = "INSERT INTO [$tableName] ("
        var coldef = ""
        var cols = ""
        var vals = ""
        var commaStr = ""
        val keys = tableData.keys.iterator()
        keys.forEach {
            coldef += "$commaStr[$it] [varchar](max)"
            cols += "$commaStr[$it]"
            vals += "$commaStr'${tableData[it]}'"
            commaStr = ","
        }
        val createTableSQL = "$createTable$coldef)"
        val insertIntoSQL = "$insertInto$cols) VALUES($vals)"
//        System.out.println(insertIntoSQL)
        statement.execute(createTableSQL)
        fitTable(tableName, keys)
        statement.execute(insertIntoSQL)
    }

    private fun fitTable(tableName: String, collist: Iterator<String>) {
        val rs =
            statement.executeQuery("SELECT [COLUMN_NAME] FROM [INFORMATION_SCHEMA].[COLUMNS] WHERE [TABLE_NAME]='$tableName'")
        val newList = HashSet<String>().apply {
            collist.forEach {
                add(it)
            }
        }
        while (rs.next()) {
            newList.remove(rs.getString("COLUMN_NAME"))
        }
        val sqlTemplate = "ALTER TABLE [$tableName] ADD "
        var coldef = ""
        var commaStr = ""
        newList.iterator().forEach {
            coldef += "$commaStr[$it] [varchar](max)"
            commaStr = ","
        }
        val sql = "$sqlTemplate$coldef"
        rs.close()
        if (newList.isEmpty()) {
            return
        }
        statement.execute(sql)
    }

    private fun checkArray(key: String, value: String): Boolean {
        val arrayRs = getArrayTable()
        while (arrayRs.next()) {
            val linkToTable = arrayRs.getString(JsonDBman.dbarrays)
            val link = arrayRs.getString(JsonDBman.dblinks)
            if (key == linkToTable && value == link) {
                arrayRs.close()
                return true
            }
        }
        arrayRs.close()
        return false
    }

    fun getJsonObject(tableName: String, key: String? = null, value: String? = null): JsonObject {
        var jsonObject = JsonObject()
        try {
            val rs = loadFromDB(tableName, key, value)
            if (rs.last()) {
                jsonObject = getJsonFromRs(rs, tableName)
            }
            rs.close()
        } catch (e: Exception) {
            e.printStackTrace()
        }
        DBCache.collectCache(jsonObject.toString(), tableName, key, value)
        DBCache.collectCacheKeyPair(Pair(key ?: "", value ?: ""))
        return jsonObject
    }

    fun getJsonElement(tableName: String, key: String? = null, value: String? = null): JsonElement {
        var jsonElement: JsonElement = getJsonObject(tableName, key, value)
        val testJsonObject = jsonElement.asJsonObject
        val isArray = testJsonObject.keySet().contains(JsonDBman.dblinks)
        if (isArray) {
            val k: String? = JsonDBman.dblinks
            val v: String? = testJsonObject.get(k).asString
//            System.out.println("检测数组link:$v\t数组字段:${k}")
            jsonElement = getJsonArray(tableName, k, v)
            DBCache.collectCache(jsonElement.toString(), tableName, k, v)
            DBCache.collectCacheKeyPair(Pair(k ?: "", v ?: ""))
        }
        return jsonElement
    }

    fun getJsonArray(tableName: String, key: String? = null, value: String? = null): JsonArray {
        val jsonArray = JsonArray()
        try {
            val rs = loadFromDB(tableName, key, value)
            while (rs.next()) {
                val jsonObject = getJsonFromRs(rs, tableName)
                jsonArray.add(jsonObject)
            }
            rs.close()
        } catch (e: Exception) {
            e.printStackTrace()
        }
        DBCache.collectCache(jsonArray.toString(), tableName, key, value)
        DBCache.collectCacheKeyPair(Pair(key ?: "", value ?: ""))
        return jsonArray
    }

    private fun getJsonFromRs(rs: ResultSet, tableName: String = ""): JsonObject {
        val jsonObject = JsonObject()
        val keyCnt = rs.metaData.columnCount
        for (index in 1..keyCnt) {
            val jsonKey = rs.metaData.getColumnName(index)
            val jsonValue = rs.getString(index)
            if (jsonKey.equals("${JsonDBman.dbprimarykey}_${tableName}_ID") || jsonKey.equals(JsonDBman.dblinks)) {
                continue
            }
            if (jsonValue.startsWith(JsonDBman.dblinkheader)) {
                val subJsonElement = getJsonElement(jsonKey, JsonDBman.dblinks, jsonValue)
                jsonObject.add(jsonKey, subJsonElement)
            } else {
                jsonObject.addProperty(jsonKey, jsonValue)
            }
        }
        DBCache.collectCache(jsonObject.toString())
        return jsonObject
    }

    private fun loadFromDB(tableName: String, key: String?, value: String?): ResultSet {
        var k = key
        var v = value
        var whereSQL = " WHERE"
        if (value == null) {
            whereSQL = ""
        } else {
            if (k == null) {
                k = "${JsonDBman.dbprimarykey}_${tableName}_ID"
                v = "${JsonDBman.dbprimarykeyid}_$v"
            }
            whereSQL += " [$k]='$v'"
        }
        DBCache.collectPath(Pair(tableName, k ?: ""))
        var sql = "SELECT * FROM [$tableName]$whereSQL"
        if (DBCache.checkStopFlag()) {
            sql = "SELECT interrupted=1"
        }
        val rs = dbConn.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY,
            ResultSet.HOLD_CURSORS_OVER_COMMIT
        ).executeQuery(sql)
        DBCache.collectCacheKeyPair(Pair(k ?: "", v ?: ""))
        return rs
    }

    fun finalize() {
        try {
            statement.closeOnCompletion()
            dbConn.close()
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }
}