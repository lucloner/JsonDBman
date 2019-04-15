package net.vicp.biggee.kotlin.db

import com.google.gson.JsonArray
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import net.vicp.biggee.kotlin.conf.JsonDBman
import java.sql.DriverManager
import java.sql.ResultSet
import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.collections.HashSet

object JsonHelper {
    val dbConn by lazy {
        Class.forName(JsonDBman.dbdriver)
        DriverManager.getConnection(JsonDBman.connStr)
    }
    val statement by lazy {
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

    private fun initTable(tableName: String, jsonObject: JsonObject) = praseJsonObject(tableName, jsonObject)

    private fun getArraryTable(linkID: String? = null): ResultSet {
        var whereSQL = " WHERE"
        if (linkID == null) {
            whereSQL = ""
        } else {
            whereSQL += " [${JsonDBman.dblinks}]='$linkID'"
        }

        return statement.executeQuery("SELECT * FROM [${JsonDBman.dbarrays}]$whereSQL ")
    }

    private fun praseJsonObject(
        tableName: String,
        jsonObject: JsonObject,
        link: String? = null
    ): List<HashMap<String, HashMap<String, String>>> {
        System.out.println("praseJsonObject$jsonObject")
        val primaryKey = getPrimaryKey(tableName)
        val primaryKeyName = primaryKey.first
        val primaryKeyValue = primaryKey.second

        val tableRow = HashMap<String, String>().apply {
            putIfAbsent(primaryKeyName, primaryKeyValue)
            if (link != null) {
                System.out.println("praseLink:$link")
                putIfAbsent(JsonDBman.dblinks, link)
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
//        System.out.println("praseJsonElement")
        val primaryKey = getPrimaryKey(tableName)
        val primaryKeyName = primaryKey.first
        val primaryKeyValue = if (tableRow.containsKey(primaryKeyName)) tableRow[primaryKeyName] else primaryKey.second
        val primaryKeyPair = Pair(primaryKeyName, primaryKeyValue!!)
        val primaryLink = getLink(elementName)
        val primaryLinkName = if (link == null) elementName else JsonDBman.dblinks
        val primaryLinkValue = if (link == null) primaryLink.second else link
        val primaryLinkPair = Pair(primaryLinkName, primaryLinkValue)

        val records = ArrayList<HashMap<String, HashMap<String, String>>>()

        tableRow.put(elementName, primaryLinkValue)
        tableRow.putIfAbsent(primaryKeyPair.first, primaryKeyPair.second)
        tableRow.putIfAbsent(primaryLinkPair.first, primaryLinkPair.second)

        if (jsonElement.isJsonObject) {
            records.addAll(praseJsonObject(elementName, jsonElement.asJsonObject, link))
        } else if (jsonElement.isJsonArray) {
            records.addAll(
                praseJsonArrary(
                    tableName,
                    elementName,
                    jsonElement.asJsonArray,
                    primaryLinkValue,
                    primaryKeyPair
                )
            )
        } else {
            var value = jsonElement.toString()
            try {
                value = jsonElement.asString
            } catch (e: Exception) {
                e.printStackTrace()
            }
            tableRow.put(elementName, value)
        }
        return records
    }

    private fun praseJsonArrary(
        tableName: String,
        elementName: String,
        jsonArray: JsonArray,
        link: String? = null,
        key: Pair<String, String>? = null
    ): List<HashMap<String, HashMap<String, String>>> {
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
        return keyValue
    }

    private fun getPrimaryKey(tableName: String) = getKey(tableName, "ID")
    private fun getLink(tableName: String) = getKey(tableName, "LINK")

    fun saveJsonToDB(tableName: String, jsonObject: JsonObject) {
//        System.out.println("saveJsonToDB")
        val missionList = initTable(tableName, jsonObject)
        missionList.iterator().forEach {
            it.iterator().forEach {
                saveToDB(it.key, it.value)
            }
        }
    }

    fun saveToDB(tableName: String, tableData: HashMap<String, String>) {
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
        System.out.println(insertIntoSQL)
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

    fun getJsonObject(tableName: String, key: String? = null, value: String? = null): JsonObject {
        var jsonObject = JsonObject()
        val rs = loadFromDB(tableName, key, value)
        if (rs.last()) {
            jsonObject = getJsonFromRs(rs, tableName)
        }
        rs.close()
        return jsonObject
    }

    fun getJsonElement(tableName: String, key: String? = null, value: String? = null): JsonElement {
        var jsonElement: JsonElement = JsonObject()
        val arrayRs = getArraryTable(value)
        if (arrayRs.fetchSize > 0) {
            jsonElement = getJsonArray(tableName, key, value)
        } else {
            jsonElement = getJsonObject(tableName, key, value)
        }
        arrayRs.close()
        return jsonElement
    }

    fun getJsonArray(tableName: String, key: String? = null, value: String? = null): JsonArray {
        val jsonArray = JsonArray()
        val rs = loadFromDB(tableName, key, value)
        while (rs.next()) {
            val jsonObject = getJsonFromRs(rs, tableName)
            jsonArray.add(jsonObject)
        }
        rs.close()
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
        val rs = dbConn.createStatement().executeQuery("SELECT * FROM [$tableName]$whereSQL")
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