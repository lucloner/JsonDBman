package net.vicp.biggee.kotlin.db

import com.google.gson.JsonArray
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import net.vicp.biggee.kotlin.conf.JsonDBman
import java.sql.DriverManager
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

    fun initTable(tableName: String, jsonObject: JsonObject): List<HashMap<String, HashMap<String, String>>> {
        val primaryKey = getPrimaryKey(tableName)
        val tableInitData = HashMap<String, String>().apply {
            put(primaryKey[0], primaryKey[1])
        }
        return praseJsonObject(tableName, tableInitData, jsonObject)
    }

    fun praseJsonObject(
        tableName: String,
        tableRow: HashMap<String, String>,
        jsonObject: JsonObject
    ): List<HashMap<String, HashMap<String, String>>> {
        System.out.println("praseJsonObject$jsonObject")
        val thisRecord = HashMap<String, HashMap<String, String>>().apply {
            put(tableName, tableRow)
        }
        val records = ArrayList<HashMap<String, HashMap<String, String>>>().apply {
            add(thisRecord)
        }
        jsonObject.keySet().iterator().forEach {
            records.addAll(praseJsonElement(tableName, tableRow, it, jsonObject[it]))
        }
        return records
    }

    fun praseJsonElement(
        tableName: String,
        tableRow: HashMap<String, String>,
        elementName: String,
        jsonElement: JsonElement
    ): List<HashMap<String, HashMap<String, String>>> {
//        System.out.println("praseJsonElement")
        val primaryKey = getPrimaryKey(tableName)
        val primaryKeyName = primaryKey[0]
        val primaryKeyValue = if (tableRow.containsKey(primaryKeyName)) tableRow[primaryKeyName] else primaryKey[1]
        val primaryLink = getLink(tableName)
        val primaryLinkName = JsonDBman.dblinks
        val primaryLinkValue = if (tableRow.containsKey(primaryLinkName)) tableRow[primaryLinkName] else primaryLink[1]
        val subLink = getLink(elementName)
        val subLinkName = elementName
        val subLinkValue = if (tableRow.containsKey(subLinkName)) tableRow[subLinkName] else subLink[1]

        tableRow.putIfAbsent(primaryKeyName, primaryKeyValue!!)
        tableRow.putIfAbsent(primaryLinkName, primaryLinkValue!!)
        tableRow.putIfAbsent(subLinkName, subLinkValue!!)

        val records = ArrayList<HashMap<String, HashMap<String, String>>>()
        val subTableRow = HashMap<String, String>().apply {
            put(primaryKeyName, primaryKeyValue)
            put(JsonDBman.dblinks, subLinkValue)
        }
        if (jsonElement.isJsonObject) {
            records.addAll(praseJsonObject(elementName, subTableRow, jsonElement.asJsonObject))
        } else if (jsonElement.isJsonArray) {
            jsonElement.asJsonArray.iterator().forEach {
                records.addAll(praseJsonElement(tableName, tableRow, elementName, it.asJsonObject))
            }
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

    fun getKey(tableName: String, mode: String): Array<String> {
        val primaryKey = "${JsonDBman.dbprimarykey}_${tableName}_$mode"
        val primaryKeyValue = "${JsonDBman.dbprimarykeyid}_ID_${UUID.randomUUID()}_${System.currentTimeMillis()}"
        val keyValue = arrayOf(primaryKey, primaryKeyValue)
        return keyValue
    }

    fun getPrimaryKey(tableName: String): Array<String> = getKey(tableName, "ID")
    fun getLink(tableName: String): Array<String> = getKey(tableName, "LINK")

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

    fun fitTable(tableName: String, collist: Iterator<String>) {
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

    fun getJson(tableName: String, key: String?, value: String): JsonElement {
        val jsonArray = loadFromDB(tableName, key, value)
        val jsonElement = if (jsonArray.size() > 1) jsonArray else jsonArray[0].asJsonObject
        return jsonElement
    }

    fun loadFromDB(tableName: String, key: String?, value: String): JsonArray {
        var k = key
        var v = value
        if (k == null) {
            k = "${JsonDBman.dbprimarykey}_${tableName}_ID"
            v = "${JsonDBman.dbprimarykeyid}_$v"
        }
        val jsonArray = JsonArray()
        val rs = statement.executeQuery("SELECT * FROM [$tableName] WHERE [$k]='$v'")
        while (rs.next()) {
            val jsonObject = JsonObject()
            val keyCnt = rs.metaData.columnCount
            for (index in 1..keyCnt) {
                val jsonKey = rs.metaData.getColumnName(index)
                val jsonValue = rs.getString(index)
                if (jsonKey.equals("${JsonDBman.dbprimarykey}_${tableName}_ID") || jsonKey.equals(JsonDBman.dblinks)) {
                    continue
                }
                if (jsonKey.startsWith(JsonDBman.dbprimarykey)) {
                    val subJsonElement = getJson(jsonKey, JsonDBman.dblinks, jsonValue)
                    jsonObject.add(jsonKey, subJsonElement)
                } else {
                    jsonObject.addProperty(jsonKey, jsonValue)
                }
            }
            jsonArray.add(jsonObject)
        }
        rs.close()
        return jsonArray
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