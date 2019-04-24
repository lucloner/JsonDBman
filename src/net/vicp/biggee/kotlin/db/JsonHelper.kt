package net.vicp.biggee.kotlin.db

import com.google.gson.*
import net.vicp.biggee.kotlin.conf.JsonDBman
import net.vicp.biggee.kotlin.json.DBCache
import java.sql.DriverManager
import java.sql.ResultSet
import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.collections.HashSet

object JsonHelper {
    val cache_sql = ArrayList<String>()
    val cache_tabnames = HashSet<String>()

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

    private fun praseJsonElement(
        tableRow: HashMap<String, String>,
        elementName: String,
        jsonElement: JsonElement
    ): HashMap<String, String> {
        print("E:($elementName)")
        var returnRow = tableRow

        when {
            jsonElement.isJsonPrimitive -> {
//                print("praseJsonElement:(JsonPrimitive)")
                val jsonPrimitive = jsonElement.asJsonPrimitive
                val tableData = praseJsonPrimitive(jsonPrimitive)
                returnRow.put(elementName, tableData)
            }
            jsonElement.isJsonObject || jsonElement.isJsonArray -> {
                val link = getLink(elementName)
                returnRow.put(elementName, link.second)
//                print("praseJsonElement:(JsonObject/JsonArray):${link.second}")
            }
            jsonElement.isJsonNull -> {
//                print("praseJsonElement:(null)")
            }
            else -> {
//                print("praseJsonElement:(else)")
                val whateverElse = jsonElement.toString()
                returnRow.put(elementName, whateverElse)
            }
        }
        return returnRow
    }

    private fun praseJsonPrimitive(
        jsonPrimitive: JsonPrimitive
    ): String {
        print("P")
        var value = jsonPrimitive.toString()
        try {
            value = jsonPrimitive.asString
        } catch (e: Exception) {
            e.printStackTrace()
        }
        return value
    }

    private fun praseJsonObject(
        tableName: String,
        jsonObject: JsonObject,
        rowTemplate: HashMap<String, String>
    ) {
        print("O:{${tableName}}")
        val key = getPrimaryKey(tableName)
        DBCache.collectCache(jsonObject.toString(), tableName)
        DBCache.collectCacheKeyPair(key)

        var tableRow = rowTemplate
        tableRow.putIfAbsent(key.first, key.second)

        jsonObject.keySet().iterator().forEach {
            val jsonElement = jsonObject[it]
            tableRow = praseJsonElement(tableRow, it, jsonElement)
            val subKey = getPrimaryKey(it)
            DBCache.collectCacheKeyPair(subKey)

            val link = tableRow[it] //meight be
            val subTableRow = HashMap<String, String>().apply {
                put(subKey.first, subKey.second)
                put(JsonDBman.dblinks, link!!)
            }

            val sql = "RIGHT JOIN [$it] ON [$it].[${subKey.first}]='${subKey.second}'"
            when {
                jsonElement.isJsonObject -> {
                    val subJsonObject = jsonElement.asJsonObject
                    praseJsonObject(it, subJsonObject, subTableRow)
                    cache_sql.add(sql)
                    DBCache.collectCache(subJsonObject.toString(), it, *subTableRow.keys.toTypedArray())
                }
                jsonElement.isJsonArray -> {
                    val subJsonArray = jsonElement.asJsonArray
                    praseJsonArray(subJsonArray, it, link)
                    cache_sql.add(sql)
                    DBCache.collectCache(subJsonArray.toString(), it, *subTableRow.keys.toTypedArray())
                }
            }
        }
        saveToDB(tableName, tableRow)
        println("_O:{${tableName}}")
    }

    private fun praseJsonArray(
        jsonArray: JsonArray,
        elementName: String? = "",
        link: String? = ""
    ) {
        print("A:[${jsonArray.size()},$elementName,$link]")
        //注册数组
        val linkTableKey = getPrimaryKey(JsonDBman.dblinkheader)
        val linkTableRow = HashMap<String, String>().apply {
            put(linkTableKey.first, linkTableKey.second)
            put("key", elementName!!)
            put("link", link!!)
        }
        DBCache.collectCacheKeyPair(linkTableKey)

        //创建表模板
        val key = getPrimaryKey(elementName!!)
        var tableRow = HashMap<String, String>().apply {
            put(key.first, key.second)
            put(JsonDBman.dblinks, link!!)
        }

        //处理矩阵
        val arrayLength = jsonArray.size()
        val jsonString = jsonArray.toString()
        val jsonLength = jsonString.length
        val ratio = jsonLength / arrayLength
        if (ratio < 5) {
            val base64 = Base64.getEncoder().encodeToString(jsonString.toByteArray())
            linkTableRow.put("${JsonDBman.dblinkheader}_value", base64)
            saveToDB(JsonDBman.dblinkheader, linkTableRow)
            println("!B!")

            return
        }
        saveToDB(JsonDBman.dblinkheader, linkTableRow)

        //遍历数组
        val subElementName = "${JsonDBman.dbprimarykey}_$elementName"

        DBCache.collectCache(jsonArray.toString(), JsonDBman.dblinkheader, linkTableKey.second, key.second)
        DBCache.collectCacheKeyPair(key)
        jsonArray.iterator().forEach {
            //预处理
            val subKey = getPrimaryKey(subElementName)
            val subTableRow = HashMap<String, String>().apply {
                put(subKey.first, subKey.second)
                put(JsonDBman.dblinks, link!!)
            }
            when {
                it.isJsonArray -> {
                    val subJsonArray = it.asJsonArray
                    tableRow.put(JsonDBman.dbarrays, link!!)
                    praseJsonArray(subJsonArray, subElementName, link)
                }
                it.isJsonObject -> {
                    val subJsonObject = it.asJsonObject
                    tableRow.put(JsonDBman.dbarrays, link!!)
                    praseJsonObject(subElementName, subJsonObject, subTableRow)
                }
                else -> {
                    //其他处理
                    tableRow = praseJsonElement(tableRow, JsonDBman.dbarrays, it)
                }
            }

            //保存表
            DBCache.collectCache(it.toString(), elementName, JsonDBman.dbarrays, key.second, subKey.second)
            DBCache.collectCacheKeyPair(subKey)
        }
        saveToDB(elementName, tableRow)
    }

    private fun getKey(tableName: String, mode: String): Pair<String, String> {
        val primaryKey = "${JsonDBman.dbprimarykey}_${tableName}_$mode"
        val primaryKeyValue = "${JsonDBman.dbprimarykeyid}_${UUID.randomUUID()}_${System.currentTimeMillis()}"
        val keyValue = Pair(primaryKey, primaryKeyValue)
        DBCache.collectCacheKeyPair(keyValue)
        return keyValue
    }

    private fun getPrimaryKey(tableName: String) = getKey(tableName, "ID")
    private fun getLink(tableName: String) = getKey(tableName, "LINK")

    fun saveJsonToDB(tableName: String, jsonObject: JsonObject) {
        val key = getPrimaryKey(tableName)
        val tableRow = HashMap<String, String>().apply {
            put(key.first, key.second)
            put(JsonDBman.dbprimarykey, "${JsonDBman.thisname}_root")
        }

        cache_sql.clear()
        cache_tabnames.clear()
        var sql = "SELECT * FROM [$tableName]"
        cache_sql.add(sql)
        praseJsonObject(tableName, jsonObject, tableRow)
        sql = "WHERE [$tableName].[${key.first}]='${key.second}'"
        cache_sql.add(sql)
//        cache_sql.iterator().forEach {
//            println(it)
//        }
    }

    private fun saveToDB(tableName: String, tableData: HashMap<String, String>) {
        val createTable =
            "IF NOT EXISTS (SELECT * FROM sysobjects WHERE id = object_id('$tableName') and OBJECTPROPERTY(id, 'IsUserTable') = 1) CREATE TABLE [$tableName] ("
        val insertInto = "INSERT INTO [$tableName] ("
        var coldef = ""
        var cols = ""
        var vals = ""
        var commaStr = ""
        val keys = tableData.keys
        keys.iterator().forEach {
            coldef += "$commaStr[$it] [varchar](max)"
            cols += "$commaStr[$it]"
            vals += "$commaStr'${tableData[it]}'"
            commaStr = ","
        }
        val createTableSQL = "$createTable$coldef)"
        val insertIntoSQL = "$insertInto$cols) VALUES($vals)"
//        println(createTableSQL)
//        println(insertIntoSQL)
        statement.execute(createTableSQL)
        fitTable(tableName, keys)
        statement.execute(insertIntoSQL)
    }

    private fun getColumns(tableName: String): Array<String> {
        val rs =
            statement.executeQuery("SELECT [COLUMN_NAME] FROM [INFORMATION_SCHEMA].[COLUMNS] WHERE [TABLE_NAME]='$tableName'")
        val returnList = HashSet<String>().apply {
            while (rs.next()) {
                add(rs.getString("COLUMN_NAME"))
            }
        }
        rs.close()
        return returnList.toTypedArray()
    }

    private fun fitTable(tableName: String, collist: Set<String>) {
        val newList = HashSet<String>().apply {
            addAll(collist)
        }
        newList.removeAll(getColumns(tableName))
        val sqlTemplate = "ALTER TABLE [$tableName] ADD "
        var coldef = ""
        var commaStr = ""
        newList.iterator().forEach {
            coldef += "$commaStr[$it] [varchar](max)"
            commaStr = ","
        }
        val sql = "$sqlTemplate$coldef"
        if (newList.isEmpty()) {
            return
        }
        statement.execute(sql)
    }

    private fun getArrayTable(linkID: String? = null): ResultSet {
        var whereSQL = " WHERE"
        if (linkID == null) {
            whereSQL = ""
        } else {
            whereSQL += "[link]='$linkID'"
        }

        return dbConn.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY,
            ResultSet.HOLD_CURSORS_OVER_COMMIT
        ).executeQuery("SELECT * FROM [${JsonDBman.dblinkheader}] $whereSQL ")
    }

    private fun checkArray(value: String): Pair<String, String?> {
        print("C.$value.")
        val arrayRs = getArrayTable(value)
        var returnData = Pair<String, String?>("", null)
        while (arrayRs.last()) {
            returnData = Pair<String, String?>(arrayRs.getString("key"), "")
            val base64 = arrayRs.getString("${JsonDBman.dblinkheader}_value")
            base64 ?: break
            returnData = Pair<String, String?>(returnData.first, base64)
            println("!b!")
        }
        arrayRs.close()
        return returnData
    }

    fun getJsonObject(tableName: String, key: String? = null, value: String? = null): JsonObject {
        print("o($tableName)")
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
        println("_o($tableName)")
        return jsonObject
    }

    fun getJsonElement(tableName: String, key: String? = null, value: String? = null): JsonElement {
        print("e:$tableName:")
        var jsonElement: JsonElement = JsonObject()
        if (value != null && value.length > 0) {
            val checkedArray = checkArray(value)
            when (checkedArray.second) {
                null -> {
                    jsonElement = getJsonObject(tableName, key, value)
                }
                "" -> {
//                    jsonElement = getJsonArray(tableName, key, value)
                    println("\n!array?$tableName\t$key\t$value!")
                }
                else -> {
                    val jsonString = Base64.getDecoder().decode(checkedArray.second).toString()
                    jsonElement = JsonParser().parse(jsonString)
                }
            }
        }

        return jsonElement
    }

    fun getJsonArray(tableName: String, key: String? = null, value: String? = null): JsonArray {
        print("a[$tableName]")
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
        print("R{$tableName}")
        val jsonObject = JsonObject()
        val keyCnt = rs.metaData.columnCount
        for (index in 1..keyCnt) {
            val jsonKey = rs.metaData.getColumnName(index)
            val jsonValue = rs.getString(index)
            jsonValue ?: continue
            if (jsonKey.equals(getPrimaryKey(tableName).first) || jsonKey.equals(JsonDBman.dblinks)) {
                print("%dbarrays%$jsonKey%")
                continue
            }
            if (jsonKey.equals(JsonDBman.dbarrays)) {
                print("%dbarrays%")
                val subElementName = "${JsonDBman.dbprimarykey}_$tableName"
                val subJsonElement = getJsonArray(subElementName, JsonDBman.dblinks, jsonValue)
                jsonObject.add(jsonKey, subJsonElement)
            } else if (jsonValue.startsWith(JsonDBman.dbprimarykeyid)) {
                val subJsonElement = getJsonObject(jsonKey, JsonDBman.dblinks, jsonValue)
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
        val cols = getColumns(tableName)
        if (value == null) {
            whereSQL = ""
        } else {
            if (k == null) {
                k = "${JsonDBman.dbprimarykey}_${tableName}_ID"
                v = "${JsonDBman.dbprimarykeyid}_$v"
            }
            whereSQL += " [$k]='$v'"
        }
        if (!cols.contains(k)) {
            k = ""
            v = ""
            whereSQL = ""
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