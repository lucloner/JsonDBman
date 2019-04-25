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
    val cache_tabnames = ArrayList<String>()

    private val dbConn by lazy {
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

    private fun praseJsonElement(
        tableRow: HashMap<String, String>,
        elementName: String,
        jsonElement: JsonElement
    ): HashMap<String, String> {
        //print("E:($elementName)")
        val returnRow = tableRow

        when {
            jsonElement.isJsonPrimitive -> {
                val jsonPrimitive = jsonElement.asJsonPrimitive
                val tableData = praseJsonPrimitive(jsonPrimitive)
                returnRow.put(elementName, tableData)
            }
            jsonElement.isJsonObject || jsonElement.isJsonArray -> {
                val link = getLink(elementName)
                returnRow.put(elementName, link.second)
            }
            jsonElement.isJsonNull -> {
                //print("E:(null)")
            }
            else -> {
                val whateverElse = jsonElement.toString()
                returnRow.put(elementName, whateverElse)
            }
        }
        return returnRow
    }

    private fun praseJsonPrimitive(
        jsonPrimitive: JsonPrimitive
    ): String {
        //print("P")
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
        //print("O:{${tableName}}")
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

            val link = tableRow[it] ?: "" //meight be
            val subTableRow = HashMap<String, String>().apply {
                put(subKey.first, subKey.second)
                put(JsonDBman.dblinks, link)
            }

            when {
                jsonElement.isJsonObject -> {
                    val subJsonObject = jsonElement.asJsonObject
                    praseJsonObject(it, subJsonObject, subTableRow)
                    DBCache.collectCache(subJsonObject.toString(), it, *subTableRow.keys.toTypedArray())
                }
                jsonElement.isJsonArray -> {
                    val subJsonArray = jsonElement.asJsonArray
                    praseJsonArray(subJsonArray, it, link)
                    DBCache.collectCache(subJsonArray.toString(), it, *subTableRow.keys.toTypedArray())

                }
            }
        }
        saveToDB(tableName, tableRow)
        //println("_O:{${tableName}}")
        cache_tabnames.add(tableName)
    }

    private fun praseJsonArray(
        jsonArray: JsonArray,
        elementName: String = "${JsonDBman.dbname}_${JsonDBman.thisname}",
        link: String = ""
    ) {
        //print("A:[${jsonArray.size()},$elementName,$link]")

        //注册数组
        val linkTableKey = getPrimaryKey(JsonDBman.dblinkheader)
        val linkTableRow = HashMap<String, String>().apply {
            put(linkTableKey.first, linkTableKey.second)
            put("key", elementName)
            put("link", link)
        }
        DBCache.collectCacheKeyPair(linkTableKey)
        cache_tabnames.add(JsonDBman.dblinkheader)

        if (jsonArray.size() == 0) {
            return
        }

        //创建表模板
        val key = getPrimaryKey(elementName)
        var tableRow = HashMap<String, String>().apply {
            put(key.first, key.second)
            put(JsonDBman.dblinks, link)
        }

        //处理矩阵
        val arrayLength = jsonArray.size()
        val jsonString = jsonArray.toString()
        val jsonLength = jsonString.length
        val ratio = jsonLength / arrayLength
        if (ratio < 5 || !jsonArray.first().isJsonObject) {
            val base64 = Base64.getEncoder().encodeToString(jsonString.toByteArray())
            linkTableRow.put("${JsonDBman.dblinkheader}_value", base64)
            saveToDB(JsonDBman.dblinkheader, linkTableRow)
            //println("!B!")
            cache_tabnames.add("${JsonDBman.dblinkheader}_base64")
            return
        }
        saveToDB(JsonDBman.dblinkheader, linkTableRow)

        //遍历数组
        DBCache.collectCache(jsonArray.toString(), JsonDBman.dblinkheader, linkTableKey.second, key.second)
        DBCache.collectCacheKeyPair(key)
        jsonArray.iterator().forEach {
            when {
                it.isJsonArray -> {
                    val subJsonArray = it.asJsonArray
                    tableRow.put(JsonDBman.dbarrays, link)
                    praseJsonArray(subJsonArray, elementName, link)
                }
                it.isJsonObject -> {
                    val subJsonObject = it.asJsonObject
                    tableRow.put(JsonDBman.dbarrays, link)
                    praseJsonObject(elementName, subJsonObject, tableRow)
                }
                else -> {
                    //其他处理
                    tableRow = praseJsonElement(tableRow, JsonDBman.dbarrays, it)
                    saveToDB(elementName, tableRow)
                }
            }

            //保存表
            DBCache.collectCache(it.toString(), elementName, JsonDBman.dbarrays, key.second)
        }
        cache_tabnames.add(elementName)
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

    fun saveJsonToDB(tableName: String, jsonObject: JsonObject): String {
        val key = getPrimaryKey(tableName)
        val tableRow = HashMap<String, String>().apply {
            put(key.first, key.second)
            put(JsonDBman.dbprimarykey, "${JsonDBman.thisname}_root")
        }

        cache_tabnames.clear()
        cache_tabnames.add(tableName)
        praseJsonObject(tableName, jsonObject, tableRow)

        val sqlHead = "SELECT * FROM [$tableName]\n"
        val sqlTail = "WHERE [$tableName].[${key.first}]='${key.second}'"
        val joinedTableSql = HashMap<String, String>()
        cache_tabnames.iterator().forEach {
            val tname = it
            val isRoot = tname == tableName
            if (!(tname.equals("${JsonDBman.dblinkheader}_base64") || tname.equals(JsonDBman.dblinkheader) || isRoot)) {
                val sql: String
                if (joinedTableSql.keys.contains(tname)) {
                    val case = "[$tableName].[$tname]=[$tname].[${JsonDBman.dblinks}]"
                    val lastsql = joinedTableSql[tname]
                    if (!lastsql!!.contains(case)) {
                        sql = "${lastsql} OR $case\n"
                    } else {
                        sql = lastsql
                    }
                } else {
                    sql = "JOIN [$tname] ON [$tableName].[$tname]=[$tname].[${JsonDBman.dblinks}]\n"
                }
                joinedTableSql.put(tname, sql)
            }
        }

        val arrayTableSqlHead = "JOIN [${JsonDBman.dblinkheader}] ON"
        val arrayTableSqlCase = "[${JsonDBman.dblinkheader}].[link]="
        val arrayTableSql = StringBuilder(arrayTableSqlHead)
        var orStr = ""
        joinedTableSql.keys.iterator().forEach {
            arrayTableSql.append(orStr)
            arrayTableSql.append(arrayTableSqlCase)
            arrayTableSql.append("[$it].[${JsonDBman.dblinks}]\n")
            orStr = " OR "
        }

        val sqlBuilder = StringBuilder(sqlHead)
        joinedTableSql.values.iterator().forEach {
            sqlBuilder.append(it)
        }
        sqlBuilder.append(arrayTableSql)
        sqlBuilder.append(sqlTail)
        return sqlBuilder.toString()
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

    fun getJsonElement(tableName: String, data: Map.Entry<String, String>): JsonElement? {
        //print("e:${data.key}:")
        val jsonKey = data.key
        val jsonValue = data.value
        DBCache.collectCacheKeyPair(Pair(jsonKey, jsonValue))
        when {
            jsonKey.equals(getPrimaryKey(tableName).first) || jsonKey.equals(JsonDBman.dblinks) -> {
                //print("%keyID%$jsonKey%")
                return null
            }
            jsonKey.equals(JsonDBman.dbarrays) -> {
                //print("%arrayID%$jsonKey%")
                return null
            }
            jsonValue.startsWith(JsonDBman.dbprimarykeyid) -> {
                val arrayTable = getArrayTable(jsonValue)
                if (arrayTable.last()) {
                    val arrayValue: String? = arrayTable.getString("${JsonDBman.dblinkheader}_value")
                    if (arrayValue != null && arrayValue.isNotBlank()) {
                        val base64Value = arrayTable.getString("${JsonDBman.dblinkheader}_value")
                        val base64 = String(Base64.getDecoder().decode(base64Value))
                        val jsonElement = JsonParser().parse(base64)
                        return jsonElement
                    }
                    val jsonArray = getJsonArray(jsonKey, JsonDBman.dblinks, jsonValue)
                    return jsonArray
                }
                //is JsonObject
                val jsonObject = getJsonObject(jsonKey, JsonDBman.dblinks, jsonValue)
                return jsonObject
            }
            else -> return JsonPrimitive(jsonValue)
        }
    }

    fun getJsonObject(tableName: String, key: String? = null, value: String? = null): JsonObject {
        //print("o($tableName)")
        val jsonObject = JsonObject()
        try {
            val rs = loadFromDB(tableName, key, value)
            if (rs.last()) {
                getMapFromRs(rs, tableName).iterator().forEach {
                    val jsonElement = getJsonElement(tableName, it)
                    if (jsonElement != null) {
                        jsonObject.add(it.key, jsonElement)
                    }
                }
            }
            rs.close()
        } catch (e: Exception) {
            e.printStackTrace()
        }
        DBCache.collectCache(jsonObject.toString(), tableName, key, value)
        DBCache.collectCacheKeyPair(Pair(key ?: "", value ?: ""))
        //println("_o($tableName)")
        return jsonObject
    }

    fun getJsonArray(tableName: String, key: String? = null, value: String? = null): JsonArray {
        //print("a[$tableName]")
        val jsonArray = JsonArray()
        try {
            val rs = loadFromDB(tableName, key, value)
            while (rs.next()) {
                val jsonObject = JsonObject()
                getMapFromRs(rs, tableName).iterator().forEach {
                    val jsonElement = getJsonElement(tableName, it)
                    if (jsonElement != null) {
                        jsonObject.add(it.key, jsonElement)
                    }
                }
                if (jsonObject.size() > 0) {
                    jsonArray.add(jsonObject)
                }
            }
            rs.close()
        } catch (e: Exception) {
            e.printStackTrace()
        }
        DBCache.collectCache(jsonArray.toString(), tableName, key, value)
        DBCache.collectCacheKeyPair(Pair(key ?: "", value ?: ""))
        return jsonArray
    }

    private fun getMapFromRs(rs: ResultSet, tableName: String = ""): Map<String, String> {
        //print("R{$tableName}")
        val keyCnt = rs.metaData.columnCount
        val data = HashMap<String, String>()
        for (index in 1..keyCnt) {
            val jsonKey = rs.metaData.getColumnName(index)
            val jsonValue = rs.getString(index)
            jsonValue ?: continue
            data.put(jsonKey, jsonValue)
        }
        return data
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