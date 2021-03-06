package net.vicp.biggee.kotlin.db

import com.google.gson.*
import net.vicp.biggee.kotlin.conf.JsonDBman
import net.vicp.biggee.kotlin.json.DBCache
import org.apache.commons.codec.binary.Base64
import java.sql.DriverManager
import java.sql.ResultSet
import java.util.*
import java.util.concurrent.Executors
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.collections.HashSet

class JsonHelper(val jsonDBman: JsonDBman) {
    val connStr: String by lazy {
        jsonDBman.apply {
            if (dbdriver.isEmpty() || dbprot.isEmpty() || dbip.isEmpty() || dbname.isEmpty() || dbuser.isEmpty() || dbpass.isEmpty()) {
                throw NullPointerException("${thisname}_init_Empty")
            }
            var charset = ""
            if (!dbcharset.isEmpty()) {
                charset = "charset=$dbcharset;"
            }
            if (!checkDB("$dbprot://$dbip;user=$dbuser;password=$dbpass", dbname)) {
                throw NullPointerException("${thisname}_init_NoDatabase")
            }
            return@lazy "$dbprot://$dbip/$dbname;user=$dbuser;password=$dbpass;$charset;$dbparams"
        }
        return@lazy ""
    }
    private val saveingThread = Executors.newSingleThreadExecutor()
    val cache_tabnames = ArrayList<String>()

    private val dbConn by lazy {
        Class.forName(jsonDBman.dbdriver)
        DriverManager.getConnection(connStr)
    }
    val statement by lazy {
        dbConn.createStatement()
    }

    fun <K, V> putIfAbsent(map: MutableMap<K, V>, key: K, value: V): V? {
        if (!map.containsKey(key)) {
            map.put(key, value)
            return value
        }
        return map.get(key)
    }

    fun checkDB(connString: String, DBName: String): Boolean {
        var res = false
        try {
            Class.forName(jsonDBman.dbdriver)
            val conn = DriverManager.getConnection(connString)
            if (conn.metaData.databaseProductName == "Microsoft SQL Server") {
                conn.createStatement()
                    .execute("IF NOT EXISTS (SELECT * FROM [master].[dbo].[sysdatabases] WHERE [name]='$DBName') CREATE DATABASE [$DBName] COLLATE  Chinese_PRC_CS_AS")
                res = true
            }
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
                put(jsonDBman.dblinks, link)
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
        elementName: String = "${jsonDBman.dbname}_${jsonDBman.thisname}",
        link: String = ""
    ) {
        //print("A:[${jsonArray.size()},$elementName,$link]")

        //注册数组
        val linkTableKey = getPrimaryKey(jsonDBman.dblinkheader)
        val linkTableRow = HashMap<String, String>().apply {
            put(linkTableKey.first, linkTableKey.second)
            put("key", elementName)
            put("link", link)
        }
        DBCache.collectCacheKeyPair(linkTableKey)
        cache_tabnames.add(jsonDBman.dblinkheader)

        if (jsonArray.size() == 0) {
            return
        }

        //创建表模板
        val key = getPrimaryKey(elementName)
        var tableRow = HashMap<String, String>().apply {
            put(key.first, key.second)
            put(jsonDBman.dblinks, link)
        }

        //处理矩阵
        val arrayLength = jsonArray.size()
        val jsonString = jsonArray.toString()
        val jsonLength = jsonString.length
        val ratio = jsonLength / arrayLength
        if (ratio < 5 || !jsonArray.first().isJsonObject) {
            val base64 = Base64.encodeBase64String(jsonString.toByteArray())
            linkTableRow.put("${jsonDBman.dblinkheader}_value", base64)
            saveToDB(jsonDBman.dblinkheader, linkTableRow)
            //println("!B!")
            cache_tabnames.add("${jsonDBman.dblinkheader}_base64")
            return
        }
        saveToDB(jsonDBman.dblinkheader, linkTableRow)

        //遍历数组
        DBCache.collectCache(jsonArray.toString(), jsonDBman.dblinkheader, linkTableKey.second, key.second)
        DBCache.collectCacheKeyPair(key)
        jsonArray.iterator().forEach {
            when {
                it.isJsonArray -> {
                    val subJsonArray = it.asJsonArray
                    tableRow.put(jsonDBman.dbarrays, link)
                    praseJsonArray(subJsonArray, elementName, link)
                }
                it.isJsonObject -> {
                    val subJsonObject = it.asJsonObject
                    tableRow.put(jsonDBman.dbarrays, link)
                    praseJsonObject(elementName, subJsonObject, tableRow)
                }
                else -> {
                    //其他处理
                    tableRow = praseJsonElement(tableRow, jsonDBman.dbarrays, it)
                    saveToDB(elementName, tableRow)
                }
            }

            //保存表
            DBCache.collectCache(it.toString(), elementName, jsonDBman.dbarrays, key.second)
        }
        cache_tabnames.add(elementName)
    }

    private fun getKey(tableName: String, mode: String): Pair<String, String> {
        val primaryKey = "${jsonDBman.dbprimarykey}_${tableName}_$mode"
        val primaryKeyValue = "${jsonDBman.dbprimarykeyid}_${UUID.randomUUID()}_${System.currentTimeMillis()}"
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
            put(jsonDBman.dbprimarykey, "${jsonDBman.thisname}_root")
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
            if (!(tname.equals("${jsonDBman.dblinkheader}_base64") || tname.equals(jsonDBman.dblinkheader) || isRoot)) {
                val sql: String
                if (joinedTableSql.keys.contains(tname)) {
                    val case = "[$tableName].[$tname]=[$tname].[${jsonDBman.dblinks}]"
                    val lastsql = joinedTableSql[tname]
                    if (!lastsql!!.contains(case)) {
                        sql = "${lastsql} OR $case\n"
                    } else {
                        sql = lastsql
                    }
                } else {
                    sql = "JOIN [$tname] ON [$tableName].[$tname]=[$tname].[${jsonDBman.dblinks}]\n"
                }
                joinedTableSql.put(tname, sql)
            }
        }

        val arrayTableSqlHead = "JOIN [${jsonDBman.dblinkheader}] ON"
        val arrayTableSqlCase = "[${jsonDBman.dblinkheader}].[link]="
        val arrayTableSql = StringBuilder()
        var orStr = ""
        joinedTableSql.keys.iterator().forEach {
            arrayTableSql.append(orStr)
            arrayTableSql.append(arrayTableSqlCase)
            arrayTableSql.append("[$it].[${jsonDBman.dblinks}]\n")
            orStr = " OR "
        }

        val sqlBuilder = StringBuilder(sqlHead)
        joinedTableSql.values.iterator().forEach {
            sqlBuilder.append(it)
        }
        if (joinedTableSql.isNotEmpty()) {
            sqlBuilder.append(arrayTableSqlHead)
            sqlBuilder.append(arrayTableSql)
        }
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
        saveingThread.execute {
            statement.execute(createTableSQL)
            fitTable(tableName, keys)
            statement.execute(insertIntoSQL)
        }
    }

    private fun getColumns(tableName: String): Array<String> {
        val rs =
            dbConn.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT
            ).executeQuery("SELECT [COLUMN_NAME] FROM [INFORMATION_SCHEMA].[COLUMNS] WHERE [TABLE_NAME]='$tableName'")
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
        ).executeQuery("SELECT * FROM [${jsonDBman.dblinkheader}] $whereSQL ")
    }

    fun getJsonElement(tableName: String, data: Map.Entry<String, String>): JsonElement? {
        //print("e:${data.key}:")
        val jsonKey = data.key
        val jsonValue = data.value
        DBCache.collectCacheKeyPair(Pair(jsonKey, jsonValue))
        when {
            jsonKey.equals(getPrimaryKey(tableName).first) || jsonKey.equals(jsonDBman.dblinks) -> {
                //print("%keyID%$jsonKey%")
                return null
            }
            jsonKey.equals(jsonDBman.dbarrays) -> {
                //print("%arrayID%$jsonKey%")
                return null
            }
            jsonValue.startsWith(jsonDBman.dbprimarykeyid) -> {
                val arrayTable = getArrayTable(jsonValue)
                if (arrayTable.last()) {
                    val arrayValue: String? = arrayTable.getString("${jsonDBman.dblinkheader}_value")
                    if (arrayValue != null && arrayValue.isNotBlank()) {
                        val base64Value = arrayTable.getString("${jsonDBman.dblinkheader}_value")
                        val base64 = String(Base64.decodeBase64(base64Value))
                        val jsonElement = JsonParser().parse(base64)
                        return jsonElement
                    }
                    val jsonArray = getJsonArray(jsonKey, jsonDBman.dblinks, jsonValue)
                    return jsonArray
                }
                //is JsonObject
                val jsonObject = getJsonObject(jsonKey, jsonDBman.dblinks, jsonValue)
                return jsonObject
            }
            else -> return JsonPrimitive(jsonValue)
        }
    }

    fun getJsonObject(tableName: String, key: String? = null, value: String? = null, lastIndexOf: Int = 0): JsonObject {
        //print("o($tableName)")
        val jsonObject = JsonObject()
        try {
            val rs = loadFromDB(tableName, key, value)
            if (rs.last()) {
                for (i in 0 until lastIndexOf) {
                    if (rs.isFirst || rs.isBeforeFirst || !rs.previous()) {
                        rs.first()
                        break
                    }
                }
                getMapFromRs(rs).iterator().forEach {
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

    fun getJsonArray(
        tableName: String,
        key: String? = null,
        value: String? = null,
        indexRange: LongRange = 0..Long.MAX_VALUE
    ): JsonArray {
        //print("a[$tableName]")
        val jsonArray = JsonArray()
        try {
            val rs = loadFromDB(tableName, key, value)
            var cnt: Long = 0L
            while (rs.next()) {
                if (!(cnt++ in indexRange)) {
                    break
                }
                val jsonObject = JsonObject()
                getMapFromRs(rs).iterator().forEach {
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

    fun getLastJsonArray(
        tableName: String,
        key: String? = null,
        value: String? = null,
        lastIndexRange: LongRange = 0..Long.MAX_VALUE
    ): JsonArray {
        try {
            val rs = loadFromDB(tableName, key, value)
            if (!rs.last()) {
                throw NullPointerException("dbEmpty")
            }
            val size = rs.row
            val range = size - lastIndexRange.endInclusive..size - lastIndexRange.start
            rs.close()
            return getJsonArray(tableName, key, value, range)
        } catch (e: Exception) {
            e.printStackTrace()
        }
        return getJsonArray(tableName, key, value)
    }

    private fun getMapFromRs(rs: ResultSet): Map<String, String> {
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
                k = "${jsonDBman.dbprimarykey}_${tableName}_ID"
                v = "${jsonDBman.dbprimarykeyid}_$v"
            }
            whereSQL += " [$k]='$v'"
        }
        if (!cols.contains(k)) {
            k = ""
            v = ""
            whereSQL = ""
        }
        DBCache.collectPath(Pair(tableName, k ?: ""))
        var sql = "SELECT * FROM [$tableName] $whereSQL"
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