package net.vicp.biggee.kotlin.db

import net.vicp.biggee.kotlin.conf.JsonDBman
import java.sql.DriverManager

object JsonHelper {
    val dbConn by lazy {
        Class.forName(JsonDBman.dbdriver)
        DriverManager.getConnection(JsonDBman.connStr)
    }

}