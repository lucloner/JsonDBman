package net.vicp.biggee.kotlin.conf

import net.vicp.biggee.kotlin.db.JsonHelper

object JsonDBman {
    var dbip = "127.0.0.1"
    var dbuser = "sa"
    var dbpass = "1234567"
    var dbparams = ""
    var dbname = "JsonDBman"
    var dbdriver = "net.sourceforge.jtds.jdbc.Driver"
    var dbprot = "jdbc:jtds:sqlserver"
    var dbcharset = "UTF-8"
    var tag = "JsonDBmanTAGS"
    var dbarrays = "JsonDBman_${tag}_ARRARYS"
    var dblinks = "JsonDBman_${tag}_LINKS"
    val connStr by lazy {
        if (dbdriver.isEmpty() || dbprot.isEmpty() || dbip.isEmpty() || dbname.isEmpty() || dbuser.isEmpty() || dbpass.isEmpty()) {
            throw NullPointerException("JsonDBman_init_Empty")
        }
        var charset = ""
        if (!dbcharset.isEmpty()) {
            charset = "charset=$dbcharset;"
        }
        if (!JsonHelper.checkDB("$dbprot://$dbip;user=$dbuser;password=$dbpass", dbname)) {
            throw NullPointerException("JsonDBman_init_NoDatabase")
        }
        "$dbprot://$dbip/$dbname;user=$dbuser;password=$dbpass;$charset;$dbparams"
    }
}