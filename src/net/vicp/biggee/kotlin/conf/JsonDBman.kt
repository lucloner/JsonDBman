package net.vicp.biggee.kotlin.conf

import net.vicp.biggee.kotlin.db.JsonHelper

object JsonDBman {
    var tag = "JsonDBmanTAGS"
    var thisname = "JsonDBman"
    var dbip = "127.0.0.1"
    var dbuser = "sa"
    var dbpass = "1234567"
    var dbparams = ""
    var dbname = thisname
    var dbdriver = "net.sourceforge.jtds.jdbc.Driver"
    var dbprot = "jdbc:jtds:sqlserver"
    var dbcharset = "UTF-8"
    var dbarrays = "${thisname}_${tag}_ARRARYS"
    var dbprimarykey = "${thisname}_${tag}"
    var dbprimarykeyid = "${thisname}_${tag}_ID"
    var dblinks = "${thisname}_${tag}_LINKS"
    var dblinkheader = "${dbprimarykeyid}_ID"
    val connStr by lazy {
        if (dbdriver.isEmpty() || dbprot.isEmpty() || dbip.isEmpty() || dbname.isEmpty() || dbuser.isEmpty() || dbpass.isEmpty()) {
            throw NullPointerException("${thisname}_init_Empty")
        }
        var charset = ""
        if (!dbcharset.isEmpty()) {
            charset = "charset=$dbcharset;"
        }
        if (!JsonHelper.checkDB("$dbprot://$dbip;user=$dbuser;password=$dbpass", dbname)) {
            throw NullPointerException("${thisname}_init_NoDatabase")
        }
        "$dbprot://$dbip/$dbname;user=$dbuser;password=$dbpass;$charset;$dbparams"
    }
}