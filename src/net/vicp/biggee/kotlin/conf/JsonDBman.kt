package net.vicp.biggee.kotlin.conf

class JsonDBman {
    val params = arrayOf(
        "tag",
        "thisname",
        "dbip",
        "dbuser",
        "dbpass",
        "dbparams",
        "dbname",
        "dbdriver",
        "dbprot",
        "dbcharset",
        "dbarrays",
        "dbprimarykey",
        "dbprimarykeyid",
        "dblinks",
        "dblinkheader"
    )
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
    var dbarrays = "${thisname}_${tag}_ARRAYS"
    var dbprimarykey = "${thisname}_${tag}"
    var dbprimarykeyid = "${thisname}_${tag}_ID"
    var dblinks = "${thisname}_${tag}_LINKS"
    var dblinkheader = "${dbprimarykeyid}_HEAD"

    override fun toString(): String {
        return "JsonDBman(params=${params.contentToString()}, tag='$tag', thisname='$thisname', dbip='$dbip', dbuser='$dbuser', dbpass='$dbpass', dbparams='$dbparams', dbname='$dbname', dbdriver='$dbdriver', dbprot='$dbprot', dbcharset='$dbcharset', dbarrays='$dbarrays', dbprimarykey='$dbprimarykey', dbprimarykeyid='$dbprimarykeyid', dblinks='$dblinks', dblinkheader='$dblinkheader')"
    }
}