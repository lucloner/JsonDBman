package net.vicp.biggee.kotlin.conf

object JsonDBman {
    var dbip="127.0.0.1"
    var dbuser="sa"
    var dbpass="1234567"
    var dbparams=""
    var dbname="JsonDBmanTest"
    var dbdriver="net.sourceforge.jtds.jdbc.Driver"
    var dbprot="jdbc:jtds:sqlserver"
    val connStr by lazy{
        "$dbprot ://$dbip /$dbname ,user=$dbuser ,password=$dbpass ${if(dbparams.isNotEmpty()) ",$dbparams" else ""}"
    }
}