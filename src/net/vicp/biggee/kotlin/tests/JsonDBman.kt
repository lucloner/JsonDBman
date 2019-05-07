package net.vicp.biggee.kotlin.tests

import com.google.gson.JsonParser
import net.vicp.biggee.kotlin.conf.JsonDBman
import net.vicp.biggee.kotlin.db.JsonHelper

private class JsonDBman {
    val json =
        "{\"featureData\":null,\"role\":1,\"twistImageData\":\"[[B@abe240e]\",\"wiegandNo\":1,\"featureSize\":513,\"jpgFileContent\":null,\"thumbImageData\":\"[[B@73d690a8]\",\"name\":\"2\",\"expireDate\":\"LONGLIVE\",\"id\":\"1\",\"startDate\":\"DISABLED\",\"featureCount\":0,\"jpgFilePath\":null}"
    val jsonObject = JsonParser().parse(json).asJsonObject

    fun demo() {
        JsonDBman.dbpass = "1Aalalala"
        JsonHelper.saveJsonToDB("demo", jsonObject)
        val jsonObject = JsonHelper.getJsonObject("demo")
        System.out.println(jsonObject)
    }
}

fun main(args: Array<String>) {
    net.vicp.biggee.kotlin.tests.JsonDBman().demo()
}