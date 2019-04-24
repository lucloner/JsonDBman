package net.vicp.biggee.kotlin.tests

import com.google.gson.JsonParser
import net.vicp.biggee.kotlin.conf.JsonDBman
import net.vicp.biggee.kotlin.db.JsonHelper
import net.vicp.biggee.kotlin.json.DBHelper
import java.io.File

private class JsonDBman {
    fun t1() {
        JsonDBman.dbpass = "1Aalalala"
//        System.out.println(JsonHelper.dbConn)
    }

    fun t2() {
        t1()
        JsonHelper.saveJsonToDB(
            "t2again1",
            JsonParser().parse("{\"featureData\":null,\"role\":1,\"twistImageData\":\"[[B@abe240e]\",\"wiegandNo\":1,\"featureSize\":513,\"jpgFileContent\":null,\"thumbImageData\":\"[[B@73d690a8]\",\"name\":\"2\",\"expireDate\":\"LONGLIVE\",\"id\":\"1\",\"startDate\":\"DISABLED\",\"featureCount\":0,\"jpgFilePath\":null}").asJsonObject
        )
    }

    fun t3() {
        JsonDBman.dbname = "JT1"
        t1()
        JsonHelper.saveJsonToDB(
            "t3again4", JsonParser().parse(
                "{\"algorithmVersion\":\"HV6.0\",\"cloundConfigIp\":\"10.88.88.15\",\"criteria\":null,\"heartBeatInterval\":5,\"cloundConfigEnable\":1,\"repeatFilterTime\":1,\"uploadPhoto\":true,\"uploadFeature\":false,\"sensorModel\":\"imx327s\",\"GATEWAY\":\"0.0.0.0\",\"uploadFeatureImage\":true,\"codeVersion\":\"347\",\"addrNo\":\"\",\"hardwarePlatform\":\"Hi3516av200_4G\",\"ftpPassword\":null,\"IP\":\"10.88.88.31\",\"ageDetect\":false,\"deviceNo\":\"YJLAB01\",\"dataUploadPort\":10002,\"MAC\":\"EE:E8:A1:F8:B3:64\",\"ftpUserName\":null,\"doReg\":false,\"NETMASK\":\"255.255.255.0\",\"ensureThreshold\":70,\"workMode\":3,\"sysType\":\"FaceGate\",\"buildDate\":\"2019/02/25 16:43:40\",\"addrName\":\"\",\"faceDemoMinVersion\":70,\"total\":22,\"firewareVersion\":\"0.10.0beta_aliv\",\"cloundConfigPort\":10001,\"protocolVersion\":\"0.6\",\"sn\":\"01231B-B23241-B746EE\",\"uploadEnvironmentImage\":false,\"dataUploadUrl\":null,\"compareSwitch\":true,\"regUserName\":null,\"sexDetect\":false,\"dataUploadMethod\":1,\"sdkMinVersion\":\"v0.6.13\",\"livingDetect\":true,\"regPassword\":null,\"dataUploadServer\":\"10.88.88.15\",\"repeatFilter\":false,\"faces\":[{\"featureData\":null,\"role\":1,\"twistImageData\":\"[[B@abe240e]\",\"wiegandNo\":1,\"featureSize\":513,\"jpgFileContent\":null,\"thumbImageData\":\"[[B@73d690a8]\",\"name\":\"2\",\"expireDate\":\"LONGLIVE\",\"id\":\"1\",\"startDate\":\"DISABLED\",\"featureCount\":0,\"jpgFilePath\":null},{\"featureData\":null,\"role\":0,\"twistImageData\":\"[[B@7d2fef50]\",\"wiegandNo\":123456,\"featureSize\":513,\"jpgFileContent\":null,\"thumbImageData\":\"[[B@22244fb]\",\"name\":\"112\",\"expireDate\":\"LONGLIVE\",\"id\":\"12\",\"startDate\":\"DISABLED\",\"featureCount\":0,\"jpgFilePath\":null},{\"featureData\":null,\"role\":0,\"twistImageData\":\"[[B@41debe80]\",\"wiegandNo\":123456,\"featureSize\":513,\"jpgFileContent\":null,\"thumbImageData\":\"[[B@3abb638d]\",\"name\":\"113\",\"expireDate\":\"LONGLIVE\",\"id\":\"13\",\"startDate\":\"DISABLED\",\"featureCount\":0,\"jpgFilePath\":null},{\"featureData\":null,\"role\":0,\"twistImageData\":\"[[B@539c1fda]\",\"wiegandNo\":123456,\"featureSize\":513,\"jpgFileContent\":null,\"thumbImageData\":\"[[B@51cb2fdc]\",\"name\":\"114\",\"expireDate\":\"LONGLIVE\",\"id\":\"114\",\"startDate\":\"DISABLED\",\"featureCount\":0,\"jpgFilePath\":null},{\"featureData\":null,\"role\":0,\"twistImageData\":\"[[B@6c42b5a]\",\"wiegandNo\":123456,\"featureSize\":513,\"jpgFileContent\":null,\"thumbImageData\":\"[[B@325ab92f]\",\"name\":\"115\",\"expireDate\":\"LONGLIVE\",\"id\":\"115\",\"startDate\":\"DISABLED\",\"featureCount\":0,\"jpgFilePath\":null},{\"featureData\":null,\"role\":0,\"twistImageData\":\"[[B@3da8fd4b]\",\"wiegandNo\":123456,\"featureSize\":513,\"jpgFileContent\":null,\"thumbImageData\":\"[[B@e794191]\",\"name\":\"116\",\"expireDate\":\"LONGLIVE\",\"id\":\"116\",\"startDate\":\"DISABLED\",\"featureCount\":0,\"jpgFilePath\":null},{\"featureData\":null,\"role\":0,\"twistImageData\":\"[[B@7de5d6ea]\",\"wiegandNo\":123456,\"featureSize\":513,\"jpgFileContent\":null,\"thumbImageData\":\"[[B@5adca810]\",\"name\":\"117\",\"expireDate\":\"LONGLIVE\",\"id\":\"117\",\"startDate\":\"DISABLED\",\"featureCount\":0,\"jpgFilePath\":null},{\"featureData\":null,\"role\":0,\"twistImageData\":\"[[B@43ec85da]\",\"wiegandNo\":123456,\"featureSize\":513,\"jpgFileContent\":null,\"thumbImageData\":\"[[B@39d4650e]\",\"name\":\"118\",\"expireDate\":\"LONGLIVE\",\"id\":\"118\",\"startDate\":\"DISABLED\",\"featureCount\":0,\"jpgFilePath\":null},{\"featureData\":null,\"role\":0,\"twistImageData\":\"[[B@2f4ee27a]\",\"wiegandNo\":123456,\"featureSize\":513,\"jpgFileContent\":null,\"thumbImageData\":\"[[B@7c4f399f]\",\"name\":\"119\",\"expireDate\":\"LONGLIVE\",\"id\":\"119\",\"startDate\":\"DISABLED\",\"featureCount\":0,\"jpgFilePath\":null},{\"featureData\":null,\"role\":0,\"twistImageData\":\"[[B@12cd8fa2]\",\"wiegandNo\":123456,\"featureSize\":513,\"jpgFileContent\":null,\"thumbImageData\":\"[[B@35280e88]\",\"name\":\"120\",\"expireDate\":\"LONGLIVE\",\"id\":\"120\",\"startDate\":\"DISABLED\",\"featureCount\":0,\"jpgFilePath\":null},{\"featureData\":null,\"role\":0,\"twistImageData\":\"[[B@4f4ea204]\",\"wiegandNo\":123456,\"featureSize\":513,\"jpgFileContent\":null,\"thumbImageData\":\"[[B@174bdfa5]\",\"name\":\"121\",\"expireDate\":\"LONGLIVE\",\"id\":\"121\",\"startDate\":\"DISABLED\",\"featureCount\":0,\"jpgFilePath\":null},{\"featureData\":null,\"role\":0,\"twistImageData\":\"[[B@44a9814a]\",\"wiegandNo\":123456,\"featureSize\":513,\"jpgFileContent\":null,\"thumbImageData\":\"[[B@3ae7a92b]\",\"name\":\"122\",\"expireDate\":\"LONGLIVE\",\"id\":\"122\",\"startDate\":\"DISABLED\",\"featureCount\":0,\"jpgFilePath\":null},{\"featureData\":null,\"role\":0,\"twistImageData\":\"[[B@373205b1]\",\"wiegandNo\":123456,\"featureSize\":513,\"jpgFileContent\":null,\"thumbImageData\":\"[[B@dd6d88d]\",\"name\":\"123\",\"expireDate\":\"LONGLIVE\",\"id\":\"123\",\"startDate\":\"DISABLED\",\"featureCount\":0,\"jpgFilePath\":null},{\"featureData\":null,\"role\":0,\"twistImageData\":\"[[B@38b02805]\",\"wiegandNo\":123456,\"featureSize\":513,\"jpgFileContent\":null,\"thumbImageData\":\"[[B@309b4c8d]\",\"name\":\"124\",\"expireDate\":\"LONGLIVE\",\"id\":\"124\",\"startDate\":\"DISABLED\",\"featureCount\":0,\"jpgFilePath\":null},{\"featureData\":null,\"role\":0,\"twistImageData\":\"[[B@24210f71]\",\"wiegandNo\":123456,\"featureSize\":513,\"jpgFileContent\":null,\"thumbImageData\":\"[[B@3198d2e]\",\"name\":\"125\",\"expireDate\":\"LONGLIVE\",\"id\":\"125\",\"startDate\":\"DISABLED\",\"featureCount\":0,\"jpgFilePath\":null},{\"featureData\":null,\"role\":0,\"twistImageData\":\"[[B@77fd298f]\",\"wiegandNo\":123456,\"featureSize\":513,\"jpgFileContent\":null,\"thumbImageData\":\"[[B@4eb4c05c]\",\"name\":\"126\",\"expireDate\":\"LONGLIVE\",\"id\":\"126\",\"startDate\":\"DISABLED\",\"featureCount\":0,\"jpgFilePath\":null},{\"featureData\":null,\"role\":0,\"twistImageData\":\"[[B@1434704d]\",\"wiegandNo\":123456,\"featureSize\":513,\"jpgFileContent\":null,\"thumbImageData\":\"[[B@38c2aaa3]\",\"name\":\"127\",\"expireDate\":\"LONGLIVE\",\"id\":\"127\",\"startDate\":\"DISABLED\",\"featureCount\":0,\"jpgFilePath\":null},{\"featureData\":null,\"role\":0,\"twistImageData\":\"[[B@62c50bf4]\",\"wiegandNo\":123456,\"featureSize\":513,\"jpgFileContent\":null,\"thumbImageData\":\"[[B@7ceaf4b7]\",\"name\":\"128\",\"expireDate\":\"LONGLIVE\",\"id\":\"128\",\"startDate\":\"DISABLED\",\"featureCount\":0,\"jpgFilePath\":null},{\"featureData\":null,\"role\":0,\"twistImageData\":\"[[B@78fe8507]\",\"wiegandNo\":123456,\"featureSize\":513,\"jpgFileContent\":null,\"thumbImageData\":\"[[B@4f7f22bd]\",\"name\":\"129\",\"expireDate\":\"LONGLIVE\",\"id\":\"129\",\"startDate\":\"DISABLED\",\"featureCount\":0,\"jpgFilePath\":null},{\"featureData\":null,\"role\":0,\"twistImageData\":\"[[B@19caa2df]\",\"wiegandNo\":123456,\"featureSize\":513,\"jpgFileContent\":null,\"thumbImageData\":\"[[B@531a89e3]\",\"name\":\"130\",\"expireDate\":\"LONGLIVE\",\"id\":\"130\",\"startDate\":\"DISABLED\",\"featureCount\":0,\"jpgFilePath\":null}]}"
            ).asJsonObject
        )
//        JsonHelper.finnalize()
    }

    fun t4() {
        JsonDBman.dbname = "JT1"
        t1()
        val jsonElement = JsonHelper.getJsonElement(
            "t3again4"
        )
//        System.out.println(jsonElement.toString())
//        System.out.println(jsonElement.asJsonArray.size())
    }

    fun t5() {
        JsonDBman.dbname = "JT1"
        t1()
        val jsonElement = JsonHelper.getJsonArray(
            "t3again4"
        )
//        System.out.println(jsonElement.toString())
//        System.out.println(jsonElement.asJsonArray.size())
    }

    fun t6() {
        JsonDBman.dbname = "JT1"
        t1()
        val jsonElement = DBHelper.getLatestJsonArrayFromDB("t3again4", "deviceNo")
//        System.out.println(jsonElement.toString())
//        System.out.println(jsonElement.asJsonArray.size())
    }

    fun t7() {
        JsonDBman.dbname = "JT1"
        t1()
        val jsonElement = DBHelper.getLatestJsonArrayFromDB("t3again4", "deviceNo1")
//        System.out.println(jsonElement.toString())
//        System.out.println(jsonElement.asJsonArray.size())
    }

    fun t8() {
        val now = System.currentTimeMillis()
        val strJson = File("/home/lucloner/source/hajava/SDKServerDemo/testjson").readText()
        val jsonElement = JsonParser().parse(strJson)
        val jsonArray = jsonElement.asJsonArray
        val jsonObject = jsonArray.first().asJsonObject
        println("测试输出:${strJson.length}\t${jsonObject["deviceNo"].asString}")
//        val faces=jsonObject["faces"].asJsonArray
//        val facesArray= JsonArray()
//        faces.iterator().forEach {
//            val facesObject=it.asJsonObject
//            val bytesJson=facesObject["thumbImageData"].asJsonArray
//            val bytesJsonCnt=it.toString().length
//            val bytesCnt=bytesJson.size()
//            println("总数组数量:$bytesCnt\t字符串长度:$bytesJsonCnt\t${bytesJsonCnt/bytesCnt}")
//            val base64Array= JsonArray()
//            bytesJson.forEach {
//                val byteJson=it.toString().length
//                val bytes= Gson().fromJson<ByteArray>(it,ByteArray::class.java)
//                val byteCnt=bytes.size
//                val base64= Base64.getEncoder().encodeToString(bytes)
//                base64Array.add(base64)
//                println("单个数组数量:$byteCnt\t字符串长度:$byteJson\t${byteJson/byteCnt}")
//            }
//
//            facesObject.add("thumbImageData",base64Array)
//            facesArray.add(facesObject)
//        }
//        jsonObject.add("faces",facesArray)
        t1()
        JsonHelper.saveJsonToDB("t3again4", jsonObject)
        println("耗时${System.currentTimeMillis() - now}ms")
    }

    fun t9() {
        val now = System.currentTimeMillis()
        t1()
        val j = JsonHelper.getJsonObject("t3again4")
        println("${j}耗时${System.currentTimeMillis() - now}ms")
    }
}

fun main(args: Array<String>) {
    net.vicp.biggee.kotlin.tests.JsonDBman().t8()
//    net.vicp.biggee.kotlin.tests.JsonDBman().t9()
}