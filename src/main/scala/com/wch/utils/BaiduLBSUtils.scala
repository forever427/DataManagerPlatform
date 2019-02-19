package com.wch.utils

import java.io.UnsupportedEncodingException
import java.net.URLEncoder
import java.security.NoSuchAlgorithmException
import java.util

import com.google.gson.{JsonArray, JsonObject, JsonParser}
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.commons.lang.StringUtils


object BaiduLBSUtils {
  def main(args: Array[String]): Unit = {
    var business: String = ""
    val requestParams = requetParams("116.411024","39.946112")
    val requestURL: String = "http://api.map.baidu.com/geocoder/v2/?" + requestParams
    val httpClient = new HttpClient()
    val getMethod = new GetMethod(requestURL)
    val statusCode: Int = httpClient.executeMethod(getMethod)
    if (statusCode == 200) {
      val response: String = getMethod.getResponseBodyAsString
      println(response)



    }

  }

  def parseBusinessTagBy(lng: String, lat: String) = {
    var business: String = ""
    val requestParams = requetParams(lng, lat)
    val requestURL: String = "http://api.map.baidu.com/geocoder/v2/?" + requestParams

    val httpClient = new HttpClient()
    val getMethod = new GetMethod(requestURL)
    val statusCode: Int = httpClient.executeMethod(getMethod)
    if (statusCode == 200) {
      val response: String = getMethod.getResponseBodyAsString
      // 判断是否是合法的json 字符串
      var str: String = response.replaceAll("renderReverse&&renderReverse\\(", "")
      if (!response.startsWith("{")) {
        str = str.substring(0, str.length - 1)
      }

      val returnData: JsonObject = new JsonParser().parse(str).getAsJsonObject
      val status: Int = returnData.get("status").getAsInt
      if (status == 0) {
        val resultObject: JsonObject = returnData.getAsJsonObject("result")
        business = resultObject.get("business").getAsString.replaceAll(",", ";")

        if (StringUtils.isEmpty(business)) {
          val pois: JsonArray = resultObject.getAsJsonArray("pois")
          var tagSet: Set[String] = Set[String]()
          for (i <- 0 until pois.size()) {
            val elemObject: JsonObject = pois.get(i).getAsJsonObject
            val tag: String = elemObject.get("tag").getAsString
            if (StringUtils.isNotEmpty(tag)) tagSet += tag
          }
          business = tagSet.mkString(";")
        }
      }
    }
    business
  }

  def requetParams(lng: String, lat: String) = {
    // AK：je232bv71YlQQsd2KeuG6FvSamq3kzGa
    // SK：Rv3f5aOQbSjiHXCuIY0zrDgGDtEKk0ip
    val ak = "je232bv71YlQQsd2KeuG6FvSamq3kzGa"
    val sk = "Rv3f5aOQbSjiHXCuIY0zrDgGDtEKk0ip"
    // 计算sn跟参数对出现顺序有关，get请求请使用LinkedHashMap保存<key,value>，该方法根据key的插入顺序排序；
    // post请使用TreeMap保存<key,value>，该方法会自动将key按照字母a-z顺序排序。
    // 所以get请求可自定义参数顺序（sn参数必须在最后）发送请求，但是post请求必须按照字母a-z顺序填充body（sn参数必须在最后）。
    // 以get请求为例：http://api.map.baidu.com/geocoder/v2/?address=百度大厦&output=json&ak=yourak，
    // paramsMap中先放入address，再放output，然后放ak，放入顺序必须跟get请求中对应参数的出现顺序保持一致。
    val paramsMap = new util.LinkedHashMap[String, String]()
    paramsMap.put("callback", "renderReverse")
    // paramsMap.put("address", "百度大厦")
    paramsMap.put("location", lat.concat(",").concat(lng))
    paramsMap.put("output", "json")
    paramsMap.put("pois", "1")
    paramsMap.put("ak", ak)
    // 调用下面的toQueryString方法，对LinkedHashMap内所有value作utf8编码，拼接返回结果address=%E7%99%BE%E5%BA%A6%E5%A4%A7%E5%8E%A6&output=json&ak=yourak
    val paramsStr: String = toQueryString(paramsMap)
    // 对paramsStr前面拼接上/geocoder/v2/?，后面直接拼接yoursk得到/geocoder/v2/?address=%E7%99%BE%E5%BA%A6%E5%A4%A7%E5%8E%A6&output=json&ak=yourakyoursk
    val wholeStr = new String("/geocoder/v2/?" + paramsStr + sk)
    // 对上面wholeStr再作utf8编码
    val tempStr: String = URLEncoder.encode(wholeStr, "UTF-8")
    // 调用下面的MD5方法得到最后的sn签名7de5a22212ffaa9e326444c75a58f9a0
    val sn: String = MD5(tempStr)

    paramsStr + "&sn=" + sn
  }

  // 对Map内所有value作utf8编码，拼接返回结果
  @throws[UnsupportedEncodingException]
  def toQueryString(data: util.LinkedHashMap[String, String]): String = {
    val queryString = new StringBuffer
    import scala.collection.JavaConversions._
    for (pair <- data.entrySet) {
      queryString.append(pair.getKey + "=")
      queryString.append(URLEncoder.encode(pair.getValue.asInstanceOf[String], "UTF-8") + "&")
    }
    if (queryString.length > 0) queryString.deleteCharAt(queryString.length - 1)
    queryString.toString
  }

  // 来自stackoverflow的MD5计算方法，调用了MessageDigest库函数，并把byte数组结果转换成16进制
  def MD5(md5: String): String = {
    try {
      val md = java.security.MessageDigest.getInstance("MD5")
      val array: Array[Byte] = md.digest(md5.getBytes)
      val sb = new StringBuffer
      var i = 0
      while ( {
        i < array.length
      }) {
        sb.append(Integer.toHexString((array(i) & 0xFF) | 0x100).substring(1, 3))

        {
          i += 1;
          i
        }
      }
      return sb.toString
    } catch {
      case e: NoSuchAlgorithmException =>

    }
    null
  }
}
