package com.DFM.Util;

import com.google.gson.Gson;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class JsonUtil {
    public static String toJSON(Object obj) {
        Gson gson = new Gson();
        String json = gson.toJson(obj);
        //System.out.println(json);
        return removeUnicode(json);
    }


    @SuppressWarnings("unchecked")
    public static <T> Object fromJSON(String json, Object obj) {
        Gson gson = new Gson();
        return gson.fromJson(json, (Class<T>) obj);
    }

    public static String getValue(String json, String key) throws ParseException {
        JSONObject jsonObj = null;
        JSONArray jsonArray = null;
        String value = "";

        try {

            Object obj = new JSONParser().parse(json);
            if (obj instanceof JSONObject) {
                jsonObj = (JSONObject) obj;
            } else {
                jsonArray = (JSONArray) obj;
                jsonObj = (JSONObject) jsonArray.get(0);
            }

            value = jsonObj.get(key).toString();
        } catch (ParseException e) {
            if (1 == 11) System.out.println("BAD JSON:" + json);
            throw e;
        }
        return value;
    }

    public static String removeUnicode(String string) {
        String cleanString = StringUtil.removeUnicode(string);
        cleanString = cleanString.replaceAll("\u003c", "<");
        cleanString = cleanString.replaceAll("\u003e", ">");
        cleanString = cleanString.replaceAll("\\\\u003c", "<");
        cleanString = cleanString.replaceAll("\\\\u003e", ">");
        return cleanString;
    }

    public static String cleanString(String string) {
        String cleanString = string;
        cleanString = cleanString.replace("\\\\", "\\"); //unescape escaped \ to avoid double escaping
        cleanString = cleanString.replace("\\", "\\\\"); //escape unescaped \
        cleanString = cleanString.replace("\\\"", "\""); //unescape escaped " to avoid double escaping
        cleanString = cleanString.replace("\"", "\\\""); //escape unescaped "
        return cleanString;
    }
}
