package com.DFM.Interface.WordPress;

import com.DFM.Client.WebClient;
import com.DFM.Client.WordPressClient;
import com.DFM.Util.JsonUtil;
import com.DFM.Util.StringUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Mick on 2/12/2016.
 */
public class WordPressInterface {

    public static Map<String, String> getPost(String postBaseEndpoint, WordPressClient wpc) throws Exception {
        Map<String, String> resultMap = new HashMap<String, String>();
        String body;
        String postLocation = postBaseEndpoint;
        String wpPostId;
        try {
            body = wpc.get(postLocation);
            wpPostId = JsonUtil.getValue(body, "id");
            postLocation = postBaseEndpoint;

            resultMap.put("code", "200");
            resultMap.put("wpPostId", wpPostId);
            resultMap.put("postLocation", postLocation);
            resultMap.put("body", body);
        } catch (Exception e) {
            String errMsg = "Fatal GET error for: " + postBaseEndpoint + " Error: " + e.getMessage() + " Cause: " + e.getCause();
            throw new Exception(errMsg);
        }
        return resultMap;
    }

    public static Map<String, String> postJson(String json, String postBaseEndpoint, WordPressClient wpc) throws Exception {
        Map<String, String> resultMap;
        String postLocation = postBaseEndpoint;
        String wpPostId = "";

        try {
            resultMap = wpc.post(postLocation, json);
            if (WebClient.isOK(Integer.parseInt(resultMap.get("code").trim()))) {
                wpPostId = JsonUtil.getValue(resultMap.get("result"), "id");
                postLocation = resultMap.get("location");
            }

            if (WebClient.isBad(Integer.parseInt(resultMap.get("code").trim()))) {
                resultMap = wpc.post(postLocation, json);
                if (WebClient.isBad(Integer.parseInt(resultMap.get("code").trim()))) {
                    String errMsg = "Fatal post error for " + postLocation + " Code: " + resultMap.get("code") + " Response: " + resultMap.get("result") + " JSON: " + JsonUtil.toJSON(json);
                    throw new Exception(errMsg);
                }
            }
        } catch (Exception e) {
            String errMsg = "Fatal POST error for: " + postBaseEndpoint + " Error: " + e.getMessage() + " Cause: " + e.getCause();
            throw new Exception(errMsg);
        }
        resultMap.put("wpPostId", wpPostId);
        resultMap.put("postLocation", postLocation);
        return resultMap;
    }

    public static Map<String, String> postEndpoint(String postBaseEndpoint, WordPressClient wpc) throws Exception {
        String json = "{}";
        return postJson(json, postBaseEndpoint, wpc);
    }

    public static Map<String, String> postMedia(String imageJson, String mediaBaseEndpoint, WordPressClient wpc) throws Exception {
        Map<String, String> resultMap;
        //  String mediaEndpoint = mediaBaseEndpoint + "media/";
        String mediaLocation;
        String postLocation;
        String wpPostid;
        String imageName;
        String imageFeatured;
        String imageSource;
        String imageMimetype;
        String imageCaption;
        String imageAuthor;
        String imageDate;
        String wpImageId;
        String json;

        //Extract image metadata
        wpPostid = JsonUtil.getValue(imageJson, "post_id");
        postLocation = JsonUtil.getValue(imageJson, "postlocation");
        imageName = JsonUtil.cleanString(JsonUtil.getValue(imageJson, "name"));
        imageFeatured = JsonUtil.getValue(imageJson, "featured");
        imageSource = JsonUtil.getValue(imageJson, "source");
        imageMimetype = JsonUtil.getValue(imageJson, "mimetype");
        imageCaption = JsonUtil.cleanString(JsonUtil.getValue(imageJson, "caption"));
        imageAuthor = JsonUtil.getValue(imageJson, "author");
        imageDate = JsonUtil.getValue(imageJson, "date");
        //fix name
        imageName = StringUtil.hyphenateString(imageName);

        //Upload the image
        resultMap = wpc.uploadImage(mediaBaseEndpoint, imageSource, imageMimetype, imageName);
        if (WebClient.isOK(Integer.parseInt(resultMap.get("code").trim()))) {
            wpImageId = JsonUtil.getValue(resultMap.get("result"), "id");
            mediaLocation = mediaBaseEndpoint + wpImageId;
        } else {
            String errMsg = "Image post error for: " + imageName + " Code: " + resultMap.get("code") + " Response: " + resultMap.get("result");
            throw new Exception(errMsg);
        }

        //Add first image as featured image for post
        if (Boolean.valueOf(imageFeatured)) {
            json = "{\"id\":" + wpPostid + ",\"featured_media\":" + wpImageId + "}";
            resultMap = wpc.post(postLocation, json);
            if (WebClient.isBad(Integer.parseInt(resultMap.get("code").trim()))) {
                resultMap = wpc.post(postLocation, json);
                if (WebClient.isBad(Integer.parseInt(resultMap.get("code").trim()))) {
                    String errMsg = "Featured Image error for: " + imageName + " Code: " + resultMap.get("code") + " Response: " + resultMap.get("result");
                    throw new Exception(errMsg);
                }
            }
        }

        json = "{\"id\":" + wpImageId + ",\"author\":" + imageAuthor + ",\"title\": \"" + imageName + "\",\"date_gmt\": \"" + imageDate + "\",\"caption\": \"" + imageCaption + "\",\"post\":" + wpPostid + "}";
        resultMap = wpc.post(mediaLocation, json);
        if (WebClient.isBad(Integer.parseInt(resultMap.get("code").trim()))) {
            resultMap = wpc.post(mediaLocation, json);
            if (WebClient.isBad(Integer.parseInt(resultMap.get("code").trim()))) {
                String errMsg = "Image Metadata error for: " + imageName + " Code: " + resultMap.get("code") + " Response: " + resultMap.get("result");
                throw new Exception(errMsg);
            }
        }

        resultMap.put("wpImageId", wpImageId);
        resultMap.put("mediaLocation", mediaLocation);
        return resultMap;
    }

    public static Map<String, String> postDelete(String deleteEndpoint, WordPressClient wpc) throws Exception {
        Map<String, String> resultMap;
        try {
            resultMap = wpc.delete(deleteEndpoint);
            if (WebClient.isBad(Integer.parseInt(resultMap.get("code").trim()))) {
                String errMsg = "Fatal DELETE error for " + deleteEndpoint + " Code: " + resultMap.get("code") + " Response: " + resultMap.get("result");
                throw new Exception(errMsg);
            }
        } catch (Exception e) {
            String errMsg = "Fatal DELETE error for: " + deleteEndpoint + " Error: " + e.getMessage() + " Cause: " + e.getCause();
            throw new Exception(errMsg);
        }
        return resultMap;
    }
}
