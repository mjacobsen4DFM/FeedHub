package com.DFM.FeedHub.Storm.Bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.DFM.Client.*;
import com.DFM.Interface.WordPress.WordPressInterface;
import com.DFM.Model.Helper.Publisher;
import com.DFM.Model.Story.StorySubjects;
import com.DFM.Model.WordPress.Image;
import com.DFM.Model.WordPress.Images;
import com.DFM.Model.WordPress.WordPressPost;
import com.DFM.Util.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class WordPressBolt extends BaseRichBolt {
    private static final long serialVersionUID = 1L;

    private static Publisher _publisher = new Publisher();
    private static RedisClient _redisClient = new RedisClient();
    private static String _xsltRootPath = "";
    private static String _sStory = "";
    private static String _feedKey = "";
    private static String _contentKey = "";
    private static String _subscriber = "";
    private static String _deliveredStoryKey = "";
    private static String _sStorySubjects = "";
    private static Map<String, String> _subscriberMap = new HashMap<String, String>();
    private static String[] _keyArgs = {};

    private static Tuple _tuple;
    private static Map<String, Object> _stormConf;
    private static OutputCollector _collector;

    private static void configure(Map conf) throws Exception {
        //Get feed and article objects
        Map<String, String> mainConf = (Map) conf.get("mainConf");
        _redisClient = new RedisClient(mainConf);
        _xsltRootPath = mainConf.get("xsltRootPath");
        _contentKey = (String) conf.get("contentKey");
        _subscriber = (String) conf.get("subscriber");
        _subscriberMap = (Map<String, String>) StormUtil.deserialize((byte[]) conf.get("subscriberMap"));
        _sStory = (String) conf.get("story");
        _sStorySubjects = (String) conf.get("sStorySubjects");
        _feedKey = (String) conf.get("feedKey");
        _publisher = new Publisher((byte[]) conf.get("publisher"));
        _keyArgs = new String[]{"delivered", _subscriberMap.get("type"), _subscriberMap.get("name"), _publisher.getSource(), _contentKey};
        _deliveredStoryKey = ContentControl.setKey(_keyArgs);

    }

    private static void run() throws Exception {
        String operation = "start";
        Map<String, Object> storyDataMap = new HashMap<String, Object>();
        Map<String, String> resultMap = new HashMap<String, String>();
        String wpPostId = "";
        String postLocation = "";
        String postEndpoint = "";
        String storyTitle = "";
        WordPressClient wpc = new WordPressClient();

        try {
            if (2 == 12) {
                System.out.println("getFeedType: " + _publisher.getFeedType() + " deliveredStoryKey: " + _deliveredStoryKey);
            }
            String authorId = _subscriberMap.get("userid");
            String baseURI = _subscriberMap.get("api_url");
            String postBaseEndpoint = baseURI + "posts/";
            String mediaBaseEndpoint = baseURI + "media/";

            operation = "Transform XML";
            String xsltPath = _xsltRootPath + _publisher.getFeedType() + "_" + _subscriberMap.get("type") + ".xslt";
            String wppXML = XmlUtil.transform(_sStory, xsltPath);
            WordPressPost wpp = WordPressPost.fromXML(wppXML);

            storyTitle = wpp.getTitle();

            operation = "Get Story data";
            storyDataMap = getStoryData(wpp, wppXML);

            operation = "Get WordPress Client";
            wpc = NewClient();

            if ((Boolean) storyDataMap.get("isNew")) {
                operation = "Set Category";
                ArrayList<Integer> categories = new ArrayList<Integer>();
                categories.add(Integer.parseInt(_subscriberMap.get("catid")));
                wpp.setCategories(categories);

                operation = "Set Tags";
                ArrayList<Integer> tags = setTags();
                wpp.setTags(tags);

                operation = "Set Author";
                wpp.setAuthor(authorId);

                operation = "Post Story";
                resultMap = postStory(wpp, storyDataMap, postBaseEndpoint, wpc);

                operation = "Gather Post Data";
                wpPostId = resultMap.get("wpPostId");
                wpp.setID(wpPostId);
                postLocation = resultMap.get("postLocation");

                operation = "Post Images";
                resultMap = postImages(wpPostId, postLocation, authorId, wpp, mediaBaseEndpoint, wpc);

                operation = "Post Meta";
                resultMap = postMetaData(postLocation, storyDataMap, wpc);

                operation = "All clear";
                System.out.println("Insert into: " + _subscriberMap.get("name") + " for: \"" + wpp.getTitle() + "\" at: " + postLocation);
                ack();
            } else {
                operation = "Post Category (additional)";
                wpPostId = (String) storyDataMap.get("id");
                postLocation = postBaseEndpoint + wpPostId;
                String json = "{\"id\":" + wpPostId + ",\"categories\": [" + _subscriberMap.get("catid") + "] }";
                resultMap = postCategory(postLocation, storyDataMap, wpc);
                ack();
            }
        } catch (Exception e) {
            String errMsg = "Operation: " + operation + ", " + e.toString() + " into: " + postEndpoint;
            String wpMsg = "";
            if (resultMap.get("result") != null) {
                if (resultMap.get("result").startsWith("[")) {
                    wpMsg = JsonUtil.getValue(resultMap.get("result").replace("[", "").replace("]", ""), "message");
                } else if (resultMap.get("result").startsWith("<")) {
                    wpMsg = resultMap.get("result");
                    errMsg += "; Wordpress says, \"" + wpMsg + "\"; For Url: " + wpc.getHost() + "/" + storyDataMap.get("id") + "; Resetting feedKey: " + _feedKey + "; for storyKey: " + _contentKey + "; deliveredStoryKey: " + _deliveredStoryKey;
                } else {
                    wpMsg = resultMap.get("result");
                    errMsg += "; Wordpress says, \"" + wpMsg;
                }
                errMsg += "; Wordpress says, \"" + wpMsg + "\"; For Url: " + wpc.getHost() + "/" + storyDataMap.get("id") + "; Resetting feedKey: " + _feedKey + "; for storyKey: " + _contentKey + "; deliveredStoryKey: " + _deliveredStoryKey;

            } else {
                errMsg += "; Wordpress didn't provide a reason for the error.";
            }
            fail(errMsg);
        }
    }

    private static Map<String, Object> getStoryData(WordPressPost wpp, String wppXML) throws Exception {
        String keyCheck = "key";
        String valCheck = "value";
        String typeCheck = "type";
        if (_publisher.getUpdateCheckType().equalsIgnoreCase("content")) {
            byte[] b5 = StormUtil.serialize(wpp.getContent());
            String s5 = new String(b5, StandardCharsets.UTF_8);
            String m5 = ContentControl.createMD5(s5);
            keyCheck = "md5";
            valCheck = m5;
            typeCheck = "content";
        } else if (_publisher.getUpdateCheckType().equalsIgnoreCase("sequence")) {
            keyCheck = "sequence";
            valCheck = XmlUtil.getNodeValue(wppXML, "sequence");
            typeCheck = XmlUtil.getAttributeValue(wppXML, "sequence", "type");
        }
        return ContentControl.createStoryStatusMap(_deliveredStoryKey, keyCheck, valCheck, typeCheck, _redisClient);
    }

    private static WordPressClient NewClient() throws Exception {
        if (_subscriberMap.get("accessType").equalsIgnoreCase("OAUTH1")) {
            if (2 == 12) {
                System.out.println("DISCOVERED OAUTH: '" + _subscriberMap.get("accessType") + "'");
            }
            return new WordPressOauth1Client(_subscriberMap.get("api_url"), _subscriberMap.get("AT"), _subscriberMap.get("AS"), _subscriberMap.get("CK"), _subscriberMap.get("CS"));
        } else if (_subscriberMap.get("accessType").equalsIgnoreCase("BASIC")) {
            if (2 == 12)
                System.out.println("DISCOVERED BASIC: '" + _subscriberMap.get("accessType").toUpperCase() + "'");
            return new WordPressBasicClient(_subscriberMap.get("api_url"), _subscriberMap.get("stamp"), _subscriberMap.get("validity"));
        } else {
            if (3 == 13) {
                System.out.println("DISCOVERED NOTHING: '" + _subscriberMap.get("accessType").toUpperCase() + "'");
            }
            throw new Exception("WordPressClient type is unknown. Access type requested: " + _subscriberMap.get("accessType"));
        }
    }

    private static Map<String, String> postStory(WordPressPost wpp, Map<String, Object> storyDataMap, String postBaseEndpoint, WordPressClient wpc) throws Exception {
        Map<String, String> resultMap = new HashMap<String, String>();
        String postLocation = postBaseEndpoint;
        String wpPostId = "";
        if ((Boolean) storyDataMap.get("isUpdated")) {
            wpPostId = (String) storyDataMap.get("id");
            wpp.setID(wpPostId);
            postLocation = postBaseEndpoint + wpPostId;
        }

        String json = JsonUtil.toJSON(wpp);
        resultMap = WordPressInterface.postJson(json, postLocation, wpc);
        resultMap.put("title", wpp.getTitle());
        if (WebClient.isOK(Integer.parseInt(resultMap.get("code").trim()))) {
            recordPost(resultMap, storyDataMap);
        }

        //NOT catching exceptions, because if the story doesn't post, the rest of the objects don't matter.

        return resultMap;
    }

    private static Map<String, String> postMetaData(String postLocation, Map<String, Object> storyDataMap, WordPressClient wpc) {
        Map<String, String> resultMap = new HashMap<String, String>();
        String postEndpoint = "";
        String json = "";
        String key = "";
        Object value = "";

        try {
            postEndpoint = postLocation + "/meta";

            Map<String, String> metaHashMap = new HashMap<String, String>();
            if (!((Boolean) storyDataMap.get("isUpdated"))) {
                metaHashMap.put("deliveredStoryKey", _deliveredStoryKey);
            }
            metaHashMap.put("feedKey", _feedKey);

            for (Map.Entry<String, String> metaEntry : metaHashMap.entrySet()) {
                key = metaEntry.getKey();
                value = metaEntry.getValue();
                json = "{ \"key\":\"" + key + "\",\"value\":\"" + value + "\" }";
                resultMap = WordPressInterface.postJson(json, postEndpoint, wpc);
            }
        } catch (Exception e) {
            String errMsg = "Fatal meta post error for: " + key + "(" + value + ")" + " into Subscriber: " + _subscriberMap.get("name") + " at: " + postLocation + " for contentKey: " + _contentKey + " from feedKey: " + _feedKey + " Message: " + e.getMessage();
            StormUtil.logError(errMsg, _redisClient);
        }
        return resultMap;
    }

    private static Map<String, String> postCategory(String postLocation, Map<String, Object> storyDataMap, WordPressClient wpc) {
        Map<String, String> resultMap = new HashMap<String, String>();
        String catId = "";
        String catName = "";
        String postEndpoint = "";
        String msg = "";

        try {
            catId = _subscriberMap.get("catid");
            catName = _subscriberMap.get("catname");
            if (catId != null) {
                String[] catKeyArgs = new String[]{_deliveredStoryKey, "category", catId};
                String catKey = ContentControl.setKey(catKeyArgs);
                if (!ContentControl.exists(catKey, "id", _redisClient)) {
                    Map<String, String> catHashMap = new HashMap<String, String>();
                    catHashMap.put("id", catId);
                    catHashMap.put("name", catName);
                    ContentControl.trackContent(catKey, catHashMap, _redisClient);
                    postEndpoint = postLocation + "/categories/" + catId;

                    resultMap = WordPressInterface.postEndpoint(postEndpoint, wpc);

                    msg = "Update into: " + _subscriberMap.get("name") + " for: \"" + storyDataMap.get("title") + "\" at: " + postLocation;
                    resultMap.put("message", msg);
                }
            }
        } catch (Exception e) {
            String errMsg = "Fatal subject post error into: " + _subscriberMap.get("name") + " at " + postLocation + " for: " + catName + " error: " + e.getMessage();
            StormUtil.logError(errMsg, _redisClient);
        }
        return resultMap;
    }

    private static Map<String, String> postTags(String postLocation, WordPressClient wpc) {
        Map<String, String> resultMap = new HashMap<String, String>();
        String tagId = "";
        String postEndpoint = "";

        try {
            String tagKeyRoot = "taxonomy:" + _subscriberMap.get("type") + ":" + _subscriberMap.get("name") + ":" + _publisher.getFeedType() + ":tag:";
            String tagKey = "";
            StorySubjects storyTags = StorySubjects.fromXML(_sStorySubjects);
            String[] tags = storyTags.getSubjects();
            if (tags != null && tags.length > 0) {
                for (String tag : tags) {
                    tagKey = tagKeyRoot + tag;
                    if (2 == 9) System.out.println("tagKey: " + tagKey);
                    tagId = _redisClient.hget(tagKey, "id");
                    if (tagId != null) {
                        //System.out.println("TAG_ID: " + tagId);
                        postEndpoint = postLocation + "/tags/" + tagId;
                        resultMap = WordPressInterface.postEndpoint(postEndpoint, wpc);
                    }
                }
            }
        } catch (Exception e) {
            String errMsg = "Fatal tag post error into: " + _subscriberMap.get("name") + " at " + postLocation + " for tagId: " + tagId + " error: " + e.getMessage();
            StormUtil.logError(errMsg, _redisClient);
        }
        return resultMap;
    }

    private static ArrayList<Integer> setTags() {
        Map<String, String> resultMap = new HashMap<String, String>();
        ArrayList<Integer> tagList = new ArrayList<Integer>();
        String tagKey = "";
        String tagId = "";
        String postEndpoint = "";

        try {
            String tagKeyRoot = "taxonomy:" + _subscriberMap.get("type") + ":" + _subscriberMap.get("name") + ":" + _publisher.getFeedType() + ":tag:";
            StorySubjects storyTags = StorySubjects.fromXML(_sStorySubjects);
            String[] tags = storyTags.getSubjects();
            if (tags != null && tags.length > 0) {
                for (String tag : tags) {
                    tagKey = tagKeyRoot + tag;
                    if (2 == 9) System.out.println("tagKey: " + tagKey);
                    tagId = _redisClient.hget(tagKey, "id");
                    if (tagId != null) {
                        tagList.add(Integer.parseInt(tagId));
                    }
                }
            }
        } catch (Exception e) {
            String errMsg = "Fatal tag set error into: " + _subscriberMap.get("name") + " for tagId: " + tagId + " error: " + e.getMessage();
            StormUtil.logError(errMsg, _redisClient);
        }
        return tagList;
    }

    private static Map<String, String> postImages(String wpPostid, String postLocation, String authorId, WordPressPost wpp, String mediaBaseEndpoint, WordPressClient wpc) {
        Map<String, String> resultMap = new HashMap<String, String>();
        //String mediaEndpoint = "";
        String mediaLocation = "";
        Image image = null;
        String imageName = "";
        String wpImageId = "";
        String imageKey = "";
        String json = "";
        Boolean bFirst = false;

        try {
            Images[] images = wpp.getImages();
            if (images != null && images.length != 0) {
                //Load/get images
                for (int i = 0; i < images.length; i++) {
                    bFirst = (i == 0);
                    image = images[i].getImage();
                    if (image != null) {
                        if (ContentControl.isNew(imageKey, "id", _redisClient)) {
                            imageName = StringUtil.hyphenateString(image.getName());
                            json = "{ \"post_id\":" + wpPostid + ", \"postlocation\":\"" + postLocation + "\", \"name\":\"" + imageName + "\", \"source\":\"" + image.getSource() + "\", \"mimetype\":\"" + image.getMimetype() + "\", \"caption\":\"" + image.getCaption() + "\", \"featured\":\"" + bFirst + "\", \"author\":\"" + authorId + "\", \"date\":\"" + wpp.getDate() + "\" }";
                            resultMap = WordPressInterface.postMedia(json, mediaBaseEndpoint, wpc);
                            resultMap = wpc.uploadImage(mediaBaseEndpoint, image.getSource(), image.getMimetype(), imageName);
                            if (WebClient.isOK(Integer.parseInt(resultMap.get("code").trim()))) {
                                recordImage(wpPostid, resultMap, image, "false");
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            String errMsg = "Fatal image post error for: " + imageName + "(" + image.getGuid() + ")" + " into Subscriber: " + _subscriberMap.get("name") + " at: " + postLocation + " for contentKey: " + _contentKey + " from feedKey: " + _feedKey + " Code: " + resultMap.get("code") + " Response: " + resultMap.get("result");
            StormUtil.logError(errMsg, _redisClient);
        }
        return resultMap;
    }

    private static Map<String, String> postAuthor(String wpPostId, String postLocation, String authorId, WordPressClient wpc) {
        Map<String, String> resultMap = new HashMap<String, String>();
        String json = "{\"id\":" + wpPostId + ",\"author\":" + authorId + "}";

        try {
            resultMap = wpc.post(postLocation, json);
            if (WebClient.isBad(Integer.parseInt(resultMap.get("code").trim()))) {
                resultMap = wpc.post(postLocation, json);
                if (WebClient.isBad(Integer.parseInt(resultMap.get("code").trim()))) {
                    String errMsg = "Final author post error for: " + authorId + " into Subscriber: " + _subscriberMap.get("name") + " at: " + postLocation + " for contentKey: " + _contentKey + " from feedKey: " + _feedKey + " Code: " + resultMap.get("code") + " Response: " + resultMap.get("result");
                    fail(errMsg);
                }
            }
        } catch (IOException e) {
            String errMsg = "Fatal author post error into: " + _subscriberMap.get("name") + " at " + postLocation + " for: " + authorId + " error: " + e.getMessage();
            StormUtil.logError(errMsg, _redisClient);
        }
        return resultMap;
    }

    private static void recordPost(Map<String, String> storyPostMap, Map<String, Object> storyDataMap) {
        Map<String, String> postMap = new HashMap<String, String>();
        try {
            postMap.put("id", storyPostMap.get("wpPostId"));
            postMap.put("location", storyPostMap.get("postLocation"));
            postMap.put("title", storyPostMap.get("title"));
            postMap.put((String) storyDataMap.get("keyCheck"), (String) storyDataMap.get("valueCheck"));
            ContentControl.trackContent(_deliveredStoryKey, postMap, _redisClient);
        } catch (Exception e) {
            postMap.put("error", e.getMessage());
        }
    }

    private static void recordImage(String wpPostid, Map<String, String> imagePostMap, Image image, String complete) {
        Map<String, String> imageHashMap = new HashMap<String, String>();
        try {
            String wpImageId = JsonUtil.getValue(imagePostMap.get("result"), "id");
            String mediaLocation = JsonUtil.getValue(imagePostMap.get("result"), "id");
            String[] imageKeyArgs = {_deliveredStoryKey, "image", image.getGuid()};
            String imageKey = ContentControl.setKey(imageKeyArgs);
            imageHashMap.put("guid", image.getGuid());
            imageHashMap.put("location", mediaLocation);
            imageHashMap.put("id", wpImageId);
            imageHashMap.put("postid", wpPostid);
            imageHashMap.put("name", image.getName());
            imageHashMap.put("complete", complete);
            ContentControl.trackContent(imageKey, imageHashMap, _redisClient);
        } catch (Exception e) {
            imageHashMap.put("error", e.getMessage());
        }
    }

    private static void ack() {
        if (_tuple != null) {
            _collector.ack(_tuple);
        }
    }

    private static void fail() {
        if (_tuple != null) {
            _collector.fail(_tuple);
        }
    }

    private static void fail(String msg) {
        StormUtil.logError(msg, _redisClient);
        if (ContentControl.getMD5(_deliveredStoryKey, _redisClient) != null) {
            ContentControl.resetMD5(_deliveredStoryKey, _redisClient);
        }
        if (ContentControl.getMD5(_contentKey, _redisClient) != null) {
            ContentControl.resetMD5(_contentKey, _redisClient);
        }
        if (ContentControl.getMD5(_feedKey, _redisClient) != null) {
            ContentControl.resetMD5(_feedKey, _redisClient);
        }

        _redisClient.hdel(_contentKey, _subscriber);

        if (_tuple != null) {
            _collector.fail(_tuple);
        }
    }

    public static void linear(Map conf) {
        try {
            configure(conf);
            run();
        } catch (Exception e) {
            String errMsg = "Reset" + " Key: " + _feedKey;
            StormUtil.logError(errMsg, e, _redisClient);
            ContentControl.resetMD5(_feedKey, _redisClient);
            _redisClient.hdel(_contentKey, _subscriber);
            fail();
        }
    }

    @SuppressWarnings("rawtypes")
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _stormConf = conf;
        _xsltRootPath = (String) conf.get("xsltRootPath");
        _collector = collector;
    }

    private void configure(Tuple tuple) throws Exception {
        _tuple = tuple;
        _redisClient = new RedisClient(_stormConf);
        //Get feed and article objects
        _contentKey = _tuple.getStringByField("contentKey");
        _subscriber = _tuple.getStringByField("subscriber");
        _subscriberMap = (Map<String, String>) StormUtil.deserialize(_tuple.getBinaryByField("subscriberMap"));
        _sStory = _tuple.getStringByField("story");
        _sStorySubjects = _tuple.getStringByField("sStorySubjects");
        _feedKey = _tuple.getStringByField("feedKey");
        _publisher = new Publisher(_tuple.getBinaryByField("publisher"));
        _keyArgs = new String[]{"delivered", _subscriberMap.get("type"), _subscriberMap.get("name"), _publisher.getSource(), _contentKey};
        _deliveredStoryKey = ContentControl.setKey(_keyArgs);
    }

    public void execute(Tuple tuple) {
        try {
            if (2 == 12) System.out.println("Entered WordPressBolt");
            configure(tuple);
            run();
        } catch (Exception e) {
            String errMsg = "Reset" + " Key: " + _feedKey;
            StormUtil.logError(tuple, errMsg, e, _redisClient);
            ContentControl.resetMD5(_feedKey, _redisClient);
            _redisClient.hdel(_contentKey, _subscriber);
            fail();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //	declarer.declare(new Fields("item"));
    }
}
