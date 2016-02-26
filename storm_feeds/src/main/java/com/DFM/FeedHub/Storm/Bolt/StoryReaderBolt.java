package com.DFM.FeedHub.Storm.Bolt;

//import java.io.PrintWriter;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.DFM.Client.RedisClient;
import com.DFM.Model.Helper.Publisher;
import com.DFM.Model.Story.Story;
import com.DFM.Model.Story.StorySubjects;
import com.DFM.Util.*;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class StoryReaderBolt extends BaseRichBolt {
    private static final long serialVersionUID = 1L;

    private static Publisher _publisher = new Publisher();
    private static RedisClient _redisClient = new RedisClient();
    private static String _xsltRootPath = "";
    private static String _sStory = "";
    private static String _feedKey = "";
    private static String _contentKey = "";

    private static Fields _fields = new Fields("storyGUID", "contentKey", "story", "sStorySubjects", "feedKey", "publisher");
    private static Values _values;
    private static Tuple _tuple;
    private static Map<String, Object> _stormConf;
    private static Map<String, Object> _linearConf;
    private static boolean _isLinear = false;
    private static OutputCollector _collector;

    private static void configure(Map conf) throws Exception {
        _linearConf = conf;
        _isLinear = true;
        //Get feed and article objects
        Map<String, String> mainConf = (Map) _linearConf.get("mainConf");
        _redisClient = new RedisClient(mainConf);
        _xsltRootPath = mainConf.get("xsltRootPath");
        _sStory = (String) _linearConf.get("story");
        _feedKey = (String) _linearConf.get("feedKey");
        _publisher = new Publisher((byte[]) _linearConf.get("publisher"));
        _sStory = _sStory.replace("<" + _publisher.getItemElement() + ">", _publisher.getItemOutRoot());
    }

    private static void run() throws Exception {
        //if (1==11) System.out.println("SRB->getPhaseType: " + publisher.getPhaseType() + ", SRB->getFeedType: " + publisher.getFeedType());

        //Transform story to get attributes
        if (1 == 11) System.out.println("EntryString:" + _sStory);
        String xsltPath = _xsltRootPath + _publisher.getFeedType() + "_Story.xslt";
        if (1 == 11) System.out.println("xslt: " + xsltPath);
        String storyXML = XmlUtil.transform(_sStory, xsltPath);
        if (1 == 11) System.out.println("storyXML: " + storyXML);
        Story story = Story.fromXML(storyXML);
        StorySubjects storySubjects = story.getStorySubjects();
        String sStorySubjects = storySubjects.toXml();
        if (1 == 11) System.out.println("sStorySubjects:" + sStorySubjects);

        String[] keyArgs = {"content", _publisher.getSourceType(), story.getGuid()};
        String contentKey = ContentControl.setKey(keyArgs);
        _contentKey = contentKey;

        //Check uniqueness
        String suStory = StringUtil.removeUnicode(_sStory);
        byte[] b5 = StormUtil.serialize(_sStory);
        String s5 = new String(b5, StandardCharsets.UTF_8);
        String m5 = ContentControl.createMD5(s5);
        Map<String, Object> statusMap = new HashMap<String, Object>();
        statusMap = ContentControl.createStoryStatusMap(contentKey, m5, _redisClient);

        if ((Boolean) statusMap.get("isNew") | (Boolean) statusMap.get("isUpdated")) {
            Map<String, String> hashMap = new HashMap<String, String>();
            hashMap.put("md5", m5);
            hashMap.put("title", story.getTitle());
            hashMap.put("viewuri", story.getViewURI());
            hashMap.put(StringUtil.hyphenateString(_publisher.getCat()), _publisher.getUrl());
            ContentControl.trackContent(contentKey, hashMap, _redisClient);
            if (2 == 12)
                System.out.println("StoryReaderBolt->Story new/changed: " + story.getTitle() + "; oldSourceMD5: " + statusMap.get("oldMD5") + "; newSourceMD5: " + statusMap.get("newMD5"));
            emit(story.getGuid(), contentKey, suStory, sStorySubjects);
        }
    }

    private static void emit(String storyGUID, String contentKey, String sStory, String sStorySubjects) {
        if (_isLinear) {
            _values = new Values(storyGUID, contentKey, sStory, sStorySubjects, _linearConf.get("feedKey"), _linearConf.get("publisher"));
            Map<String, Object> outConf = LinearControl.buildConf(_fields, _values);
            outConf.put("mainConf", _linearConf.get("mainConf"));
            SubscriberBolt.linear(outConf);
        } else if (_tuple != null) {
            _values = new Values(storyGUID, contentKey, sStory, sStorySubjects, _tuple.getStringByField("feedKey"), _tuple.getBinaryByField("publisher"));
            _collector.emit(_tuple, _values);

        } else {
            String msg = "StoryReaderBolt: both conf and tuple are null";
            StormUtil.logError(msg, _redisClient);
        }
    }

    public static void linear(Map conf) {
        try {
            configure(conf);
            run();
        } catch (Exception e) {
            StormUtil.logError(e, _redisClient);
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
        _isLinear = false;
        _redisClient = new RedisClient(_stormConf);
        //Get feed and article objects
        _sStory = _tuple.getStringByField("story");
        _feedKey = _tuple.getStringByField("feedKey");
        _publisher = new Publisher(_tuple.getBinaryByField("publisher"));
        _sStory = _sStory.replace("<" + _publisher.getItemElement() + ">", _publisher.getItemOutRoot());
    }

    public void execute(Tuple tuple) {
        try {
            configure(tuple);
            run();
            _collector.ack(tuple);
        } catch (Exception e) {
            String msg = "Reset" + " Key: " + _feedKey;
            StormUtil.logError(tuple, msg, e, _redisClient);
            ContentControl.resetMD5(_contentKey, _redisClient);
            ContentControl.resetMD5(_feedKey, _redisClient);
            _collector.fail(tuple);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(_fields);
    }
}
