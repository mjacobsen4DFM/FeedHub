package com.DFM.FeedHub.Storm.Bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.DFM.Client.RedisClient;
import com.DFM.Client.WebClient;
import com.DFM.Model.Helper.Publisher;
import com.DFM.Model.Story.Story;
import com.DFM.Model.Story.StoryLinks;
import com.DFM.Util.ContentControl;
import com.DFM.Util.LinearControl;
import com.DFM.Util.StormUtil;
import com.DFM.Util.XmlUtil;

import java.util.HashMap;
import java.util.Map;

public class MultiPhaseLoadBolt extends BaseRichBolt {
    private static final long serialVersionUID = 1L;

    private static RedisClient _redisClient = new RedisClient();
    private static Publisher _publisher = new Publisher();
    private static String _xsltRootPath = "";
    private static String _feedKey = "";
    private static String _sStory = "";

    private static Fields _fields = new Fields("storyGUID", "story", "feedKey", "publisher");
    private static Values _values;
    private static Tuple _tuple;
    private static Map<String, Object> _stormConf;
    private static Map<String, Object> _linearConf;
    private static boolean _isLinear = false;
    private static OutputCollector _collector;

    private static void configure(Map conf) throws Exception {
        _linearConf = conf;
        _isLinear = true;
        Map<String, String> mainConf = (Map) _linearConf.get("mainConf");
        _redisClient = new RedisClient(mainConf);
        _xsltRootPath = mainConf.get("xsltRootPath");
        //Get feed and article objects
        _publisher = new Publisher((byte[]) _linearConf.get("publisher"));
        _sStory = (String) _linearConf.get("story");
        _feedKey = (String) _linearConf.get("feedKey");
    }

    private static void run() throws Exception {
        if (2 == 12) System.out.println("MultiPhaseLoadBolt->phaseTypeBOLT: MultiPhase");
        //Get feed and article objects
        Map<String, Object> statusMap = new HashMap<String, Object>();
        String xsltPath = _xsltRootPath + _publisher.getFeedType() + "_MultiPhase.xslt";
        String storyXML = XmlUtil.transform(_sStory, xsltPath);
        Story story = Story.fromXML(storyXML);
        StoryLinks storyLinks = story.getStoryLinks();
        String[] links = storyLinks.getLinks();
        String storyGuid = story.getGuid();
        String[] keyArgs = {"content", _publisher.getSourceType(), storyGuid};
        String contentKey = ContentControl.setKey(keyArgs);
        Map<String, String> contentMap = new HashMap<String, String>();
        WebClient client = new WebClient(_publisher);
        String phaseStory = "";
        String updateTest = "";
        String sourceURL = "";

        updateTest = story.getUpdateTest();
        if (updateTest != null && !updateTest.equals("")) {
            statusMap = ContentControl.createStoryStatusMap(contentKey, "updateTest", updateTest, _redisClient);
            if ((Boolean) statusMap.get("isNew") | (Boolean) statusMap.get("isUpdated")) {
                for (String link : links) {
                    sourceURL = link.trim();
                    contentMap.put("sourceURL", sourceURL);
                    client.setUrl(sourceURL);
                    phaseStory += client.get();
                }

                contentMap.put("updateTest", story.getUpdateTest());
                contentMap.put("title", story.getTitle());
                ContentControl.trackContent(contentKey, contentMap, _redisClient);

                emit(storyGuid, phaseStory);
            }
        }
    }

    private static void emit(String storyGuid, String phaseStory) {
        if (_isLinear) {
            _values = new Values(storyGuid, phaseStory, _linearConf.get("feedKey"), _linearConf.get("publisher"));
            Map<String, Object> outConf = LinearControl.buildConf(_fields, _values);
            outConf.put("mainConf", _linearConf.get("mainConf"));
            StoryReaderBolt.linear(outConf);
        } else if (_tuple != null) {
            _values = new Values(storyGuid, phaseStory, _tuple.getStringByField("feedKey"), _tuple.getBinaryByField("publisher"));
            _collector.emit(_tuple, _values);
        } else {
            String msg = "MultiPhaseLoadBolt: both conf and tuple are null";
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
        _publisher = new Publisher(_tuple.getBinaryByField("publisher"));
        _sStory = _tuple.getStringByField("story");
        _feedKey = _tuple.getStringByField("feedKey");
    }

    public void execute(Tuple tuple) {
        try {
            configure(tuple);
            run();
            _collector.ack(tuple);
        } catch (Exception e) {
            String msg = "Reset" + " Key: " + _feedKey;
            StormUtil.logError(tuple, msg, e, _redisClient);
            ContentControl.resetMD5(_feedKey, _redisClient);
            _collector.fail(tuple);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(_fields);
    }
}
