package com.DFM.FeedHub.Storm.Bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.DFM.Client.RedisClient;
import com.DFM.Model.Helper.Publisher;
import com.DFM.Util.ContentControl;
import com.DFM.Util.LinearControl;
import com.DFM.Util.StormUtil;
import com.DFM.Util.XmlUtil;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.util.HashMap;
import java.util.Map;


public class FeedReaderBolt extends BaseRichBolt {
    private static final long serialVersionUID = 1L;

    private static Publisher _publisher = new Publisher();
    private static RedisClient _redisClient = new RedisClient();
    private static String _sStories = "";
    private static String _feedKey = "";
    private static String _phaseType = "";

    private static Fields _fields = new Fields("story", "feedKey", "publisher");
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
        _sStories = (String) _linearConf.get("stories");
        _publisher = new Publisher((byte[]) _linearConf.get("publisher"));
        _phaseType = _publisher.getPhaseType();
        _feedKey = (String) _linearConf.get("feedKey");
    }

    private static void run() throws Exception {
        String md5 = ContentControl.createMD5(_sStories);
        Map<String, Object> statusMap = new HashMap<String, Object>();
        statusMap = ContentControl.createStoryStatusMap(_feedKey, md5, _redisClient);
        boolean bNew = (Boolean) statusMap.get("isNew");
        boolean bUpdated = (Boolean) statusMap.get("isUpdated");

        if (bNew | bUpdated) {
            if (1 == 11) System.out.println("UPDATED FeedType: " + _publisher.getFeedType() + " feedKey: " + _feedKey);
            Document doc = XmlUtil.deserialize(_sStories);
            NodeList stories = doc.getElementsByTagName(_publisher.getItemElement());
            for (int i = 0; i < stories.getLength(); i++) {
                Element story = (Element) stories.item(i);
                String sStory = XmlUtil.XMLtoString(story);
                //sStory= BasicUtil.removeUnicode(sStory);
                if (2 == 12) System.out.println("FeedReaderBolt->phaseType: " + _phaseType);
                emit(sStory);
            }
            Map<String, String> hashMap = new HashMap<String, String>();
            hashMap.put("md5", md5);
            ContentControl.trackContent(_feedKey, hashMap, _redisClient);
        }
    }

    private static void emit(String sStory) {
        if (_isLinear) {
            _values = new Values(sStory, _linearConf.get("feedKey"), _linearConf.get("publisher"));
            Map<String, Object> outConf = LinearControl.buildConf(_fields, _values);
            outConf.put("mainConf", _linearConf.get("mainConf"));
            if (_phaseType.equalsIgnoreCase("SinglePhase")) {
                SinglePhaseLoadBolt.linear(outConf);
            }
            if (_phaseType.equalsIgnoreCase("MultiPhase")) {
                MultiPhaseLoadBolt.linear(outConf);
            }
        } else if (_tuple != null) {
            _values = new Values(sStory, _tuple.getStringByField("feedKey"), _tuple.getBinaryByField("publisher"));
            _collector.emit(_phaseType, _tuple, _values);
        } else {
            String msg = "FeedReaderBolt: both conf and tuple are null";
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
        _collector = collector;
    }

    private void configure(Tuple tuple) throws Exception {
        _tuple = tuple;
        _isLinear = false;
        _redisClient = new RedisClient(_stormConf);
        //Get feed and article objects
        _sStories = tuple.getStringByField("stories");
        _publisher = new Publisher(_tuple.getBinaryByField("publisher"));
        _phaseType = _publisher.getPhaseType();
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
        declarer.declareStream("SinglePhase", _fields);
        declarer.declareStream("MultiPhase", _fields);
    }
}
