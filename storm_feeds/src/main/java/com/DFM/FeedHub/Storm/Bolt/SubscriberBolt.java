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

import java.util.ArrayList;
import java.util.Map;

public class SubscriberBolt extends BaseRichBolt {
    private static final long serialVersionUID = 1L;

    private static Publisher _publisher = new Publisher();
    private static RedisClient _redisClient = new RedisClient();
    private static String _storyGUID = "";
    private static String _subscriberKey = "";
    private static ArrayList<String> _subscribers = null;
    private static String[] _keyArgs = {};

    private static Fields _fields = new Fields("storyGUID", "subscriber", "subscriberMap", "contentKey", "story", "sStorySubjects", "feedKey", "publisher");
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
        _storyGUID = (String) _linearConf.get("storyGUID");
        _publisher = new Publisher((byte[]) _linearConf.get("publisher"));
        _keyArgs = new String[]{"subscribers", "*", _publisher.getFeedKey().replace("publishers:", "")};
        _subscriberKey = ContentControl.setKey(_keyArgs);
        _subscribers = _redisClient.keys(_subscriberKey);
    }

    private static void run() throws Exception {
        for (String subscriber : _subscribers) {
            Map<String, String> subscriberMap = _redisClient.hgetAll(subscriber);
            byte[] binarySubscriberMap = StormUtil.serialize(subscriberMap);
            String subscriberType = subscriberMap.get("type");
            if (2 == 12)
                System.out.println("SubscriberBolt->subscriberType: " + subscriberType + ", _storyGUID: " + _storyGUID);
            emit(subscriber, binarySubscriberMap, subscriberType);
        }
    }

    private static void emit(String subscriber, byte[] binarySubscriberMap, String subscriberType) {
        if (_isLinear) {
            _values = new Values(_storyGUID, subscriber, binarySubscriberMap, _linearConf.get("contentKey"), _linearConf.get("story"), _linearConf.get("sStorySubjects"), _linearConf.get("feedKey"), _linearConf.get("publisher"));
            Map<String, Object> outConf = LinearControl.buildConf(_fields, _values);
            outConf.put("mainConf", _linearConf.get("mainConf"));
            if (subscriberType.equalsIgnoreCase("WordPress")) {
                WordPressBolt.linear(outConf);
            }
        } else if (_tuple != null) {
            _values = new Values(_storyGUID, subscriber, binarySubscriberMap, _tuple.getStringByField("contentKey"), _tuple.getStringByField("story"), _tuple.getStringByField("sStorySubjects"), _tuple.getStringByField("feedKey"), _tuple.getBinaryByField("publisher"));
            _collector.emit(subscriberType, _tuple, _values);

        } else {
            String msg = "???: both conf and tuple are null";
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
        _storyGUID = tuple.getStringByField("storyGUID");
        _publisher = new Publisher(tuple.getBinaryByField("publisher"));
        _keyArgs = new String[]{"subscribers", "*", _publisher.getFeedKey().replace("publishers:", "")};
        _subscriberKey = ContentControl.setKey(_keyArgs);
        _subscribers = _redisClient.keys(_subscriberKey);
    }

    public void execute(Tuple tuple) {
        try {
            configure(tuple);
            run();
            _collector.ack(tuple);
        } catch (Exception e) {
            String msg = "Reset" + " feedKey: " + tuple.getStringByField("feedKey") + " storyKey: " + tuple.getStringByField("storyKey");
            ContentControl.resetMD5(tuple.getStringByField("storyKey"), _redisClient);
            ContentControl.resetMD5(tuple.getStringByField("feedKey"), _redisClient);
            StormUtil.logError(tuple, msg, e, _redisClient);
            _collector.fail(tuple);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("WordPress", _fields);
        //declarer.declareStream("Other", _fields);
    }
}
