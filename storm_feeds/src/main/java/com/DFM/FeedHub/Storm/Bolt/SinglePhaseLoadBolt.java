package com.DFM.FeedHub.Storm.Bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.DFM.Client.RedisClient;
import com.DFM.Model.Story.Story;
import com.DFM.Util.ContentControl;
import com.DFM.Util.LinearControl;
import com.DFM.Util.StormUtil;

import java.util.Map;

public class SinglePhaseLoadBolt extends BaseRichBolt {
    private static final long serialVersionUID = 1L;
    private static RedisClient _redisClient = new RedisClient();
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
        //Get feed and article objects
        _feedKey = (String) _linearConf.get("feedKey");
        _sStory = (String) _linearConf.get("story");
    }

    private static void run() throws Exception {
        if (2 == 12) System.out.println("SinglePhaseLoadBolt->phaseTypeBOLT: SinglePhase");
        //Simple pass-thru for when story content is within the feed
        Story story = Story.fromXML(_sStory);
        String storyGuid = story.getGuid();
        emit(storyGuid);
    }

    private static void emit(String storyGuid) {
        if (_isLinear) {
            _values = new Values(storyGuid, _sStory, _feedKey, _linearConf.get("publisher"));
            Map<String, Object> outConf = LinearControl.buildConf(_fields, _values);
            outConf.put("mainConf", _linearConf.get("mainConf"));
            StoryReaderBolt.linear(outConf);
        } else if (_tuple != null) {
            _values = new Values(storyGuid, _sStory, _feedKey, _tuple.getBinaryByField("publisher"));
            _collector.emit(_tuple, _values);
        } else {
            String msg = "SinglePhaseLoadBolt: both conf and tuple are null";
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
        _feedKey = _tuple.getStringByField("feedKey");
        _sStory = _tuple.getStringByField("story");
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
