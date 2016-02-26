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
import com.DFM.Util.ContentControl;
import com.DFM.Util.LinearControl;
import com.DFM.Util.StormUtil;
import com.DFM.Util.XmlUtil;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.util.Map;

//import com.sun.jndi.toolkit.url.Uri;


public class FeedLoadBolt extends BaseRichBolt {
    private static final long serialVersionUID = 1L;

    private static RedisClient _redisClient = new RedisClient();
    private static Publisher _publisher = new Publisher();
    private static String[] _keyArgs = {};

    private static Fields _fields = new Fields("stories", "feedKey", "publisher");
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
        _publisher = new Publisher((byte[]) _linearConf.get("publisher"));
        _keyArgs = new String[]{"feeds", _publisher.getFeedKey().replace("publishers:", "")};
    }

    private static void run() throws Exception {
        String feedKey = ContentControl.setKey(_keyArgs);
        if (2 == 12) System.out.println("FeedLoadBolt->feedKey: " + feedKey);

        //Read feed
        WebClient client = new WebClient(_publisher);
        DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        Document doc = builder.parse(client.openStream());

        if (1 == 11) System.out.println("XML: " + XmlUtil.XMLtoString(doc));

        NodeList items = doc.getElementsByTagNameNS(_publisher.getItemNamespace(), _publisher.getItemElement());

        if (items.getLength() == 0) {
            items = doc.getElementsByTagName(_publisher.getItemElement());
        }

        if (items.getLength() != 0) {
            if (1 == 11) System.out.println("found items");
            doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
            Element root = doc.createElement("root");
            doc.appendChild(root);
            if (_publisher.getSortOrder().equals("reverse")) {
                for (int i = items.getLength() - 1; i >= 0; i--) {
                    if (1 == 11) System.out.println("item");
                    Node node = items.item(i);
                    Node copyNode = doc.importNode(node, true);
                    root.appendChild(copyNode);
                }
            } else {
                for (int i = 0; i < items.getLength(); i++) {
                    if (1 == 11) System.out.println("item");
                    Node node = items.item(i);
                    Node copyNode = doc.importNode(node, true);
                    root.appendChild(copyNode);
                }
            }
            String sStories = XmlUtil.XMLtoString(doc);
            //sStories= BasicUtil.removeUnicode(sStories);
            emit(sStories, feedKey);
        }
    }

    private static void emit(String sStories, String feedKey) {
        if (_isLinear) {
            _values = new Values(sStories, feedKey, _linearConf.get("publisher"));
            Map<String, Object> outConf = LinearControl.buildConf(_fields, _values);
            outConf.put("mainConf", _linearConf.get("mainConf"));
            FeedReaderBolt.linear(outConf);
        } else if (_tuple != null) {
            _values = new Values(sStories, feedKey, _tuple.getBinaryByField("publisher"));
            _collector.emit(_tuple, _values);

        } else {
            String msg = "FeedLoadBolt: both conf and tuple are null";
            StormUtil.logError(msg, _redisClient);
        }
    }

    public static void linear(Map inConf) {
        try {
            configure(inConf);
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
        _publisher = new Publisher(_tuple.getBinaryByField("publisher"));
        _keyArgs = new String[]{"feeds", _publisher.getFeedKey().replace("publishers:", "")};
    }

    public void execute(Tuple tuple) {
        try {
            configure(tuple);
            run();
            _collector.ack(tuple);
        } catch (Exception e) {
            StormUtil.logError(tuple, e, _redisClient);
            _collector.fail(tuple);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(_fields);
    }
}
