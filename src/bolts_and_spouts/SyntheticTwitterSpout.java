package bolts_and_spouts;

import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import backtype.storm.spout.SleepSpoutWaitStrategy;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class SyntheticTwitterSpout extends BaseRichSpout {
	private static final long serialVersionUID = -1493865329234512372L;

	SpoutOutputCollector _collector;

	private int topicsNum = 0;
	private int genericTagsNum = 0;
	private int perTopicTagsNum = 0;
	private double useGenericTagProb = 0.0;
	private int maxTagSetSize = 0;

	private Random rand = new Random(12345678);
	
	private int tuplesProduced = 0;

	public SyntheticTwitterSpout(int topicsNum, int genericTagsNum, int perTopicTagsNum, double useGenericTagProb, int maxTagSetSize) {
		super();
		this.topicsNum = topicsNum;
		this.genericTagsNum = genericTagsNum;
		this.perTopicTagsNum = perTopicTagsNum;
		this.useGenericTagProb = useGenericTagProb;
		this.maxTagSetSize = maxTagSetSize;
	}

//	@SuppressWarnings("rawtypes")
//	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
	}

//	@Override
	public void nextTuple() {
		Set<String> tagset = new HashSet<String>();

		// pick a random number determining the size of the tag-set
		int tagsetSize = rand.nextInt(maxTagSetSize) + 1;
		//pick a random topic
		int topic = rand.nextInt(topicsNum);

		// create the tag-set
		for (int i = 0; i < tagsetSize; i++) {
			boolean getFromTopic = (rand.nextDouble() > useGenericTagProb);
			String tag = null;
			if (getFromTopic) tag = "#" + topic + "_" + rand.nextInt(perTopicTagsNum);
			else tag = "#" + rand.nextInt(genericTagsNum);
			tagset.add(tag);
		}
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		_collector.emit("tagset", new Values(tagset));
		tuplesProduced ++;
	}

//	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("tagset", new Fields("tagset"));
	}

}