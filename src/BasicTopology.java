import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import bolts_and_spouts.*;


public class BasicTopology {

	static int numberOfPlotters = 5;
	
	//data synthesis
	static int topicsNum = 30;
	static int genericTagsNum = 50;
	static int perTopicTagsNum = 100;
	static double useGenericTagProb = 0.05;
	static int maxTagSetSize = 4;

	static int tickFreq = 30; 
	public static void main(String[] args) {
		
		String topologyName = "BasicTopology";
	    
		Config conf = new Config();
	    conf.setDebug(true);
	    //conf.setMaxTaskParallelism(1); //number of threads spawned 
	    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFreq);

		// The Topology Builder
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout("generator", new SyntheticTwitterSpout(topicsNum, genericTagsNum, perTopicTagsNum, useGenericTagProb, maxTagSetSize),1);
		
		builder.setBolt("disseminator",new DisseminatorBolt(),1)
			.allGrouping("plotter", "index_update")
			.allGrouping("generator", "tagset");
		
		builder.setBolt("plotter", new PlotterBolt(), numberOfPlotters)
			.directGrouping("disseminator", "tagset")
			.directGrouping("disseminator", "disseminator_new_tagset")
			.directGrouping("plotter", "plotter_new_tagset")
			.directGrouping("disseminator", "overloaded")
			.directGrouping("plotter", "relocation")
			.directGrouping("plotter", "change_owner");
		
		//local run
		System.out.println("LOCAL");
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(topologyName, conf, builder.createTopology());

	}
	
	
}
