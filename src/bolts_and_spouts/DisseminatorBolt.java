package bolts_and_spouts;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import misc_classes.GraphInconsistencyException;

import clojure.lang.MapEntry;

import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class DisseminatorBolt extends BaseRichBolt {

	private static final long serialVersionUID = 4698063418423806013L;


	private OutputCollector _collector;
	
	private Map<String, Set<Integer>> tagToPlottersIndex = new HashMap<String, Set<Integer>>(); //tag -> plotters
	private Set<Set<String>> activeTagsets = new HashSet<>(); //active tagsets //TODO check: I can make it a map and add a timestamp here.

	private Map<Integer, Integer> observedLoad = new HashMap<Integer, Integer>(); // plotter -> load
	//private Map<Integer, Integer> observedLoad_previous = new HashMap<Integer, Integer>();
	
	int totalLoad;
	int numOfplotters;
	double loadCap;
	
	//private int taskId;
	int totalTagsetsReceived; //a variable to count the number of tagsets i.e. number of documents. You can compare this to the totalLoad to determine the global overhead

	//TODO use a prefix for the variables holding data for the experiments. e.g. exp_*
	
	//Experiments
	
	List<Integer> plottersIdsList;


	private int ticksReceived =0;
	
	public DisseminatorBolt (){
		totalLoad=0;
		loadCap = 0.2;
		totalTagsetsReceived = 0;
		ticksReceived = 0;
	}
	
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		//taskId = context.getThisTaskId();
		plottersIdsList = new LinkedList<>(context.getComponentTasks("plotter")); //TODO check. Is it possible that storm changes the list internally? should I copy the list? 
		for (int plotterId : plottersIdsList){
			observedLoad.put(plotterId, 0);
		}
		numOfplotters = plottersIdsList.size();
	}
	
	
	public void execute(Tuple input) {
		if (input.getSourceComponent().equals("generator") && input.getSourceStreamId().equals("tagset")) {
			totalTagsetsReceived++;
			Set<String> tagset = new HashSet<String>((HashSet<String>) input.getValueByField("tagset"));  //foteinh does it differently //Set<String> tags = new HashSet<String>((Collection<? extends String>) input.getValueByField("tags"));
			if (activeTagsets.contains(tagset)){
				List<Integer> alreadySentTo = new ArrayList(); 
				for (String tag : tagset){
					for (Integer pl : tagToPlottersIndex.get(tag)){
						if (!alreadySentTo.contains(pl)){
							observedLoad.put(pl, observedLoad.get(pl) + 1);
							totalLoad +=1;
							_collector.emitDirect(pl, "tagset", new Values(tagset));
							alreadySentTo.add(pl);
						}
					}
					//timestampIndex.put(tag, System.currentTimeMillis());
				}
			}
			else{ //if this is a new tag-set
				//assign it to the plotter which has the most connections i.e. most common tags 
				//note: new tag-sets will always be heads (supersets)
				Map<Integer,Integer> temp_counter = new HashMap<>(); //plotter -> number of common tags
				Set<Integer>receiverPlotters = new HashSet<Integer>(); //this set is used to send the new tagset to all plotters that have a common tag
				//find the plotter with the most connections
				for(String tag : tagset){
					if (!tagToPlottersIndex.containsKey(tag)) continue;
					for(Integer pl : tagToPlottersIndex.get(tag)){
						if(temp_counter.containsKey(pl)){
							temp_counter.put(pl, temp_counter.get(pl) +1);
						}
						else{
							temp_counter.put(pl, 1);
						}
					}
					receiverPlotters.addAll(tagToPlottersIndex.get(tag));
				}
				int max = -1; 
				int plotterWithMax = 0;
				for (Integer pl : temp_counter.keySet()){
					if (temp_counter.get(pl) > max){
						max = temp_counter.get(pl);
						plotterWithMax = pl;
					}
				}
				if (plotterWithMax == 0){
					//this means that all tags are new tags and no plotter has any common tag
					//assign it to the one with the less load
					Entry<Integer,Integer> minLoad = new MapEntry(0, 9999999); //TODO initialise
					for (Entry<Integer,Integer> entry : observedLoad.entrySet()){
						if (minLoad.getValue() > entry.getValue()){
							minLoad = entry;
						}
					}
					plotterWithMax=minLoad.getKey();
					max = 0;
					receiverPlotters.add(minLoad.getKey());
				}
				//send tuples
				for (int pl : receiverPlotters){
					observedLoad.put(pl, observedLoad.get(pl) + 1);
					totalLoad +=1;
					_collector.emitDirect(pl,"disseminator_new_tagset",new Values(tagset,plotterWithMax)); //plotter with max i.e. plotter in charge
				}
				//update tags to plotters Index
				for(String tag : tagset){
					if (!tagToPlottersIndex.containsKey(tag)){
						tagToPlottersIndex.put(new String(tag), new HashSet<Integer>());
					}
					tagToPlottersIndex.get(tag).add(new Integer(plotterWithMax));
				}
				//add this tagset and its subsets to the tagsets list in order not to misinterpret subsets of this tag-set as new tag-sets
				LinkedList<Set<String> > subsetStack = new LinkedList<>();
				activeTagsets.add(tagset);
				subsetStack.push(tagset);
				while (!subsetStack.isEmpty()){
					Set<String> tagsetPopped = subsetStack.pop();
					if (tagsetPopped.size()<=1) continue;
					for(String tag : tagsetPopped){
						Set<String> subset = new HashSet<String>(tagsetPopped);
						subset.remove(tag);
						if (!activeTagsets.contains(subset)){
							activeTagsets.add(subset);
							subsetStack.push(subset);
						}
					}
				}
			}
		}
		else if (input.getSourceComponent().equals("plotter") && input.getSourceStreamId().equals("index_update")) {
			String operation = input.getStringByField("operation");
			int plotter = input.getSourceTask();
			String tag = input.getStringByField("tag");
			if (operation.equals("add")){
				tagToPlottersIndex.get(tag).add(plotter);
			}
			if (operation.equals("remove")){
				tagToPlottersIndex.get(tag).remove(plotter);
			}
		}
		
//TODO in erase update tagToPlottersIndex and active tagsets list
		
//		else if (){ //clock from erase window
//			//this is for tag-based erase (not tagset-based)
//			for (Map.Entry<String, Long> entry : timestampIndex.entrySet()){
//				if (System.currentTimeMillis() - entry.getValue() < eraseCap){
//					String tag = entry.getKey();
//					for (Integer pl : tagsIndex.get(tag)){
//						_collector.emitDirect(pl,"erase",new Values(tag));
//					}
//				}
//			}
//		}
		else if (input.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && input.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)){ 
			ticksReceived++;
			if ((ticksReceived  +1) % 2 == 0 ){
				double averageLoad = totalLoad / numOfplotters; 
				double loadThreshold = averageLoad + loadCap * averageLoad;
				for (Integer pl : observedLoad.keySet()){
					if(observedLoad.get(pl) > loadThreshold){
						_collector.emitDirect(pl, "overloaded", new Values(observedLoad,loadThreshold));
						break; //if more than one plotter is trying to relocate vertices things are going to get messy
					}
				}
				totalLoad =0;
				totalTagsetsReceived = 0;
				//observedLoad_previous = observedLoad;
				observedLoad= new HashMap<Integer, Integer>();
				
				for (int plotterId : plottersIdsList){
					observedLoad.put(plotterId, 0);
				}	
			}
		}
		
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//These are the streams that the disseminator outputs
		declarer.declareStream("tagset", new Fields("tagset"));
		//declarer.declareStream("erase", new Fields("tag"));
		declarer.declareStream("disseminator_new_tagset", new Fields("tagset","plotter_in_charge")); 
		declarer.declareStream("overloaded", new Fields("loadsMap","LoadThreshold")); 
	}
	
}
