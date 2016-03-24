package bolts_and_spouts;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;

import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import misc_classes.GraphInconsistencyException;

public class PlotterBolt extends BaseRichBolt{

	private static final long serialVersionUID = 2584610809613067891L;

	private OutputCollector _collector;
	
	private Map<Set<String>, Vertex > tagsetToVertexIndex = new HashMap<Set<String>, Vertex >(); //tagset -> vertex (vertex = (# docs, edges))
	private Map<String, Set<Set<String> > > tagToTagsetsIndex = new HashMap<>(); //tag -> tagsets
	private Set<Set<String> > supersets = new HashSet<>();
	private int MyId;	
	private int ticksReceived = 0;
	
	//experiments
	private int exp_numberOfMessages = 0;
	
	
	
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		MyId = context.getThisTaskId();
	}
	
	public void execute(Tuple input) {
		if (input.getSourceComponent().equals("generator") && input.getSourceStreamId().equals("tagset")) {
			Set<String> tagset= new HashSet<>((Set<String>) input.getValueByField("tagset"));	
			if(tagsetToVertexIndex.containsKey(tagset)){
				tagsetToVertexIndex.get(tagset).setCounter(tagsetToVertexIndex.get(tagset).getCounter() + 1);
			}
			else {
				throw new GraphInconsistencyException("Disseminator thinks that tagset " + tagset + " exists while Plotter "+MyId+" thinks not");
			}
		}
		else if (input.getSourceComponent().equals("disseminator") && input.getSourceStreamId().equals("disseminator_new_tagset")) {
			Set<String> newTagset= new HashSet<>((Set<String>) input.getValueByField("tagset"));	
			int plInCharge = input.getIntegerByField("plotter_in_charge");
			if (tagsetToVertexIndex.containsKey(newTagset)){
				throw new GraphInconsistencyException("Disseminator thinks that tagset " + newTagset + " does not exist while Plotter "+ MyId +" thinks otherwise");
			}
			if (plInCharge == MyId){ //If it is in charge of the new tag-set
				//Insert to graph. Produces all subsets  
				addSupersetToGrah(newTagset,plInCharge);
				tagsetToVertexIndex.get(newTagset).setCounter(1);
			}
			else{ //if not in charge of the new tag-set
				//send vertices which have common tags with the new tagset to the plotter in charge of it
				Set<Set<String>> supersetsToSend = new HashSet<>();
				Map<Set<String>, Integer> tagsetsToSend = new HashMap<>(); //tagset -> counter
				for(String tag : newTagset){
					if (!tagToTagsetsIndex.containsKey(tag)) continue;
					for ( Set<String> conTagset : tagToTagsetsIndex.get(tag)){ //TODO check if alreadySeen list is needed. You could put one here but little is to be gained
						if (tagsetToVertexIndex.get(conTagset).getPlinCharge()!=MyId) continue; //Only sends vertices that belong to it. Otherwise, a lot of duplicate vertices would be sent
						if (supersets.contains(conTagset)) {
							supersetsToSend.add(conTagset);
						}
						tagsetsToSend.put(conTagset, new Integer(tagsetToVertexIndex.get(conTagset).getCounter()));
					}
				}
				//insert tagset. Same as before except the supersetsToSend list
				//Insert to graph. produces all subsets
				Set<Set<String>> supersetsRemoved = addSupersetToGrah(newTagset, plInCharge);
				tagsetToVertexIndex.get(newTagset).setCounter(1);
				supersetsToSend.removeAll(supersetsRemoved);
				//we send the messages now because some tag-set might have been removed form supersetsToSend
//				if (tagsetsToSend.isEmpty()){
//					throw new GraphInconsistencyException("Disseminator thought that new tagset " + newTagset + " had common tags which plotter "+MyId);
//				}
				_collector.emitDirect(plInCharge,"plotter_new_tagset",new Values(supersetsToSend,tagsetsToSend));
				exp_numberOfMessages ++;
				//since some vertices changed owner, some other may no longer be needed. Perform a clean up
				deleteUnneeded(newTagset);
				updateIndex(newTagset);
			}
		}
		else if (input.getSourceComponent().equals("plotter") && input.getSourceStreamId().equals("plotter_new_tagset")) {
			//message sent from a plotter that is not in charge of a new tag-set to the one in charge.
			//Contains all vertices of sender that are connected to the new tag-set
			Set<Set<String>> supersetsReceived = new HashSet<>((Set<Set<String>>) input.getValueByField("supersets"));
			Map<Set<String>,Integer> tagsetsReceived = new HashMap<>((Map<Set<String>,Integer>) input.getValueByField("tagsets"));
			int plInCharge = input.getSourceTask(); 
			for ( Set<String> head : supersetsReceived){
				if (!supersets.contains(head)){
					addSupersetToGrah(head, plInCharge);
				}
			}
			for (Set<String> tagset : tagsetsReceived.keySet()){
				//TODO check whether the tagset is in the the index. Also check if it should always be!
				if(tagsetToVertexIndex.containsKey(tagset)){
					tagsetToVertexIndex.get(tagset).setCounter(tagsetsReceived.get(tagset));
				}
			}
		}
		else if(input.getSourceComponent().equals("disseminator") && input.getSourceStreamId().equals("overloaded")) {
			Map<Integer, Integer> plottersLoad= new HashMap<Integer,Integer>((Map<Integer,Integer>)input.getValueByField("loadsMap"));
			double LoadThreshold = input.getDoubleByField("LoadThreshold");
			Set<tagsetNodePair> movesThatOverload = new HashSet<>(); //list containing all moves, both refused and accepted, that were previously considered.
			int totalReduce = 0;
			while (plottersLoad.get(MyId) - totalReduce > LoadThreshold && !supersets.isEmpty()){ //while still overloaded remove more vertices. Moves might change the threshold a bit, but it is not taken into account.
				int minOverheadChange = 99999999;
				tagsetNodePair move = null; 
				Map <Set<String>, Integer> supersetsConnections = new HashMap<>(); //Maps superset to the number of connections they have to other supersets of the plotter. Contains only supersets that this plotter is responsible. !supersets.isEmpty() for the rare occasion where the plotter does not have any vertices
				int overhead=0; 
				boolean noMovesFound = true;
				for (Set<String> superset : supersets){
					if (tagsetToVertexIndex.get(superset).getPlinCharge() != MyId) continue;
					Set<Set<String> > commonTagEdges = tagsetToVertexIndex.get(superset).getCommonTagEdges();
					Set<Integer> alreadyConsideredPlotters = new HashSet<>(); //holds plotters' ids that the overhead has already been calculated (for a specific superset)
					for (Set<String> edge : commonTagEdges){
						int plID = tagsetToVertexIndex.get(edge).getPlinCharge();
						if (plID == MyId){
							if (supersets.contains(edge)){
								//this is used later if no possible moves toward connected plotters is found
								if( supersetsConnections.containsKey(superset)) {
									supersetsConnections.put(superset, supersetsConnections.get(superset) + 1);
								}
								else supersetsConnections.put(superset, new Integer(1));
							}
							continue; //we only care about edges to other plotters		
						}
						if (alreadyConsideredPlotters.contains(plID)) continue; //TODO check debug phase
						alreadyConsideredPlotters.add(new Integer(plID));
						tagsetNodePair tnPair = new tagsetNodePair(superset, plID);
						if (movesThatOverload.contains(tnPair)) continue; //we have considered this move already and was rejected because it would overload the destination plotter.
						overhead = overheadChange(tnPair);
						if (minOverheadChange>overhead){
							move=tnPair;
							minOverheadChange = overhead;
							noMovesFound = false;
						}
					}
				}
				if (noMovesFound){
					//find the superset with the minimum connections
					Entry<Set<String>, Integer> minCon = new SimpleEntry<Set<String>, Integer>(null, 9999999);
					for (Entry<Set<String>, Integer> entry : supersetsConnections.entrySet()) {
						if (minCon.getValue() > entry.getValue()) {
					        minCon = entry;
					    }
					}
					//find the plotter with the least load
					Entry<Integer,Integer> minLoad = new SimpleEntry<Integer, Integer>(0, 9999999);
					for (Entry<Integer,Integer> entry : plottersLoad.entrySet()){
						if (minLoad.getValue() > entry.getValue()){
							minLoad = entry;
						}
					}
					move = new tagsetNodePair(minCon.getKey(), minLoad.getKey());
					//add the load to the destination plotter
					int loadToDest = destLoadAddition(move);
					plottersLoad.put(move.getNode(),plottersLoad.get(move.getNode()) + loadToDest); 
				}
				else{
					//check if this will overload the destination plotter. If yes do not perform the move
					int loadToDest = destLoadAddition(move);
					if ( plottersLoad.get(move.getNode()) + loadToDest > LoadThreshold ){
						movesThatOverload.add(move);
						continue;
					}
					plottersLoad.put(move.getNode(),plottersLoad.get(move.getNode()) + loadToDest); 
				}
				movesThatOverload = new HashSet<>(); //empty it. This IS the correct place to empty it
				
				//calculate the load that will be subtracted off of the overloaded plotter (this plotter)
				int loadReduce = loadReduction(move);		
				totalReduce+=loadReduce;
				
				//send ownership change messages to all plotters that are connected to the vertex and collect data for the relocation message
				Map<Set<String> , Integer> supersetsToSend = new HashMap<>(); //supersets -> plotter in charge
				Map<Set<String>, Integer> tagsetsToSend = new HashMap<>(); //tagset -> counter
				Set<Integer> sentOwnerChange = new HashSet<>();
				for (Set<String> conTagset : tagsetToVertexIndex.get(move.getTagset()).getCommonTagEdges()){
					int plOfConnected = tagsetToVertexIndex.get(conTagset).getPlinCharge();
					if (!sentOwnerChange.contains(plOfConnected) && plOfConnected != MyId && plOfConnected != move.getNode()){ 							
						_collector.emitDirect(plOfConnected,"change_owner",new Values(move.getTagset(),move.getNode().intValue()));
						exp_numberOfMessages ++;
						sentOwnerChange.add(plOfConnected);
					}
					//collect data to send to the node the vertex is sent to
					if(plOfConnected == move.getNode()) continue; //the destination plotter already has its own vertices
					if (supersets.contains(conTagset)){
						supersetsToSend.put(conTagset,plOfConnected);
					}
					tagsetsToSend.put(conTagset, new Integer(tagsetToVertexIndex.get(conTagset).getCounter()));
				}
				
				//change owner locally
				changeOwner(move.getTagset(),move.getNode());
				
				//send relocation msg
				tagsetsToSend.put(move.getTagset(),tagsetToVertexIndex.get(move.getTagset()).getCounter());
				//BE CAREFUL the supersets list will not contain the superset it's being relocated. It is indicated as a separate field though.
				_collector.emitDirect(move.getNode(),"relocation",new Values(move.getTagset(),supersetsToSend,tagsetsToSend));
				exp_numberOfMessages ++;
				
				//delete unneeded vertices
				deleteUnneeded(move.getTagset());
				
				//index changes to send to disseminator
				updateIndex(move.getTagset());
			}
		}
		else if (input.getSourceComponent().equals("plotter") && input.getSourceStreamId().equals("relocation")) {
			Set<String> movedSuperset= new HashSet<>((Set<String>) input.getValueByField("movedSuperset"));	
			Map<Set<String>,Integer> supersetsReceived = new HashMap<>((Map<Set<String>,Integer>) input.getValueByField("supersets")); //supersets -> plotters in charge
			Map<Set<String>,Integer> tagsetsReceived = new HashMap<>((Map<Set<String>,Integer>) input.getValueByField("tagsets")); //tagsets -> counters
			
			//add supersets and subsets into the graph
			if (supersets.contains(movedSuperset)){
				changeOwner(movedSuperset, MyId);
			} else {
				addSupersetToGrah(movedSuperset, MyId);
			}
			for (Set<String> s : supersetsReceived.keySet()){
				if (!supersets.contains(s)){
					addSupersetToGrah(s, supersetsReceived.get(s));
				}
			}
			//update the counters of inserted supersets
			for (Set<String> ts : tagsetsReceived.keySet()){
				if ( tagsetsReceived.get(ts) > tagsetToVertexIndex.get(ts).getCounter()){
					tagsetToVertexIndex.get(ts).setCounter(tagsetsReceived.get(ts));
				}
			}
			//index updates
			for (String tag : movedSuperset){
				_collector.emit("index_update",new Values("add",tag));
				exp_numberOfMessages ++;
			}
		}
		else if (input.getSourceComponent().equals("plotter") && input.getSourceStreamId().equals("change_owner")) {
			Set<String> tagset= new HashSet<>((Set<String>) input.getValueByField("tagset"));	
			int plotter = input.getIntegerByField("plotter");
			if(tagsetToVertexIndex.containsKey(tagset)){
				changeOwner(tagset, plotter);
			}
		}
		else if (input.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && input.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)){ //clock tick
			ticksReceived ++;
			if (ticksReceived % 2 ==0 ){
				Queue<Set<String>> JCqueue = new LinkedList<>(); //tag-sets that get in this queue will have their JC calculated
				for (Set<String> s : supersets){
					if (tagsetToVertexIndex.get(s).getPlinCharge() == MyId){
						JCqueue.add(s);
					}
				}
				Set<Set<String>> alreadyComputedJC = new HashSet<Set<String>>();
				while (!JCqueue.isEmpty()){
					Set<String> tagset = JCqueue.poll();
					if(alreadyComputedJC.contains(tagset)) continue;
					alreadyComputedJC.add(tagset);
					if(tagset.size() == 1){
						System.err.println("JC for " + tagset + " is 1.0");
						continue;
					}
					//supersets
					double totalS = 0;
					LinkedList <Set<String>> supStack = new LinkedList<>(); //superset of the tag-set are inserted in this stack
					Set<Set<String>> alreadyCountedSupersets = new HashSet<Set<String>>();
					supStack.push(tagset);
					while(!supStack.isEmpty()){
						Set<String> superset = supStack.pop();
						if (alreadyCountedSupersets.contains(superset)) continue;
						alreadyCountedSupersets.add(superset);
						totalS+=tagsetToVertexIndex.get(superset).getCounter();
						//supersets step
						for ( Set<String> edge : tagsetToVertexIndex.get(superset).getSupersetEdges()){
							supStack.add(edge);
						}
					}
					//connected
					double totalC = 0;
					totalC += tagsetToVertexIndex.get(tagset).getCounter();
					for ( Set<String> conTs : tagsetToVertexIndex.get(tagset).getCommonTagEdges() ){
						totalC+=tagsetToVertexIndex.get(conTs).getCounter();
					}
					if (totalC !=0 ){ //note: if totalC is zero then totalS would be zero. don't divide by zero. 
						double JC = totalS/totalC;
						System.err.println("JC for " + tagset + " is "+JC);
					}
					//step
					for( Set<String> subset : tagsetToVertexIndex.get(tagset).getSubsetEdges()){
						JCqueue.add(subset);
					}
				}
			}
			//I think I can do this with dynamic programming. Save the count of supersets (NO I CANT, I will inevitably count duplicates)
		}
	}
	
	private int loadReduction(tagsetNodePair tnPair) {
		Set<String> head = tnPair.getTagset();
		int destPlotter = tnPair.getNode(); //TODO check this... why the dest plotter is not used??
		int totalLoadReduction = 0;
		changeOwner(tnPair);
		boolean shouldCountHead = true; 		//if the head is not connected to any vertex owned by the plotter count it in load reduction
		for (Set<String> edge : tagsetToVertexIndex.get(head).getCommonTagEdges()){
			if (tagsetToVertexIndex.get(edge).getPlinCharge() == MyId) {
				shouldCountHead = false; //this is here for the case when the moved vertex is not connected to the dest plotter. This makes this possibility not true
				continue; // the plotter will still get its own vertices
			}
			boolean shouldCountIt = true;
			for ( Set<String> conTagsetOfEdge : tagsetToVertexIndex.get(edge).getCommonTagEdges() ){
				if (tagsetToVertexIndex.get(conTagsetOfEdge).getPlinCharge() == MyId){
					//if it has a connection to any vertex that belong to the plotter, then plotter will gets its load even after the relocation
					shouldCountIt = false;
					break;
				}
			}
			if (shouldCountIt){
				totalLoadReduction += tagsetToVertexIndex.get(edge).getCounter();
			}
		}
		if(shouldCountHead){
			totalLoadReduction += tagsetToVertexIndex.get(head).getCounter();
		}
		changeOwner(head, MyId); //change owner back
		return totalLoadReduction;
	}

	private int destLoadAddition(tagsetNodePair tnpair) {
		Set<String> head = tnpair.getTagset();
		int destPlotter = tnpair.getNode();
		int totalLoadAddition = 0;
		boolean shouldCountHead = true;
		for ( Set<String> edge : tagsetToVertexIndex.get(head).getCommonTagEdges()){ //common tag  edges contain the subsets as well
			if (tagsetToVertexIndex.get(edge).getPlinCharge() == destPlotter){
				shouldCountHead = false; //this is here for the case when the moved vertex is not connected to the dest plotter at all. This makes this possibility not true
				continue; // the dest plotter already gets its own vertices
			}
			boolean shouldCountIt = true;
			for ( Set<String> conTagsetOfEdge : tagsetToVertexIndex.get(edge).getCommonTagEdges() ){
				if (tagsetToVertexIndex.get(conTagsetOfEdge).getPlinCharge() == destPlotter){
					//if it has a connection to any vertex that belong to the destination plotter, then destination plotter already gets this load
					shouldCountIt = false;
					break;
				}
			}
			if (shouldCountIt){
				totalLoadAddition += tagsetToVertexIndex.get(edge).getCounter();
			}
		}
		if(shouldCountHead){
			totalLoadAddition += tagsetToVertexIndex.get(head).getCounter();
		}
		return totalLoadAddition;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("index_update", new Fields("operation","tag")); //operation can be either add or remove
		declarer.declareStream("plotter_new_tagset", new Fields("supersets","tagsets"));
		declarer.declareStream("relocation", new Fields("movedSuperset","supersets","tagsets"));
		declarer.declareStream("change_owner", new Fields("tagset","plotter"));
	}
		public class Vertex{  //put this in a different file
			private int plotterInCharge; //the plotter that is responsible for this vertex
			private int counter;			
			private Set<Set<String> > commonTagsEdges;
			private Set<Set<String> > supersetsEdges;
			private Set<Set<String> > subsetsEdges;

			public Vertex(){
				counter= 0;
				plotterInCharge = 0;
				commonTagsEdges = new HashSet<>();
				supersetsEdges = new HashSet<>();
				subsetsEdges = new HashSet<>();
			}
			public Vertex(int c, int pl){
				counter = c;
				plotterInCharge = pl;
				commonTagsEdges = new HashSet<>();
				supersetsEdges = new HashSet<>();
				subsetsEdges = new HashSet<>();
			}

			public void putCommonTagEdge(Set<String> tagset){
				commonTagsEdges.add(tagset);
			}
			
			public Set<Set<String>> getCommonTagEdges() {
				return commonTagsEdges;
			}
			
			public void putSubsetEdge(Set<String> subset) {
				subsetsEdges.add(subset);
			}
			
			public Set<Set<String>> getSubsetEdges() {
				return subsetsEdges;
			}

			public void putSupersetEdge(Set<String> tagSet) {
				supersetsEdges.add(tagSet);
			}
			
			public Set<Set<String>> getSupersetEdges() {
				return supersetsEdges;
			}

			public void setPlInCharge(int plInCharge) {
				plotterInCharge = plInCharge;
			}
			
			public int getPlinCharge() {
				return plotterInCharge;
			}

			public void setCounter(int c) {
				counter=c;
			}

			public int getCounter() {
				return counter;
			}
		}
		
		private class tagsetNodePair{
			//TODO check: this will be used in a map as a key, so it must be implemented properly.
			private Set<String> tagset;
			private Integer node;
			
			public tagsetNodePair(Set<String> s, int n){ //initialize with copies!
				tagset = new HashSet<String>(s);
				node = new Integer(n);
			}
			
			public Set<String> getTagset(){
				return tagset;
			}
			
			public Integer getNode(){
				return node;
			}
			
			
//			public boolean equals(tagsetNodePair p){
//				TODO check if this is needed
//			}
		}
		

		
		
		private int overheadChange(tagsetNodePair tnPair){
			Integer destPlotter = tnPair.getNode();
			Set<String> head = tnPair.getTagset();
			int overheadChange = 0;
			changeOwner(head, 0); //change owner to zero just to distinguish what will be moved 
			Set<Set<String> >  alreadySeen = new HashSet<>(); //alreadyCounted
			//don't use subsets
			if (!isConnectedToNode(head, destPlotter)){
				//count all subsets
				LinkedList<Set<String> > subsetStack = new LinkedList<>();
				subsetStack.push(head);
				while (!subsetStack.isEmpty()){
					Set<String> tagSet = subsetStack.pop();
					if (alreadySeen.contains(tagSet)) continue;
					alreadySeen.add(tagSet);
					overheadChange += tagsetToVertexIndex.get(tagSet).getCounter();
					for ( Set<String> subset : tagsetToVertexIndex.get(tagSet).getSubsetEdges()){
						subsetStack.push(subset);
					}
				}
			}
			if (!isConnectedToNode(head, destPlotter)){
				if(overheadChange > 0){ //that means that the head was connected to dest plotter and the overheadChange was incremented above.
					overheadChange = 0;
				}
				else {
					//same as above except for the sign.
					LinkedList<Set<String> > subsetStack = new LinkedList<>();
					subsetStack.push(head);
					while (!subsetStack.isEmpty()){ //WHY YOU DON'T USE THE SUBSETS EDGES??
						Set<String> tagSet = subsetStack.pop();
						if (alreadySeen.contains(tagSet)) continue;
						alreadySeen.add(tagSet);
						overheadChange -= tagsetToVertexIndex.get(tagSet).getCounter();
						for ( Set<String> subset : tagsetToVertexIndex.get(tagSet).getSubsetEdges()){
							subsetStack.push(subset);
						}
					}
				}
			}
			//we care only for connected SUPERSETS
			//then go through and count subsets as well
			for( Set<String> headEdge : tagsetToVertexIndex.get(head).getCommonTagEdges()){
				if(!supersets.contains(headEdge)) continue; //we care only for supersets
				if (tagsetToVertexIndex.get(headEdge).getPlinCharge() == MyId){
					if (!isConnectedToNode(headEdge, destPlotter)){
						overheadChange += tagsetToVertexIndex.get(headEdge).getCounter();
						//produce all subsets
						LinkedList<Set<String> > subsetStack = new LinkedList<>();
						subsetStack.push(head);
						while (!subsetStack.isEmpty()){ //WHY YOU DON'T USE THE SUBSETS EDGES??
							Set<String> tagSet = subsetStack.pop();
							if (alreadySeen.contains(tagSet)) continue;
							alreadySeen.add(tagSet);
							overheadChange += tagsetToVertexIndex.get(tagSet).getCounter();
							for ( Set<String> subset : tagsetToVertexIndex.get(tagSet).getSubsetEdges()){
								subsetStack.push(subset);
							}
						}
					}
				} else if (tagsetToVertexIndex.get(headEdge).getPlinCharge() == destPlotter){
					if (!isConnectedToNode(headEdge, MyId)){
						overheadChange -= tagsetToVertexIndex.get(headEdge).getCounter();
						//count all subsets as well
						LinkedList<Set<String> > subsetStack = new LinkedList<>();
						subsetStack.push(head);
						while (!subsetStack.isEmpty()){ //WHY YOU DON'T USE THE SUBSETS EDGES??
							Set<String> tagSet = subsetStack.pop();
							if (alreadySeen.contains(tagSet)) continue;
							alreadySeen.add(tagSet);
							overheadChange -= tagsetToVertexIndex.get(tagSet).getCounter();
							for ( Set<String> subset : tagsetToVertexIndex.get(tagSet).getSubsetEdges()){
								subsetStack.push(subset);
							}
						}
					}
				}
			}
			changeOwner(head, MyId); //change owner back to normal
			return overheadChange;
		}
		
		
		private int overheadChange_exact (tagsetNodePair tnPair){ //Slower but more exact function
			//remember this is just a heuristic function. It does not have to be perfect, and it might be computational better if it isn't
			Integer destPlotter = tnPair.getNode();
			Set<String> head = tnPair.getTagset();
			int overheadChange = 0;
			Set<Set<String> >  alreadySeen = new HashSet<>();
			changeOwner(head, 0); //change owner to zero just to distinguish what will be moved 
			//first attend to subsets
			LinkedList<Set<String> > subsetStack = new LinkedList<>();
			subsetStack.push(head);
			while (!subsetStack.isEmpty()){ //WHY YOU DON'T USE THE SUBSETS EDGES??
				Set<String> tagSet = subsetStack.pop();
				if (alreadySeen.contains(tagSet)) continue;
				alreadySeen.add(tagSet);
				boolean conToDest = false;
				boolean conToSource = false;  
				for (Set<String> conEdge : tagsetToVertexIndex.get(tagSet).getCommonTagEdges()){
					if (tagsetToVertexIndex.get(conEdge).getPlinCharge() == destPlotter){
						conToDest = true;
						if(conToSource) break;
					}
					if (tagsetToVertexIndex.get(conEdge).getPlinCharge() == MyId){
						conToSource = true;
						if(conToDest) break;
					}
				}
				if(!conToDest){
					overheadChange += tagsetToVertexIndex.get(tagSet).getCounter();
				}
				if(!conToSource){
					overheadChange -= tagsetToVertexIndex.get(tagSet).getCounter();
				}
				//produce subsets
				if (tagSet.size() > 1){
					for(String tag : tagSet){
						Set<String> subset = new HashSet<String>(tagSet);
						subset.remove(tag);
						subsetStack.push(subset);
					}
				}
			}
			//secondly to all other connected vertices
			for( Set<String> headEdge : tagsetToVertexIndex.get(head).getCommonTagEdges()){
				if (alreadySeen.contains(headEdge)) continue;
				alreadySeen.add(headEdge);
				if (tagsetToVertexIndex.get(headEdge).getPlinCharge() == MyId){
					if (!isConnectedToNode(headEdge, destPlotter)){
						overheadChange += tagsetToVertexIndex.get(headEdge).getCounter();
					}
						
				} else if (tagsetToVertexIndex.get(headEdge).getPlinCharge() == destPlotter){
					if (!isConnectedToNode(headEdge, MyId)){
						overheadChange -= tagsetToVertexIndex.get(headEdge).getCounter();
					}
				}
			}
			changeOwner(head, MyId); //change owner back to normal
			return overheadChange;
		}
		
		private boolean isConnectedToNode(Set<String> tagset, int node){
			//TODO check this will throw null pointer exception if tagset is not in tagsetsToVertexIndex
			for (Set<String> edge : tagsetToVertexIndex.get(tagset).getCommonTagEdges()){
				if ( tagsetToVertexIndex.get(edge).getPlinCharge() == node ) return true;
			}
			return false;
		}
		
		private boolean isConnectedToNode2(Set<String> tagset, int node){
			//if supersets list is generally shorter than connections of a tag-set then this would be faster 
			for(Set<String> s : supersets){
				if (tagsetToVertexIndex.get(s).getPlinCharge() == node ){
					if ( tagsetToVertexIndex.get(tagset).getCommonTagEdges().contains(s)){
						return true;
					}
				}
			}
			return false;
		}		
		
		private Set<Set<String>> addSupersetToGrah( Set<String> initialTagSet, int plInCharge){ //inserts a head superset in the graph, producing all subsets and making all appropriate connections
			LinkedList<Set<String> > subsetStack = new LinkedList<>();
			Set<Set<String>> alreadySeen = new HashSet<>();
			Set<Set<String>> supersetsRemoved = new HashSet<>();
			
			//first step 
			if (!tagsetToVertexIndex.containsKey(initialTagSet)){ 
				tagsetToVertexIndex.put(initialTagSet, new Vertex(0,plInCharge)); //counter is initialized to 1 for the case this is a new tagset.
				for( String tag : initialTagSet){ 
					if (!tagToTagsetsIndex.containsKey(tag)){
						tagToTagsetsIndex.put(new String(tag), new HashSet<Set<String>>());
					}
					tagToTagsetsIndex.get(tag).add(initialTagSet);
				}
				//common tags connections
				for( String tag : initialTagSet){ //for each tag of tag-set
					Set<Set<String> > tagsets_with_tag = tagToTagsetsIndex.get(tag);
					for ( Set<String> ts : tagsets_with_tag){
						if(!ts.equals(initialTagSet)){ //avoid connecting it to itself 
							tagsetToVertexIndex.get(ts).putCommonTagEdge(initialTagSet);
							tagsetToVertexIndex.get(initialTagSet).putCommonTagEdge(ts);
						}
					}
				}
				
			}
			subsetStack.push(initialTagSet);
			tagsetToVertexIndex.get(initialTagSet).setPlInCharge(plInCharge);
			//next steps add the subsets
			while (!subsetStack.isEmpty()){
				Set<String> tagSet = subsetStack.pop();
				if(alreadySeen.contains(tagSet)) continue;
				alreadySeen.add(tagSet);
				if(tagSet.size()<=1) continue;
				for(String tag : tagSet){
					Set<String> subset = new HashSet<String>(tagSet);
					subset.remove(tag);
					if (!tagsetToVertexIndex.containsKey(subset)){ //note: if vertices already exist do not alter the common tags edges. All new vertices will connect to them.
						tagsetToVertexIndex.put(subset, new Vertex(0,plInCharge));
						//common tags connections for subset
						for( String tagOfSubset : subset){ 
							Set<Set<String> > tagsets_with_tag = tagToTagsetsIndex.get(tagOfSubset);
							for ( Set<String> ts : tagsets_with_tag){
								if(!ts.equals(subset)){ //avoid connecting it to itself 
									tagsetToVertexIndex.get(ts).putCommonTagEdge(subset);
									tagsetToVertexIndex.get(subset).putCommonTagEdge(ts);
								}
							}
							tagToTagsetsIndex.get(tagOfSubset).add(subset);
						}
					}
					if (supersets.contains(subset)){
						//if the subset exists in supersets list erase it because a superset of it just arrived
						supersets.remove(subset); 
						supersetsRemoved.add(subset);

					}
					subsetStack.push(subset); //even if subset is already in the tagsets index I have to "produce" all subsets in order to change the owner
					tagsetToVertexIndex.get(subset).setPlInCharge(plInCharge); //if the subset did not exist this is not necessary, but nevermind 
					tagsetToVertexIndex.get(subset).putSupersetEdge(tagSet);
					tagsetToVertexIndex.get(tagSet).putSubsetEdge(subset);
				}
			}
			supersets.add(initialTagSet);
			return supersetsRemoved;
		}
		
		private void changeOwner(Set<String> tagset, Integer plotter){ //changes owner of tagset and all its subset to the plotter specified 
			LinkedList<Set<String> > stack = new LinkedList<>();
			Set<Set<String>> alreadyChanged = new HashSet<>();
			stack.push(tagset);
			while (!stack.isEmpty()){
				Set<String> ts = stack.pop();
				if (alreadyChanged.contains(ts)) continue;
				alreadyChanged.add(ts);
				tagsetToVertexIndex.get(ts).setPlInCharge(plotter);
				for ( Set<String> subset : tagsetToVertexIndex.get(ts).getSubsetEdges()){
					stack.push(subset);
				}
			}
		}
		
		private void changeOwner(tagsetNodePair tnPair){ //TODO change THIS to match the above!! changes owner of tagset and all its subset to the plotter specified 
			Set<String> tagset = tnPair.getTagset();
			Integer plotter = tnPair.getNode();
			LinkedList<Set<String> > stack = new LinkedList<>();
			Set<Set<String>> alreadyChanged = new HashSet<>();
			stack.push(tagset);
			while (!stack.isEmpty()){
				Set<String> ts = stack.pop();
				if (alreadyChanged.contains(ts)) continue;
				alreadyChanged.add(ts);
				tagsetToVertexIndex.get(ts).setPlInCharge(plotter);
				for ( Set<String> subset : tagsetToVertexIndex.get(ts).getSubsetEdges()){
					stack.push(subset);
				}
			}
		}
	
		private void deleteTagset(Set<String> head){ //remove the tagsets and all its subsets
			Queue<Set<String>> queue = new LinkedList<>();
			supersets.remove(head);
			//TODO should remove vertex/vertices from the plotters_index as well
			queue.add(head);
			while (!queue.isEmpty()){
				Set<String> tagsetToDelete = queue.poll();
				if (!tagsetToVertexIndex.containsKey(tagsetToDelete)) continue; //it has already been deleted
				if(tagsetToVertexIndex.get(tagsetToDelete).getSupersetEdges().isEmpty()){ 
					//remove the common tag edges towards the tagset that is going to be deleted
					for ( Set<String> comEdge : tagsetToVertexIndex.get(tagsetToDelete).getCommonTagEdges()){
						tagsetToVertexIndex.get(comEdge).getCommonTagEdges().remove(tagsetToDelete);
					}
					//remove the superset edges towards the tagset that is going to be deleted
					for ( Set<String> subEdge : tagsetToVertexIndex.get(tagsetToDelete).getSubsetEdges()){
						tagsetToVertexIndex.get(subEdge).getSupersetEdges().remove(tagsetToDelete);
					}
					//delete the actual tagset
					tagsetToVertexIndex.remove(tagsetToDelete);
					//add all its immediate subsets
					for(String tag : tagsetToDelete){
						Set<String> subset = new HashSet<String>(tagsetToDelete);
						subset.remove(tag);
						queue.add(subset);
						//delete it from the tag -> tagsets map
						tagToTagsetsIndex.get(tag).remove(tagsetToDelete);
						if ( tagToTagsetsIndex.get(tag).isEmpty()){
							//this is an opportunity to delete tag key entries that have empty tagsets list
							tagToTagsetsIndex.remove(tag);
						}
					}
				}
				//else if the tagset has a superset do not delete it. do nothing..
			}		
		}
		

		private tagsetsTotalCountPair fakeDeleteTagset(Set<String> head){ //returns a list of all the tagsets that would be deleted
			//WRONG!!! fake delete does not take into account of the subsets that we keep around but not longer receive any documents with them (because the are not connected with any vertex of that the plotter owns)
			//I can still use it to calcualte E.O.!!
			Set<Set<String> > wouldBeDeleted = new HashSet<>();
			Queue<Set<String>> queue = new LinkedList<>();
			int totalLoad = 0; 	
			queue.add(head);
			while (!queue.isEmpty()){
				Set<String> tagsetToDelete = queue.poll();
				if (wouldBeDeleted.contains(tagsetToDelete)) continue; //it has already been deleted
				Set<Set<String>> supersetsOfDeleteCandidate = tagsetToVertexIndex.get(tagsetToDelete).getSupersetEdges();
				if (wouldBeDeleted.containsAll(supersetsOfDeleteCandidate) || supersetsOfDeleteCandidate.isEmpty()){ //in a real setting we would check if it was any supersets 
					totalLoad+= tagsetToVertexIndex.get(tagsetToDelete).getCounter();
					wouldBeDeleted.add(tagsetToDelete);
					for(String tag : tagsetToDelete){
						Set<String> subset = new HashSet<String>(tagsetToDelete);
						subset.remove(tag);
						queue.add(subset);
					}
				}
				//else if the tagset has a superset do not delete it. do nothing..
			}
			return new tagsetsTotalCountPair(wouldBeDeleted,totalLoad);
		}
		
		private class tagsetsTotalCountPair{
			private Set<Set<String>> tagsets;
			private int TotalCount;
			
			public tagsetsTotalCountPair(Set<Set<String>> wouldBeDeleted, int totalLoad) {
				// TODO Auto-generated constructor stub
			}	
			
		}
		
		private void deleteUnneeded(Set<String> change){ //deletes vertices that are no longer needed to plotter in order to calculate JC. Change is a head vertex that has changed owner
			boolean shouldDeleteMoved = true;
			List<Set<String>> headsToDelete = new LinkedList<>();
			for (Set<String> conTagset : tagsetToVertexIndex.get(change).getCommonTagEdges()){ 
				if(!supersets.contains(conTagset)) continue;
				if (tagsetToVertexIndex.get(conTagset).getPlinCharge()==MyId){ 
					//this is here in order to check whether the superset moved should be deleted
					shouldDeleteMoved = false;
					continue; //do not attempt to delete vertices that the plotter owns
				}
				//check if verices connected with the one being moved should get deleted
				boolean shouldDelete =true;
				for( Set<String> edgeOfConnected : tagsetToVertexIndex.get(conTagset).getCommonTagEdges()){ //the edges of the edges of the tagset that is being moved
					if (tagsetToVertexIndex.get(edgeOfConnected).getPlinCharge() == MyId){
						shouldDelete=false;
						break;
					}
				}
				if (shouldDelete){ 
					headsToDelete.add(conTagset); //deleteTagset(conTagset); throws java.util.concurrentmodificationexception
				}
			}
			for ( Set<String> head : headsToDelete){
				deleteTagset(head);
			}
			if(shouldDeleteMoved){
				deleteTagset(change); 
			}
		}
		
		private void updateIndex(Set<String> change){ //index changes to send to disseminator. 
			for (String tag : change){
				if (!tagToTagsetsIndex.containsKey(tag)){
					_collector.emit("index_update",new Values("remove",tag)); //Can send redundant messages. e.g. the plotter is not in the list of plotter for this tag in the Disseminator index
					exp_numberOfMessages ++;
					continue;
				}
				boolean shouldSendIndexUpdate = true;
				for (Set<String> ts : tagToTagsetsIndex.get(tag)){ 
					if (tagsetToVertexIndex.get(ts).getPlinCharge() == MyId){
						shouldSendIndexUpdate=false; 
						break;
					}
				}
				if (shouldSendIndexUpdate){
					_collector.emit("index_update",new Values("remove",tag)); //Can send redundant messages. e.g. the plotter is not in the list of plotter for this tag in the Disseminator index
					exp_numberOfMessages ++;
				}
			}
		}
		
		
}
