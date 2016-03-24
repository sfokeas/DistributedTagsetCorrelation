**README FILE**

The following work has been completed in Athens, 2015. It was completed as part of the bachelor program in the department of Informatics and Telecommunications of the University of Athens.

Author: Sotirios P. Fokeas  
Date: July, 2015

Build Instructions:
1) Follow instructions in https://storm.apache.org/documentation/Setting-up-development-environment.html

2) Define parameters in topology file

Tested With:
- Storm version: Apache Storm 0.9.3
- Java VM version: Java 1.7

Parameters:
- Storm paramaters:
	- ticks frequency (tickFreq)
	- number of plotters (numberOfPlotters)
- Synthetic Data paramaters:
	- Topics number (topicsNum)
	- Generic tags number (genericTagsNum)
	- Tags per topic number (perTopicTagsNum)
	- Generic tag usage probability (useGenericTagProb)
	- Maximum tag-set size (maxTagSetSize)


Folders & Files:
- bolts_and_spouts
	- DisseminatorBolt.java
	- SyntheticTwitterSpout.java
	- PlotterBolt.java
- misc_classes
	- GraphInconsistencyException.java
- BasicTopology.java


