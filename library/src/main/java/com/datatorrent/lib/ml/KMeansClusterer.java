/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.ml;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

/**
 *
 * Emits the point along with its cluster center at the end of window.
 * <p>
 * This can be partitioned. <br>
 * <b>Ports</b>:<br>
 * <b>inpoints</b>: expects double array<br>
 * <b>incentroidList</b>: expect 2D double array<br>
 * <b>clusterCenterOutput</b>: emits instance of KMeansData class<br>
 * <br>
 * <br>
 * <b>Properties</b>: None<br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * @displayName KMeansClusterer
 * @category ML
 * @tags kmeans, ml
 * 
 */

public class KMeansClusterer extends BaseOperator
{ 
	
	protected transient KMeansData kMeansData;
	protected double[][] centroidList;

	private int k;
	private int centroidDimension;
	KMeansConfig kmc = null;
	private long maxEmittedTuples;
	private long counter;
	
	public KMeansClusterer() {

	}

	public KMeansClusterer(KMeansConfig kmc) {
		this.kmc = kmc;
		k=kmc.getNumClusters();
		centroidDimension=kmc.getDataDimension();
		maxEmittedTuples=kmc.getMaxEmittedTuples();
	}

	/**
	 * Input port that takes the point as a double vector.
	 */
	public final transient DefaultInputPort<double[]> inPoints = new DefaultInputPort<double[]>() {

		@Override
		public void process(double[] inpointVector) {
			// TODO Auto-generated method stub
			kMeansData.clear();
			kMeansData.process(centroidList,inpointVector);
			clusterCenterOutput.emit(kMeansData);
			counter++;
		}
	};
	
	
	/**
	 * Input port that takes the list of centroids as a 2D double array.
	 */
	
	public final transient DefaultInputPort<double[][]> inCentroidList = new DefaultInputPort<double[][]>() {

		@Override
		public void process(double[][] clusterCenterList) {
			// TODO Auto-generated method stub
			centroidList=clusterCenterList;
		}
	};

	/**
	 * Output port that emits point and its nearest cluster center.
	 */
	public final transient DefaultOutputPort<KMeansData> clusterCenterOutput = new DefaultOutputPort<KMeansData>();

	@Override
	public void setup(OperatorContext context) {
		// TODO Auto-generated method stub
		super.setup(context);
		kMeansData = new KMeansData();
	}
	
	public void beginWindow(long windowId){
		counter=0;
	}
	
	@Override
	public void endWindow(){
		LOG.info("Emitted touples in window = "+counter);
	}
	
	private static final Logger LOG = LoggerFactory.getLogger(KMeansClusterer.class);

	
}
	