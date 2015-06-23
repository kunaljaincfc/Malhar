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
 * Emits the sum of vectors and instance count for every cluster center key
 * <p>
 * This is an end window operator. This can be partitioned. <br>
 * <b>Ports</b>:<br>
 * <b>inpointcenter</b>: expects instance of KMeansData class<br>
 * <b>centroidCountOutput</b>: emits instance of KMeansCounter class<br>
 * <br>
 * <br>
 * <b>Properties</b>: None<br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * @displayName KMeansAggragator
 * @category ML
 * @tags kmeans, ml
 * 
 */

public class KMeansAggregator extends BaseOperator
{
	
	protected transient KMeansCounter kMeansCounter;
	private int k;
	private int centroidDimension;
	KMeansConfig kmc = null;
	boolean isEmit;
	
	public KMeansAggregator() {

	}

	public KMeansAggregator(KMeansConfig kmc) {
		this.kmc = kmc;
		k=kmc.getNumClusters();
		centroidDimension=kmc.getDataDimension();
		isEmit=false;
	}
	
	/**
	 * Input port that takes the point vector along with the key of its nearest centroid.
	 */
	public final transient DefaultInputPort<KMeansData> inpointcenter = new DefaultInputPort<KMeansData>() {

		@Override
		public void process(KMeansData inputcluster) {
			// TODO Auto-generated method stub
			isEmit=true;
			kMeansCounter.process(inputcluster,k);			
		}
	};

	/**
	 * Output port that emits sum of vectors per cluster center key every window.
	 */
	public final transient DefaultOutputPort<KMeansCounter> centroidCountOutput = new DefaultOutputPort<KMeansCounter>();

	@Override
	public void setup(OperatorContext context) {
		// TODO Auto-generated method stub
		super.setup(context);
		kMeansCounter = new KMeansCounter(k, centroidDimension);
	}
	
	/**
	 * Emit sum of vectors per cluster center key.
	 */
	@Override
	public void endWindow(){
		if(isEmit){
			centroidCountOutput.emit(kMeansCounter);
			isEmit=false;
		}
	}

	public void beginWindow(long windowId){
		kMeansCounter.clear();
	}
	
	private static final Logger LOG = LoggerFactory.getLogger(KMeansAggregator.class);

}
	