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
 * Emits the updated centroid list for every iteration.
 * <p>
 * This can not be partitioned. Partitioning
 * this will yield incorrect result.<br>
 * <b>Ports</b>:<br>
 * <b>insumcount</b>: expects an instance of KMeansCounter class<br>
 * <b>inputPortIterationCount</b>: expects Integer<br>
 * <b>centroidUpdatedOutput</b>: emits 2D double array<br>
 * <br>
 * <br>
 * <b>Properties</b>: None<br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * @displayName KMeansCentroidUpdater
 * @category ML
 * @tags kmeans, ml
 * 
 */

public class KMeansCentroidUpdater extends BaseOperator
{
	
	protected transient double[][] updatedCentroidList;
	private int k;
	private int centroidDimension;
	protected transient KMeansCounter aggregatedkmeanscounter;
	KMeansConfig kmc = null;
	private int iterationCount=0;
	
	public KMeansCentroidUpdater() {

	}

	public KMeansCentroidUpdater(KMeansConfig kmc) {
		this.kmc = kmc;
		k=kmc.getNumClusters();		
		centroidDimension=kmc.getDataDimension();
	}
	
	/**
	 * Input port that takes sum of vectors per cluster center key and sum all the received inputs
	 */
	public final transient DefaultInputPort<KMeansCounter> insumcount = new DefaultInputPort<KMeansCounter>() {

		@Override
		public void process(KMeansCounter inputsumcount) {
			// TODO Auto-generated method stub
			for (int i = 0; i < k; i++){
				aggregatedkmeanscounter.instanceCount[i]+=inputsumcount.instanceCount[i];
				for (int j = 0; j < centroidDimension; j++){
						aggregatedkmeanscounter.centerSum[i][j]+= inputsumcount.centerSum[i][j];
				}
			}
		}
	};

	public final transient DefaultInputPort<Integer> inputPortIterationCount = new DefaultInputPort<Integer>() {

		@Override
		public void process(Integer inputiterationcount) {
			// TODO Auto-generated method stub
			iterationCount++;
			LOG.info("Signal from Line Reader Received"+iterationCount);
			centroidUpdatedOutput.emit(updatedCentroidList);
			}
	};

	/**
	 * Output port that emits updated centroid list per iteration.
	 */
	public final transient DefaultOutputPort<double[][]> centroidUpdatedOutput = new DefaultOutputPort<double[][]>();

	@Override
	public void setup(OperatorContext context) {
		// TODO Auto-generated method stub
		super.setup(context);
		aggregatedkmeanscounter = new KMeansCounter(k, centroidDimension);
		updatedCentroidList=new double[k][centroidDimension];
	}
	
	/**
	 * Calculate the updated centroid for the values received so far
	 */
	@Override
	public void endWindow(){
		for(int a = 0 ; a < k ; a++){
			if(aggregatedkmeanscounter.instanceCount[a]!=0){
				for(int b = 0; b < centroidDimension; b++){
					updatedCentroidList[a][b]=aggregatedkmeanscounter.centerSum[a][b]/(double)aggregatedkmeanscounter.instanceCount[a];
				}
			}
			else{
				for(int b = 0; b < centroidDimension; b++){
					updatedCentroidList[a][b]=0;
				}
			}
		}
		
	}

	public void beginWindow(long windowId){
		
	}
	
	private static final Logger LOG = LoggerFactory.getLogger(KMeansCentroidUpdater.class);

}
	