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
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

/**
 * This is the KMeans Input Reader Operator.
 * This will read the ARFF format files and pass on 
 * the point as a vector in the input data in a double array
 * and the centroid list as a 2D double array. 
 * 
 * TODO 
 * This assumes that the input file is a plain csv file 
 * (i.e. it ignores the @header part of the ARFF files and just considers the @data portion which is just plain csv)
 * @author 
 *
 */
public class KMeansInputReader extends BaseOperator{


	double[] pointAsVector;
	double[][] centroidListAsArray;
	KMeansConfig kmc = null;
	private int k;
	private int centroidDimension;
	private long maxEmittedTuples;
	private long counter;
	private long windowID;
	
	public KMeansInputReader(){
	
	}

	public KMeansInputReader(KMeansConfig kmc){
		this.kmc = kmc;
		k=kmc.getNumClusters();
		centroidDimension=kmc.getDataDimension();
		maxEmittedTuples=kmc.getMaxEmittedTuples();
	}

	/**
	 * Output port that emits the point as a vector (double array).
	 */
	public final transient DefaultOutputPort<double[]> pointVectorOut = new DefaultOutputPort<double[]>();
	
	/**
	 * Output port that emits the centroid list as a 2D double array.
	 */
	public final transient DefaultOutputPort<double[][]> CentroidListOut = new DefaultOutputPort<double[][]>();
	
	/**
	 * Input port receives tuples as String.   
	 */
	public final transient DefaultInputPort<String> input = new DefaultInputPort<String>(){

		@Override
		public void process(String tuple) {
			pointAsVector = parseAsCsv(tuple);
			if(pointAsVector!=null){
				pointVectorOut.emit(pointAsVector);
				counter++;
			}
			else{
				return;
			}
		}
	};
	
	/**
	 * Input port receives tuples as String array.    
	 */
	public final transient DefaultInputPort<String[]> inputCentroidList = new DefaultInputPort<String[]>(){

		@Override
		public void process(String[] incentroidList) {
			centroidListAsArray = parseAsCsvList(incentroidList);
			if (centroidListAsArray!=null){
				CentroidListOut.emit(centroidListAsArray);
				LOG.info("Centroidlist parsed "+windowID);
			}
			else {
				return;	
			}
		}
	};

	/**
	 * Parses the String as a csv tuple
	 * @param tuple
	 * @return Array of double - listing the point as a vector
	 */
	public double[] parseAsCsv(String tuple){
		String[] attrs = tuple.trim().split(",");
		double[] pointVal = new double[attrs.length];
		try{
			for(int i=0;i<centroidDimension;i++){
				pointVal[i] = Double.parseDouble(attrs[i]);
			}
		}
		catch(NumberFormatException nfe){
			LOG.info("Tuple datatype error");
			return null;
		}
		catch(ArrayIndexOutOfBoundsException aioobe){
			LOG.info("Tuple dimension error");
			return null;
		}
		return pointVal;
	}
	
	/**
	 * Parses the String as a csv tuple
	 * @param incentroidlist
	 * @return 2D Double Array - representing the list of centroids
	 */
	public double[][] parseAsCsvList(String[] incentroidList){
		double [][] clist = new double[k][centroidDimension];
		try{
			for(int j=0;j<k;j++){
				String[] attrs = incentroidList[j].trim().split(",");
				double[] pointVal = new double[attrs.length];
				for(int i=0;i<centroidDimension;i++){
						pointVal[i] = Double.parseDouble(attrs[i]);
				}
				clist[j]=pointVal;
			}
		}
		catch(NumberFormatException nfecentroid){
			LOG.info("Centroid datatype error");
			return null;
		}
		catch(ArrayIndexOutOfBoundsException aioobecentroid){
			LOG.info("Centroid dimension error");
			return null;
		}
		return clist;
	}
	
	public void beginWindow(long windowId){
		counter=0;
		windowID=windowId;
	}

	@Override
	public void endWindow() {
		LOG.info("Emitted tuples in window = "+counter+" Window ID : "+windowID);
	}
	
	private static final Logger LOG = LoggerFactory.getLogger(KMeansInputReader.class);

}
