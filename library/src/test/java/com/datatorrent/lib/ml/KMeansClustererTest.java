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

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.lib.testbench.CollectorTestSink;

/**
*
* Functional tests for {@link com.datatorrent.lib.ml.KMeansClusterer}.
* <p>
*
*/
public class KMeansClustererTest
{
	/**
	 * Test operator logic emits correct results.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	/*@Test
	public void testNodeProcessing()
	{
		KMeansClusterer oper = new KMeansClusterer();

		CollectorTestSink KMeansClustererSink = new CollectorTestSink();
		oper.clusterCenterOutput.setSink(KMeansClustererSink);
		oper.setup(null);
		oper.beginWindow(0); 

		double[][] list1 = new double[][]{
				  { 5.0, 3.0, 1.1},
				  { 4.5, 3.5, 1.2},
				  { 7.0, 2.0, 6.2},
				  };
		oper.incentroidList.process(list1);
		
		double [][] input1=new double[][] {{5.1,3.5,1.4},
				{4.9,3,1.4},
				{4.7,3.2,1.3},
				{4.6,3.1,1.5},
				{5,3.6,1.4},
				{5.4,3.9,1.7},
				{4.6,3.4,1.4},
				{5,3.4,1.5},
				{4.4,2.9,1.4},
				{4.9,3.1,1.5},
				{5.4,3.7,1.5},
				{4.8,3.4,1.6},
				{4.8,3,1.4},
				{4.3,3,1.1},
				{5.8,4,1.2},
				{5.7,4.4,1.5},
				{5.4,3.9,1.3},
				{5.1,3.5,1.4},
				{5.7,3.8,1.7},
				{5.1,3.8,1.5},
				{5.4,3.4,1.7},
				{5.1,3.7,1.5},
				{4.6,3.6,1},
				{5.1,3.3,1.7},
				{4.8,3.4,1.9},
				{5,3,1.6},
				{5,3.4,1.6},
				{5.2,3.5,1.5},
				{5.2,3.4,1.4},
				{4.7,3.2,1.6},
				{4.8,3.1,1.6},
				{5.4,3.4,1.5},
				{5.2,4.1,1.5},
				{5.5,4.2,1.4},
				{4.9,3.1,1.5},
				{5,3.2,1.2},
				{5.5,3.5,1.3},
				{4.9,3.1,1.5},
				{4.4,3,1.3},
				{5.1,3.4,1.5},
				{5,3.5,1.3},
				{4.5,2.3,1.3},
				{4.4,3.2,1.3},
				{5,3.5,1.6},
				{5.1,3.8,1.9},
				{4.8,3,1.4},
				{5.1,3.8,1.6},
				{4.6,3.2,1.4},
				{5.3,3.7,1.5},
				{5,3.3,1.4},
				{7,3.2,4.7},
				{6.4,3.2,4.5},
				{6.9,3.1,4.9},
				{5.5,2.3,4},
				{6.5,2.8,4.6},
				{5.7,2.8,4.5},
				{6.3,3.3,4.7},
				{4.9,2.4,3.3},
				{6.6,2.9,4.6},
				{5.2,2.7,3.9},
				{5,2,3.5},
				{5.9,3,4.2},
				{6,2.2,4},
				{6.1,2.9,4.7},
				{5.6,2.9,3.6},
				{6.7,3.1,4.4},
				{5.6,3,4.5},
				{5.8,2.7,4.1},
				{6.2,2.2,4.5},
				{5.6,2.5,3.9},
				{5.9,3.2,4.8},
				{6.1,2.8,4},
				{6.3,2.5,4.9},
				{6.1,2.8,4.7},
				{6.4,2.9,4.3},
				{6.6,3,4.4},
				{6.8,2.8,4.8},
				{6.7,3,5},
				{6,2.9,4.5},
				{5.7,2.6,3.5},
				{5.5,2.4,3.8},
				{5.5,2.4,3.7},
				{5.8,2.7,3.9},
				{6,2.7,5.1},
				{5.4,3,4.5},
				{6,3.4,4.5},
				{6.7,3.1,4.7},
				{6.3,2.3,4.4},
				{5.6,3,4.1},
				{5.5,2.5,4},
				{5.5,2.6,4.4},
				{6.1,3,4.6},
				{5.8,2.6,4},
				{5,2.3,3.3},
				{5.6,2.7,4.2},
				{5.7,3,4.2},
				{5.7,2.9,4.2},
				{6.2,2.9,4.3},
				{5.1,2.5,3},
				{5.7,2.8,4.1},
				{6.3,3.3,6},
				{5.8,2.7,5.1},
				{7.1,3,5.9},
				{6.3,2.9,5.6},
				{6.5,3,5.8},
				{7.6,3,6.6},
				{4.9,2.5,4.5},
				{7.3,2.9,6.3},
				{6.7,2.5,5.8},
				{7.2,3.6,6.1},
				{6.5,3.2,5.1},
				{6.4,2.7,5.3},
				{6.8,3,5.5},
				{5.7,2.5,5},
				{5.8,2.8,5.1},
				{6.4,3.2,5.3},
				{6.5,3,5.5},
				{7.7,3.8,6.7},
				{7.7,2.6,6.9},
				{6,2.2,5},
				{6.9,3.2,5.7},
				{5.6,2.8,4.9},
				{7.7,2.8,6.7},
				{6.3,2.7,4.9},
				{6.7,3.3,5.7},
				{7.2,3.2,6},
				{6.2,2.8,4.8},
				{6.1,3,4.9},
				{6.4,2.8,5.6},
				{7.2,3,5.8},
				{7.4,2.8,6.1},
				{7.9,3.8,6.4},
				{6.4,2.8,5.6},
				{6.3,2.8,5.1},
				{6.1,2.6,5.6},
				{7.7,3,6.1},
				{6.3,3.4,5.6},
				{6.4,3.1,5.5},
				{6,3,4.8},
				{6.9,3.1,5.4},
				{6.7,3.1,5.6},
				{6.9,3.1,5.1},
				{5.8,2.7,5.1},
				{6.8,3.2,5.9},
				{6.7,3.3,5.7},
				{6.7,3,5.2},
				{6.3,2.5,5},
				{6.5,3,5.2},
				{6.2,3.4,5.4},
				{5.9,3,5.1}};
		for(int j=0;j<150;j++){
			oper.inpoints.process(input1[j]);
				KMeansData k= (KMeansData) KMeansClustererSink.collectedTuples.get(j);
				System.out.println(k.clusterCenterkey+","+Arrays.toString(k.pointVector));
				
			
		}
		

		oper.endWindow();

		//Assert.assertEquals("number emitted tuples", 1,
		//		KMeansClustererSink.collectedTuples.size());
		
		//Assert.assertEquals(1, k.clusterCenterkey);
		//Assert.assertEquals(input1,k.pointVector);
	}*/
	@Test
	public void testNodeProcessingPerformance()
	{
		KMeansConfig kmc =new KMeansConfig(3, 3, 3, "C:\\Users\\kunal\\Desktop\\three.txt", "C:\\Users\\kunal\\Desktop\\", "instance", true,10);
		
		KMeansClusterer oper = new KMeansClusterer(kmc);

		CollectorTestSink KMeansClustererSink1 = new CollectorTestSink();
		oper.clusterCenterOutput.setSink(KMeansClustererSink1);
		oper.setup(null);
		oper.beginWindow(0); 

		double[][] list1 = new double[][]{
				  { 1.0, 1.0, 1.0},
				  { 14.5, 13.5, 11.2},
				  { 17.0, 12.0, 16.2},
				  };
		oper.inCentroidList.process(list1);
		
		double [] array1=new double []{2,2,2};
		
		long timeBegin = System.currentTimeMillis();
		for (int i=0;i<10000;i++){
			oper.inPoints.process(array1);
		}
		long timeEnd=System.currentTimeMillis();
		System.out.println(timeEnd-timeBegin);
		
		oper.endWindow();
		
		Assert.assertEquals("number emitted tuples", 10000,KMeansClustererSink1.collectedTuples.size());
		KMeansData k= (KMeansData) KMeansClustererSink1.collectedTuples.get(1);
		System.out.println(Arrays.toString(k.pointVector));
		System.out.println(k.clusterCenterkey);
	}
}
