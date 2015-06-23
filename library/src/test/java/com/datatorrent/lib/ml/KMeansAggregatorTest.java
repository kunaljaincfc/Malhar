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
 * Functional tests for {@link com.datatorrent.lib.ml.KMeansAggregator}.
 * <p>
 *
 */
public class KMeansAggregatorTest
{
	/**
	 * Test operator logic emits correct results.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testNodeProcessing()
	{
				
		KMeansAggregator oper = new KMeansAggregator();

		CollectorTestSink KMeansAggregatorSink = new CollectorTestSink();
		oper.centroidCountOutput.setSink(KMeansAggregatorSink);
		oper.setup(null);
		oper.beginWindow(0); 

		double [] array1=new double []{2,2,2};
		double [] array2=new double []{3,4,5};
		double [] array3=new double []{1,1,1};
		
		double [] sum1= new double[] {5,6,7};
		double [] sum2= new double[] {1,1,1};
		double [] sum3= new double[] {0,0,0};
		
		
		KMeansData input1=new KMeansData();
		input1.clusterCenterkey=0;
		input1.pointVector=array1;
		
		KMeansData input2=new KMeansData();
		input2.clusterCenterkey=0;
		input2.pointVector=array2;
		
		KMeansData input3=new KMeansData();
		input3.clusterCenterkey=1;
		input3.pointVector=array3;
		
		oper.inpointcenter.process(input1);
		oper.inpointcenter.process(input2);
		oper.inpointcenter.process(input3);
		
		
		oper.endWindow();
		
		Assert.assertEquals("number emitted tuples", 1,KMeansAggregatorSink.collectedTuples.size());
		KMeansCounter k= (KMeansCounter) KMeansAggregatorSink.collectedTuples.get(0);
		for (int i=0;i<3;i++){
			System.out.println(Arrays.toString(k.centerSum[i]));
		}
		System.out.println(Arrays.toString(k.instanceCount));
		Assert.assertEquals(2, k.instanceCount[0]);
		Assert.assertEquals(1, k.instanceCount[1]);
		Assert.assertEquals(Arrays.toString(sum1), Arrays.toString(k.centerSum[0]));
		Assert.assertEquals(Arrays.toString(sum2), Arrays.toString(k.centerSum[1]));
		Assert.assertEquals(Arrays.toString(sum3), Arrays.toString(k.centerSum[2]));
		
		
	}
	
	@Test
	public void testNodeProcessingPerformance()
	{
		KMeansConfig kmc =new KMeansConfig(3, 3, 3, "C:\\Users\\kunal\\Desktop\\three.txt", "C:\\Users\\kunal\\Desktop\\", "instance", true,10);
		
		KMeansAggregator oper = new KMeansAggregator(kmc);

		CollectorTestSink KMeansAggregatorSink1 = new CollectorTestSink();
		oper.centroidCountOutput.setSink(KMeansAggregatorSink1);
		oper.setup(null);
		oper.beginWindow(0); 

		double [] array1=new double []{2,2,2};
		
		KMeansData input1=new KMeansData();
		input1.clusterCenterkey=0;
		input1.pointVector=array1;
		
		long timeBegin = System.currentTimeMillis();
		for (int i=0;i<10000000;i++){
			oper.inpointcenter.process(input1);
		}
		long timeEnd=System.currentTimeMillis();
		System.out.println(timeEnd-timeBegin);
		
		oper.endWindow();
		
		Assert.assertEquals("number emitted tuples", 1,KMeansAggregatorSink1.collectedTuples.size());
		KMeansCounter k= (KMeansCounter) KMeansAggregatorSink1.collectedTuples.get(0);
		for (int a=0;a<3;a++){
			System.out.println(Arrays.toString(k.centerSum[a]));
		}
		System.out.println(Arrays.toString(k.instanceCount));
	}
}
