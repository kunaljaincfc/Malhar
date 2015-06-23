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
* Functional tests for {@link com.datatorrent.lib.ml.KMeansCentroidUpdater}.
* <p>
*
*/
public class KMeansCentroidUpdaterTest
{
	/**
	 * Test operator logic emits correct results.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testNodeProcessing()
	{
		KMeansCentroidUpdater oper = new KMeansCentroidUpdater();

		CollectorTestSink KMeansCentroidUpdaterSink = new CollectorTestSink();
		oper.centroidUpdatedOutput.setSink(KMeansCentroidUpdaterSink);
		oper.setup(null);
		oper.beginWindow(0); 

		KMeansCounter input1 = new KMeansCounter(3,3);
		int[] array11=new int[]{1,2,3};
		double[][] array12= new double[][]{
				{1,2,3},{4,5,6},{7,8,9}};
		input1.instanceCount=array11;
		input1.centerSum=array12;
		
		oper.insumcount.process(input1);
		
		KMeansCounter input2 = new KMeansCounter(3,3);
		int[] array21=new int[]{4,5,6};
		double[][] array22= new double[][]{
				{10,11,12},{13,14,15},{16,17,18}};
		input2.instanceCount=array21;
		input2.centerSum=array22;
		
		oper.insumcount.process(input2);
		
		oper.endWindow();
		

		oper.beginWindow(0); 

		KMeansCounter input3 = new KMeansCounter(3,3);
		int[] array31=new int[]{7,8,9};
		double[][] array32= new double[][]{
				{19,20,21},{22,23,24},{25,26,27}};
		input3.instanceCount=array31;
		input3.centerSum=array32;
		
		oper.insumcount.process(input3);
		
		KMeansCounter input4 = new KMeansCounter(3,3);
		int[] array41=new int[]{10,11,12};
		double[][] array42= new double[][]{
				{28,29,30},{31,32,33},{34,35,36}};
		input4.instanceCount=array41;
		input4.centerSum=array42;
		
		oper.insumcount.process(input4);
		
		oper.endWindow();
		
		double[][] check1=new double[][]{
				{(double)58/22,(double)62/22,(double)66/22},{(double)70/26,(double)74/26,(double)78/26},{(double)82/30,(double)86/30,(double)90/30}};
							

		Assert.assertEquals("number emitted tuples", 2,
				KMeansCentroidUpdaterSink.collectedTuples.size());
		double[][] k=  (double[][]) KMeansCentroidUpdaterSink.collectedTuples.get(1);
		for (int i=0;i<3;i++){
			System.out.println(Arrays.toString(k[i]));
		}
		//Assert.assertEquals(check1[0][0], k[0][0],0.0001);
		Assert.assertEquals(Arrays.toString(check1[1]), Arrays.toString(k[1]));
		//Assert.assertEquals(Arrays.toString(check1[2]), Arrays.toString(k[2]));
		
		}
}
