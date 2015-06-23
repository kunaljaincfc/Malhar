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

import java.io.IOException;

import org.junit.Test;

import com.datatorrent.lib.testbench.CollectorTestSink;

/**
*
* Functional tests for {@link com.datatorrent.lib.ml.KMeansInputReader}.
* <p>
*
*/
public class KMeansInputReaderTest
{
	/**
	 * Test operator logic emits correct results.
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	/*@Test
	public void testNodeProcessing() throws IllegalArgumentException, IOException
	{
		KMeansConfig kmc =new KMeansConfig(3, 3, 3, "C:\\Users\\kunal\\Desktop\\three.txt", "C:\\Users\\kunal\\Desktop\\", "instance", true,10);
		
		KMeansInputReader oper = new KMeansInputReader(kmc);
		
		CollectorTestSink KMeansInputReaderSink = new CollectorTestSink();
		oper.pointVectorOut.setSink(KMeansInputReaderSink);
		CollectorTestSink KMeansInputReaderSink2 = new CollectorTestSink();
		oper.CentroidListOut.setSink(KMeansInputReaderSink2);
		
		String a="1,2,3";
		String b="11,21,41";
		
		String[]c ={"0.0,0,0",
				"1.0,1.0,1.0",
				"2,2,2"};		
		oper.setup(null);
		
		
		oper.beginWindow(0); 
		oper.input.process(a);
		oper.input.process(b);
		oper.inputCentroidList.process(c);
		oper.endWindow();

		Assert.assertEquals("number emitted tuples", 2,
			KMeansInputReaderSink.collectedTuples.size());
		//
		double[] k=  (double[]) KMeansInputReaderSink.collectedTuples.get(0);
		double[] k1=  (double[]) KMeansInputReaderSink.collectedTuples.get(1);
		double [][] l=(double[][])KMeansInputReaderSink2.collectedTuples.get(0);
		System.out.println(Arrays.toString(k));
		System.out.println(Arrays.toString(k1));
		for (int i=0;i<3;i++){
			System.out.println(Arrays.toString(l[i]));
		}
		System.out.println("done");
		
		}*/
	@Test
	public void testNodeProcessingPerformance()
	{
		KMeansConfig kmc =new KMeansConfig(3, 3, 3, "C:\\Users\\kunal\\Desktop\\three.txt", "C:\\Users\\kunal\\Desktop\\", "instance", true,10);
		
		KMeansInputReader oper = new KMeansInputReader(kmc);

		CollectorTestSink KMeansInputReaderSink = new CollectorTestSink();
		oper.pointVectorOut.setSink(KMeansInputReaderSink);
		oper.setup(null);
		oper.beginWindow(0); 
		String b="11,21,41";
		
		
		long timeBegin = System.currentTimeMillis();
		for (int i=0;i<100000;i++){
			oper.input.process(b);
		}
		long timeEnd=System.currentTimeMillis();
		System.out.println(timeEnd-timeBegin);
		
		oper.endWindow();
		
		}
}
