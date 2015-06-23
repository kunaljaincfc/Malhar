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

/**
*
* Functional tests for {@link com.datatorrent.lib.ml.KMeansOutputWriterOperator}.
* <p>
*
*/
public class KMeansOutputWriterOperatorTest
{
	/**
	 * Test operator logic emits correct results.
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testNodeProcessing() throws IllegalArgumentException, IOException
	{
		KMeansConfig kmc =new KMeansConfig(3, 3, 3, "C:\\Users\\kunal\\Desktop\\oddlarge.txt", "C:\\Users\\kunal\\Desktop\\", "instance", true,10);
		
		KMeansOutputWriterOperator oper = new KMeansOutputWriterOperator(kmc);

		oper.setup(null);
		oper.beginWindow(0); 

		double[][] list1 = new double[][]{
				  { 0, 0, 0},
				  { 1, 1, 1},
				  { 5, 5, 5},
				  };
		oper.inCentroidListWriter.process(list1);
		
		
		oper.endWindow();
		oper.beginWindow(0); 

		double[][] list2 = new double[][]{
				  { 0, 0, 1},
				  { 1, 1, 1},
				  { 5, 5, 5},
				  };
		oper.inCentroidListWriter.process(list2);
		oper.endWindow();
		
		System.out.println("done");
		}
}
