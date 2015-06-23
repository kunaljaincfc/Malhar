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

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.lib.testbench.CollectorTestSink;

/**
*
* Functional tests for {@link com.datatorrent.lib.ml.KMeansLineReader}.
* <p>
*
*/
public class KMeansLineReaderTest
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
		KMeansConfig kmc =new KMeansConfig(3, 3, 3, "C:\\Users\\kunal\\Desktop\\three.txt", "C:\\Users\\kunal\\Desktop\\", "instance", true,10);
		
		KMeansLineReader oper = new KMeansLineReader(kmc);
		CollectorTestSink KMeansLineReaderSink = new CollectorTestSink();
		oper.centroidListOutputPort.setSink(KMeansLineReaderSink);
		CollectorTestSink KMeansLineReaderSink2 = new CollectorTestSink();
		oper.lineOutputPort.setSink(KMeansLineReaderSink2);
		CollectorTestSink KMeansLineReaderSink3 = new CollectorTestSink();
		oper.iterationCountOutputPort.setSink(KMeansLineReaderSink3);
		
		
		oper.setup(null);
		
		
		oper.beginWindow(0); 
		oper.emitTuples();
		oper.endWindow();

		Assert.assertEquals("number emitted tuples", 1,
				KMeansLineReaderSink.collectedTuples.size());
		//System.out.print(KMeansLineReaderSink.collectedTuples.size());
		Assert.assertEquals("number emitted tuples", 0,
				KMeansLineReaderSink3.collectedTuples.size());
		//System.out.print(KMeansLineReaderSink.collectedTuples.get(0));
		
		oper.beginWindow(0); 
		oper.emitTuples();
		oper.emitTuples();
		oper.emitTuples();
		oper.emitTuples();
		oper.emitTuples();
		oper.emitTuples();
		oper.endWindow();
			
		oper.beginWindow(0); 
		oper.emitTuples();
		oper.endWindow();
		Assert.assertEquals("number emitted tuples", 1,
				KMeansLineReaderSink3.collectedTuples.size());
		System.out.println(KMeansLineReaderSink3.collectedTuples.get(0));
		
/*		oper.beginWindow(0); 
		oper.emitTuples();
		oper.endWindow();
		
		
		oper.beginWindow(0); 
		oper.emitTuples();
		oper.endWindow();
		Assert.assertEquals("number emitted tuples", 1,
				KMeansLineReaderSink3.collectedTuples.size());
		System.out.println(KMeansLineReaderSink3.collectedTuples.get(0));
		
			
/*		
 * Assert.assertEquals("number emitted tuples", 1,
				KMeansLineReaderSink3.collectedTuples.size());
		System.out.print(KMeansLineReaderSink3.collectedTuples.get(0));
	*/	
		
		
		//System.out.println(KMeansLineReaderSink2.collectedTuples.get(3));
		Assert.assertEquals("number emitted tuples", 4,
				KMeansLineReaderSink2.collectedTuples.size());
		//System.out.println(KMeansLineReaderSink2.collectedTuples.get(3));
		
		
		String[] k=  (String[]) KMeansLineReaderSink.collectedTuples.get(0);

		for (int i=0;i<3;i++){
			System.out.println(k[i]);
		}
		System.out.println("done");
		
		}
}
