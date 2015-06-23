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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;

/**
 * This is the Naive Bayes Output operator for overwriting output file on Local File System or HDFS..
 * Custom created, since AbstractFileOutputOperator does not have the capability to overwrite files.<br>
 * <br>
 *<b>Ports</b>:<br>
 *<b>inCentroidListWriter</b>: expects a 2D double array<br>
 */
public class KMeansOutputWriterOperator extends BaseOperator {

  @SuppressWarnings("unused")

  String tuple = "";
  KMeansConfig kmc = null;
  transient FileSystem fs = null;

  private int iterationCount;
  private int k;
  private int centroidDimension;
  
  public KMeansOutputWriterOperator(){
  }

  public KMeansOutputWriterOperator(KMeansConfig kmc){
    this.kmc = kmc;
    k=kmc.getNumClusters();
    centroidDimension=kmc.getDataDimension();
    iterationCount=0;
  }
  
  /**
   * Input port receives the updated centroid list for every iteration.
   * This updated centroid list is written to Local File System or HDFS.  
   */

  public final transient DefaultInputPort<double[][]> inCentroidListWriter = new DefaultInputPort<double[][]>() {
    @Override
    public void process(double[][] inputCentroidList) {
    	iterationCount++;
    	StringBuilder sb= new StringBuilder();
    	//Create updated centroid list for writing to file in a format which the input reader can read
    		for(int i=0;i<k;i++){
    			for(int j=0;j<centroidDimension;j++){
    				if(j==(centroidDimension-1))
    					sb.append(Double.toString(inputCentroidList[i][j]));
    				else
    					sb.append(inputCentroidList[i][j]+",");
    			}
    			sb.append("\n");
    		}
    	writeData( fs ,kmc.getCentroidListDir(), kmc.getCentroidListName()+"."+Integer.toString(iterationCount), false, sb.toString());
    	LOG.info("Writing for"+iterationCount+": "+sb.toString());
    }
  };

  @Override
  public void setup(OperatorContext context){
    try {
    	if(kmc.getisLocalfs()){
    		FileSystem tempFS = FileSystem.newInstance(new Configuration());
    		tempFS = ((LocalFileSystem) tempFS).getRaw();
    		fs = tempFS;
    	}
    	else{
    	      fs = getFSInstance();    		
    	}
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  

  @Override
  public void endWindow(){
    
  }
  // Writes the centroid list to a file which has the iteration number appended 
  public void writeData(FileSystem fs, String filePath, String fileName, boolean overwrite, String tuple){

    FSDataOutputStream out = null;
    try {
      Path writePath = new Path(filePath + Path.SEPARATOR + fileName);
      if(overwrite){
        out = fs.create(writePath, overwrite);
      }
      else{
        if(fs.exists(writePath)){
          out = fs.append(writePath);
        }
        else{
          out = fs.create(writePath, overwrite);
        }
      }

      out.writeBytes(tuple);
      out.hflush();
      out.close();

    } catch (IOException e) {
      e.printStackTrace();
    }  
  }

  public FileSystem getFSInstance() throws IOException
  {
    Configuration conf = new Configuration();
    return FileSystem.get(conf);
  }


  @Override
  public void teardown() {
    super.teardown();
    try {
      fs.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  private static final Logger LOG = LoggerFactory.getLogger(KMeansOutputWriterOperator.class);

}


