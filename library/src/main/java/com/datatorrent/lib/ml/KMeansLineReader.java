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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;

/**
 * This is the File line reader operator responsible for reading a file on HDFS and sends a line as a tuple.
 * 
 * @author 
 *
 */
public class KMeansLineReader implements InputOperator{

	
	public final transient DefaultOutputPort<String> lineOutputPort = new DefaultOutputPort<String>();
	public final transient DefaultOutputPort<String[]> centroidListOutputPort = new DefaultOutputPort<String[]>();
	public final transient DefaultOutputPort<Integer> iterationCountOutputPort = new DefaultOutputPort<Integer>();
	
	private transient BufferedReader br = null;
	private transient BufferedReader brData = null;
	private transient BufferedReader brCentroidList = null;
	private boolean check;
	private boolean readData;
	private boolean readCentroidList;
	private transient FileSystem fs = null;
	
	KMeansConfig kmc = null;
	
	private int k;
	private int iterationCount;
	private long maxEmittedTuples;
	private long counter;
	private long windowID;

	public KMeansLineReader() {

	}

	public KMeansLineReader(KMeansConfig kmc) {
		this.kmc = kmc;
		k=kmc.getNumClusters();
		readData=false;
		readCentroidList=true;
		check=false;
		iterationCount=0;
		maxEmittedTuples=kmc.getMaxEmittedTuples();
	}

	

	@Override
	public void setup(OperatorContext arg0) {
		Configuration conf = new Configuration();
    	try{
    		fs = FileSystem.get(conf);
			initializeFilepaths();
		}
		catch(IOException e){
			throw new RuntimeException("Error initilializing",e);
		}
	}
	
	public void initializeFilepaths() throws IOException {
		Path filePathData = new Path(kmc.getInputDataFile());
		Path filePathCentroidList= new Path(kmc.getCentroidListDir()+Path.SEPARATOR+kmc.getCentroidListName()+"."+Integer.toString(iterationCount));
		brCentroidList=getBufferedReader(filePathCentroidList);
		brData=getBufferedReader(filePathData);
	}

	public BufferedReader getBufferedReader(Path filepath) throws IOException  {
		Path filePathInit = filepath;
		if(kmc.getisLocalfs()){
    		FileSystem tempFS = FileSystem.newInstance(new Configuration());
    		tempFS = ((LocalFileSystem) tempFS).getRaw();
    		fs = tempFS;
    	}
		br = new BufferedReader(new InputStreamReader(fs.open(filePathInit)));
		LOG.info("br is reset: {}",br.toString(),Integer.toString(iterationCount));
		return br;
	}

	
	
	
	@Override
	public void beginWindow(long arg0) {
		counter=0;
		windowID=arg0;
		if(check==true){
			if(readCentroidList==true){
				iterationCount++;
				LOG.info(Integer.toString(iterationCount));
				iterationCountOutputPort.emit(iterationCount);
				check=false;
			}
		}
	}
	@Override
	public void emitTuples() {
		if(counter>=maxEmittedTuples){
			return;
		}
		if(iterationCount>=kmc.getMaxIterations()){
			LOG.info("Max Iterations Done");
			return;
		}
		if (readData){
			String tuple = readEntityData();
				if(tuple != null && tuple .trim().length()!=0){			
					lineOutputPort.emit(tuple);
					counter++;
				}
		}
	}
	
	protected String readEntityData() {
		try{
			String s = brData.readLine();
			if(s != null){
				return s;
			}
			else{
				initializeFilepaths();
				readData = false;
				LOG.info("File System Reseted and read data = false",Integer.toString(iterationCount));
				return null;
			}		
		}catch(IOException e){
			e.printStackTrace();
			throw new RuntimeException();
		}
	}
	
	
	public void endWindow() {
		LOG.info("Processed tuples in a window = "+counter+" WindowID: "+windowID);
		if (readCentroidList){
			try {
				Path doneFile = new Path(kmc.getCentroidListDir()+Path.SEPARATOR+kmc.getCentroidListName()+"."+Integer.toString(iterationCount));
					
				if (fs.exists(doneFile)) {
					LOG.debug("Iteration Done",Integer.toString(iterationCount));
					String[] centroidList=new String[k];
					for(int i=0;i<k;i++){
						centroidList[i]=readEntityCentroidList();
						if(centroidList[i]==null){
							throw new RuntimeException("Centroid number mismatch");
						}
					}
					if(centroidList[k-1] != null && centroidList[k-1] .trim().length()!=0){			
						centroidListOutputPort.emit(centroidList);
						readCentroidList=false;
						readData=true;
						check=true;
						initializeFilepaths();
						LOG.info("File System Reseted and read centroid list = false"+Integer.toString(iterationCount)+windowID);
					}
				}
			} catch (IOException e) {
				throw new RuntimeException("Error",e);
			}

				
		}
		else if (readData==false){
			readCentroidList=true;
		}
		
	}
	
	protected String readEntityCentroidList() {
		try{
			String s1 = brCentroidList.readLine();
			return s1;
		}			
		catch(IOException e){
			e.printStackTrace();
			throw new RuntimeException();
		}
	}
	
	@Override
	public void teardown() {
		LOG.info("Tearing down :(");
		try {
			fs.close();
			br.close();
			brData.close();
			brCentroidList.close();
		} catch (IOException e) {
			throw new RuntimeException();
		}
		br=null;
		brData = null;
		brCentroidList=null;
	}
	
	private static final Logger LOG = LoggerFactory.getLogger(KMeansLineReader.class);


}
