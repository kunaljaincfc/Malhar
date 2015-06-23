package com.datatorrent.lib.ml;

/**
 * This class holds the configuration for the KMeans Clustering Algorithm. 
 * This configuration has attributes defined for clustering the data
 * 
 * @author
 *
 */
public class KMeansConfig {

  private int numClusters = 3;
  private int maxIterations = 50;
  private int dataDimension;
  private String inputDataFile;
  private String centroidListDir;
  private String centroidListFileName;
  boolean isLocalfs;
  private long maxEmittedTuples; 
    
  public KMeansConfig(){
    
  }
  
  /**
   * Constructor for NBConfig. Not all attributes are needed in all cases.
   * 
   * @param numClusters - Number of clusters. k = ? Default value=3
   * @param maxIterations - Maximum number of iterations before the algorithm stops. Default value=50
   * @param dataDimension - Dimension of the input data
   * @param inputDataFile - Input Data for Training the model
   * @param outputResultDir - Output directory for storing the centroid list
   * @param outputResultFileName - File name of the centroid list.The different iterations will have the iteration number appended to it.
   * @param isLocalfs - Whether to write/read file from local file system or HDFS
   * @param maxEmittedTuples - Maximum emitted tuples in a window 
   */
  public KMeansConfig(int numClusters, int maxIterations, int dataDimension, 
      String inputDataDir,  
      String centroidListDir, String centroidListFileName, boolean fs, long maxEmittedTuples){  
    this.numClusters = numClusters;
    this.maxIterations = maxIterations;
    this.dataDimension = dataDimension;
    this.inputDataFile = inputDataDir;
    this.centroidListDir = centroidListDir;
    this.centroidListFileName = centroidListFileName;
    this.isLocalfs = fs;
    this.maxEmittedTuples = maxEmittedTuples; 
  }

  /*
   * Getters and Setters
   */
  
  public int getNumClusters() {
    return numClusters;
  }

  public void setNumClusters(int numClusters) {
    this.numClusters = numClusters;
  }
  
  public int getMaxIterations() {
	    return maxIterations;
  }

  public void setMaxIterations(int maxIterations) {
	    this.maxIterations = maxIterations;
  }
  
  public int getDataDimension()
  {
    return dataDimension;
  }

  public void setDataDimension(int dataDimension)
  {
    this.dataDimension = dataDimension;
  }

  public String getInputDataFile() {
    return inputDataFile;
  }

  public void setInputDataFile(String inputDataFile) {
    this.inputDataFile = inputDataFile;
  }

  public String getCentroidListDir() {
    return centroidListDir;
  }

  public void setCentroidListDir(String centroidListDir) {
    this.centroidListDir = centroidListDir;
  }

  public String getCentroidListName() {
    return centroidListFileName;
  }

  public void setCentroidListName(String centroidListFileName) {
    this.centroidListFileName = centroidListFileName;
  }
  
  public boolean getisLocalfs() {
	    return isLocalfs;
  }

  public void setisLocalfs(boolean isLocalfs) {
	    this.isLocalfs = isLocalfs;
  }

  public long getMaxEmittedTuples() {
	    return maxEmittedTuples;
	  }

  public void setMaxEmittedTuples(long maxEmittedTuples) {
	    this.maxEmittedTuples = maxEmittedTuples;
  }
 

} 