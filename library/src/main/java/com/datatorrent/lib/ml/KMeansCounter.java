package com.datatorrent.lib.ml;

public class KMeansCounter {

	double [][] centerSum;
	int [] instanceCount;
	
	public KMeansCounter(){
		
	}
	public KMeansCounter(int k, int centroidDimension){
		instanceCount=new int[k];
		centerSum=new double[k][centroidDimension];
	}

	public void clear(){
		for(int a = 0; a < instanceCount.length; a++){
			instanceCount[a] = 0;
			for(int b = 0; b < centerSum[a].length;b++){
				centerSum[a][b] = 0;
			}
		}
	}
	
	
	public void process(KMeansData inputcluster, int k){
		instanceCount[inputcluster.clusterCenterkey]++;
		for (int j = 0; j < inputcluster.pointVector.length; j++){
			centerSum[inputcluster.clusterCenterkey][j]+= inputcluster.pointVector[j];
		}
	}
}
