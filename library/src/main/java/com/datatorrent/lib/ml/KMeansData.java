package com.datatorrent.lib.ml;

import java.util.Arrays;

import org.apache.commons.math3.ml.distance.EuclideanDistance;
 


public class KMeansData {

	int clusterCenterkey;
	double [] pointVector;
	 
	public KMeansData(){
		clusterCenterkey=-1;
	}

	public void clear(){
		clusterCenterkey=-1;
	}
	
	/*Assigns point to nearest centroid*/
	public void process(double[][] clusterCenterList, double[] inputpointVector){
		
		double nearestDistance=0;
		EuclideanDistance ed = new EuclideanDistance();
		pointVector=inputpointVector;
		for (int i=0 ; i<clusterCenterList.length;i++) {
			double dist=ed.compute(clusterCenterList[i], inputpointVector);
			if (clusterCenterkey == -1) {
				clusterCenterkey = i;
				nearestDistance = dist;
			} else {
				if (nearestDistance > dist) {
					clusterCenterkey = i;
					nearestDistance = dist;
				}
			}
		}
	}

	@Override
	public int hashCode() {
		int result = 1;
		result = ((1 + (int)(Math.random()*100))%31); 
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		KMeansData other = (KMeansData) obj;
		if (clusterCenterkey != other.clusterCenterkey)
			return false;
		if (!Arrays.equals(pointVector, other.pointVector))
			return false;
		return true;
	}
	
}
