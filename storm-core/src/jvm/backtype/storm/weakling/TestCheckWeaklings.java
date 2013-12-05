package backtype.storm.weakling;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * 
 * @author abhijeet
 * Revise and test the actual CheckWeaklings!
 *
 */
public class TestCheckWeaklings {
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		System.out.println(findOutliers(getInput(true)));
		
		System.out.println(findOutliers(getInput(false)));

	}

	private static List<Double> findOutliers(double[] a1) {
		
		Arrays.sort(a1);
		int n = a1.length;
		int qIdx = (n+1)/4;	////lower quartile
		double q1 = a1[qIdx - 1];
		if (0 != (n+1) % 4) {
			q1 = (a1[qIdx - 1] + a1[qIdx])/2;
		}
		
		qIdx = (3*(n+1))/4;	//upper quartile
	    double q2 = a1[qIdx - 1];
	    if (0 != (3*(n+1)) % 4) {
			q2 = (a1[qIdx - 1] + a1[qIdx])/2;
		}
	    
	    double IQR = q2 - q1;
	    double upFence = q2 + IQR*1.5;

	    List<Double> ret = new ArrayList<Double>();
	    
	    for (int i = 0, j = 0; i < a1.length; ++i) {
	    	if (a1[i] > upFence) {
	    		ret.add(a1[i]);
	    	}
	    }
		
	    return ret;
	}

	private static double[] getInput(boolean b) {
		
		double ret1[] = {1,1,2,1,6,6,7,6,20,21,25,35,45,45,45};
		double ret2[] = {1,1,2,1,2,1,2,3,1,1,1,2,1,4,5,1,4,5,45,56,70,200,200,200,200,200,200,200,200};
		double ret2a[] = {1,1,2,1,2,1,2,3,1,1,1,2,1,2,3,1,3,2,2,1,1,1,2,3,6,6,6,7,7,7};
		double ret2b[] = {1,1,2,1,2,2,1,2,6,7};
		double ret3[] = {1,2,3};
		double ret4[] = {1,2,3,4,5,6,4,8,7,6,8};
		double ret10[] = {0.01649484536082474,
				0.631911532385466,
				0.02395833333333333,
				0.1903409090909091,
				0.01365068253412671,
				0.1859756097560976,
				0.02081887578070784,
				1.17300131061599,
				0.01305841924398625,
				0.7145110410094637,
				0.04202848211184439,
				0.2741477272727273,
				0.01645082254112706,
				0.0471841704718417,
				0.0128383067314365,
				0.912303664921466,};
		
		if (b) {
			return ret10;
		}
		return ret4;

	}

}
