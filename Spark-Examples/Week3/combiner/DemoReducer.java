package main;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

// Author: Nat Tuck
public class DemoReducer extends Reducer<Text, Text, Text, Text> {
	private int reduces;
	private String sampleLetters = ""; 

	@Override
	protected void setup(Context ctx) {
		reduces = 0;
		System.out.println("DemoReducer.setup");
	}

	@Override
	protected void cleanup(Context ctx) {
		System.out.println("DemoReducer.cleanup");
	        System.out.println ("There are " + reduces + " words starting with letters like [" + sampleLetters + "]");
	}

	@Override
	protected void reduce(Text key, Iterable<Text> vals, Context ctx) {
		int count = 0;

		for (Text xx : vals) {
			count += Integer.parseInt(xx.toString());
		}

		try {
		    ctx.write(key, new Text("" + count));
		}
		catch (Exception _e) {
		    throw new Error("I give up");
		}
		
		if(key.toString()!=null && !key.toString().equals("") && !sampleLetters.contains(key.toString().substring(0,1)))
			sampleLetters = sampleLetters + " " + key.toString().substring(0,1);
		

		reduces += 1;
	}
}
