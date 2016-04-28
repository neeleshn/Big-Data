package main;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

// Author: Nat Tuck
public class DemoReducer extends Reducer<Text, Text, Text, Text> {
    private int reduces;

    @Override
    protected void setup(Context ctx) {
        reduces = 0;

        System.out.println("DemoReducer.setup");
    }

    @Override
    protected void cleanup(Context ctx) {
        System.out.println("DemoReducer.cleanup");
        System.out.println("Reduces: " + reduces);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> vals, Context ctx) {
        int count = 0;

        for (Text _x : vals) {
            count += 1;
        }

        System.out.println("DemoReducer.reduce: key = " + key + ", |vals| = " + count);

        reduces += 1;
    }
}
