package main;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

// Author: Nat Tuck
public class DemoReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> vals, Context ctx) {
        Double salary=0.0;
        Double hr=0.0;

        for (Text xx : vals) {
            String[] parts = xx.toString().split("=");

            if (parts[0].equals("salary")) {
                salary = Double.parseDouble(parts[1]);
            }
            
            if (parts[0].equals("hr")) {
                hr = Double.parseDouble(parts[1]);
            }

        }

        try {
            if (hr != 0.0 && salary!=0.0) {
                Double prun=salary/hr;
                ctx.write(key, new Text(""+prun));
            }
        }
        catch (Exception ee) {
            ee.printStackTrace(System.err);
            throw new Error("I give up");
        }
    }
}
