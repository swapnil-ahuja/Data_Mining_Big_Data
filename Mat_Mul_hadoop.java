// Two phase matrix multiplication in Hadoop MapReduce
// Template file for homework #1 - INF 553 - Spring 2017
// - Wensheng Wu

import java.io.IOException;

// add your import statement here if needed
// you can only import packages from java.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TwoPhase {

    // mapper for processing entries of matrix A
    public static class PhaseOneMapperA 
	extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text outKey = new Text();
	private Text outVal = new Text();

	public void map(LongWritable key, Text value, Context context)
	    throws IOException, InterruptedException {
	    
	    // fill in your code
	    //System.out.println("hereA="+value.toString());
	    String s=value.toString();
	    String[] p=s.split(",");
	    outKey.set(p[1].trim());
	    outVal.set("A,"+p[0].trim()+","+p[2].trim());
	    context.write(outKey,outVal);
	}

    }

    // mapper for processing entries of matrix B
    public static class PhaseOneMapperB
	extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text outKey = new Text();
	private Text outVal = new Text();

	public void map(LongWritable key, Text value, Context context)
	    throws IOException, InterruptedException {
	    
	    // fill in your code
	    //System.out.println("hereB="+value.toString());
	    String s=value.toString();
	    String[] p=s.split(",");
	    outKey.set(p[0].trim());
	    outVal.set("B,"+p[1].trim()+","+p[2].trim());
	    context.write(outKey,outVal);

	}
    }

    public static class PhaseOneReducer
	extends Reducer<Text, Text, Text, Text> {

	private Text outKey = new Text();
	private Text outVal = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) 
	    throws IOException, InterruptedException {
	    HashMap<Integer,Integer> a=new HashMap<Integer,Integer>();
	    HashMap<Integer,Integer> b=new HashMap<Integer,Integer>();
	    

	    for (Text val : values) {
        
        String s=val.toString();
        String[] p=s.split(",");
        //System.out.println("red12="+key.toString()+","+p[0]+","+p[1]+","+p[2]);
        

        if(p[0].equals("A"))
        	a.put(Integer.parseInt(p[1]),Integer.parseInt(p[2]));
        else if(p[0].equals("B"))
        	b.put(Integer.parseInt(p[1]),Integer.parseInt(p[2]));
      	}
      	for(int i : a.keySet()){
      			
      	
      	for(int j : b.keySet()){
      		//System.out.println("key="+i+","+j+"value="+a.get(i)*b.get(j));	
      		outVal.set(String.valueOf(a.get(i)*b.get(j)));
      		outKey.set(i+","+j);
      		context.write(outKey,outVal);

      		}
      	}
	    

	}

    }

    public static class PhaseTwoMapper 
	extends Mapper<Text, Text, Text, Text> {
	
	private Text outKey = new Text();
	private Text outVal = new Text();

	public void map(Text key, Text value, Context context)
	    throws IOException, InterruptedException {

	    // fill in your code
	    	
	    		//System.out.println("Key="+key.toString()+"Val="+value.toString());
	    		context.write(key,value);
	    	
        

	}
    }

    public static class PhaseTwoReducer 
	extends Reducer<Text, Text, Text, Text> {
	
	private Text outKey = new Text();
	private Text outVal = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context)
	    throws IOException, InterruptedException {
	 
	    // fill in your code
	    	int sum=0;
	    	for(Text val:values){
	    		int t=Integer.parseInt(val.toString());
	    		sum=sum+t;
	    	}
	    	//System.out.println(key.toString()+";"+sum);
	    	outVal.set(String.valueOf(sum));
	    	context.write(key,outVal);

	}
    }


    public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();

	Job jobOne = Job.getInstance(conf, "phase one");

	jobOne.setJarByClass(TwoPhase.class);

	jobOne.setOutputKeyClass(Text.class);
	jobOne.setOutputValueClass(Text.class);

	jobOne.setReducerClass(PhaseOneReducer.class);

	MultipleInputs.addInputPath(jobOne,
				    new Path(args[0]),
				    TextInputFormat.class,
				    PhaseOneMapperA.class);

	MultipleInputs.addInputPath(jobOne,
				    new Path(args[1]),
				    TextInputFormat.class,
				    PhaseOneMapperB.class);

	Path tempDir = new Path("temp");

	FileOutputFormat.setOutputPath(jobOne, tempDir);
	jobOne.waitForCompletion(true);


	// job two
	Job jobTwo = Job.getInstance(conf, "phase two");
	

	jobTwo.setJarByClass(TwoPhase.class);

	jobTwo.setOutputKeyClass(Text.class);
	jobTwo.setOutputValueClass(Text.class);

	jobTwo.setMapperClass(PhaseTwoMapper.class);
	jobTwo.setReducerClass(PhaseTwoReducer.class);

	jobTwo.setInputFormatClass(KeyValueTextInputFormat.class);

	FileInputFormat.setInputPaths(jobTwo, tempDir);
	FileOutputFormat.setOutputPath(jobTwo, new Path(args[2]));
	
	jobTwo.waitForCompletion(true);
	
	FileSystem.get(conf).delete(tempDir, true);
	
    }
}
