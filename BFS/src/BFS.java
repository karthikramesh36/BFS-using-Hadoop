import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class BFS {
	
	public static enum UpdateCounter{
		UPDATED
	};
	
	
	public static void main(String args[]) throws Exception{
		
		//Number of iterations for initial purpose
		int iterations = 3;
		
		//To keep track of depth of BFS
		int depth = 0;	
		//Checking if program is passed a input file of the right format
		if (args.length != 1) {
		      System.err.println("Check input path/file ");
		      System.exit(-1);
		    }
		
		Path input_path;
		Path output_path;
		
		//To run the first iteration of mapreduce with input file 
		
		Job job = new Job();
	    
	    job.setJarByClass(BFS.class);
	    job.setJobName("BFS "+depth);
	    
	    //Setting up the path for mapreduce output_path
	    output_path = new Path("files/parallel-bfs/depth_"+depth);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, output_path);
	    
	    job.setMapperClass(BFSmap.class);
	    job.setReducerClass(BFSred.class);
	    
	    
	    // Below 2 Lines added
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(Text.class);

	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    job.waitForCompletion(true);
	    
	    depth++;
		
		for(int i = 1; i < iterations; i++){
				
				job = new Job();
			    
			    job.setJarByClass(BFS.class);
			    job.setJobName("BFS "+depth);
			    
			    input_path = new Path("files/parallel-bfs/depth_"+(depth - 1)+"/");
			    output_path= new Path("files/parallel-bfs/depth_"+depth);
			    
			    FileInputFormat.addInputPath(job, input_path);
			    FileOutputFormat.setOutputPath(job, output_path);
			    
			    job.setMapperClass(BFSmap.class);
			    job.setReducerClass(BFSred.class);
			    
			    
			    job.setMapOutputKeyClass(IntWritable.class);
			    job.setMapOutputValueClass(Text.class);
	
			    job.setOutputKeyClass(IntWritable.class);
			    job.setOutputValueClass(Text.class);
			    
			    job.waitForCompletion(true);
			    depth++;
		}
	    	
	}

}