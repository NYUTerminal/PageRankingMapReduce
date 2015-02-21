import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class PageRanking {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err
					.println("Usage: Page Ranking <input path> <output path>");
			System.exit(-1);
		}
	    JobConf jobConf = new JobConf(PageRanking.class); 
	    jobConf.setJarByClass(PageRanking.class);
	    jobConf.setJobName("Page Ranking");
		FileInputFormat.setInputPaths(jobConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
		jobConf.setMapperClass(PageRankingMapper.class);
		jobConf.setReducerClass(PageRankingReducer.class);
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(Text.class);
		JobClient.runJob(jobConf);
	}
}