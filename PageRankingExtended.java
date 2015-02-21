import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class PageRankingExtended {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err
					.println("Usage: MaxTemperature <input path> <output path>");
			System.exit(-1);
		}
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		for (int i = 0; i < 3; i++) {
			JobConf jobConf = new JobConf(PageRankingExtended.class);
			jobConf.setJarByClass(PageRankingExtended.class);
			jobConf.setJobName("PageRankingExtra");
			FileInputFormat.setInputPaths(jobConf, inputPath);
			FileOutputFormat.setOutputPath(jobConf, outputPath);
			jobConf.setMapperClass(PageRankingExtendedMapper.class);
			jobConf.setReducerClass(PageRankingExtendedReducer.class);
			jobConf.setOutputKeyClass(Text.class);
			jobConf.setOutputValueClass(Text.class);
			JobClient.runJob(jobConf);
			inputPath = new Path(outputPath + "/part-00000");
			outputPath = new Path(outputPath+String.valueOf(i));
		}
	}
}