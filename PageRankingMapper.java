import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class PageRankingMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		String line = value.toString();
		// L() the denominator value in the page
		// ranking algorithm.

		String[] split = line.split(" ");
		if (split != null) {
			int length = split.length;
			if (length >= 3) {
				String firstPage = split[0];
				double newPr;
				double outBoundLins = length - 2;
				double currentPr = Double.valueOf(split[length - 1]);
				for (int i = 1; i < length - 1; i++) {
					newPr = currentPr / outBoundLins;
					Text mapValue = new Text(firstPage + ", "
							+ String.valueOf(newPr));
					output.collect(new Text(split[i]), mapValue);
				}
				StringBuffer valuesWithOutPageRank = new StringBuffer();
				valuesWithOutPageRank.append(split[1]);
				for (int j = 2; j < length - 1; j++) {
					valuesWithOutPageRank.append(" " + split[j]);
				}
				output.collect(new Text(split[0]), new Text(
						valuesWithOutPageRank.toString()));
			}
		} else {
			// Log it properly
			System.out.println("Invalid Row in the input");
		}

	}
}
