import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class PageRankingExtendedReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		String value = "";
		double finalPr = 0;
		while (values.hasNext()) {
			String currentString = ((Text) values.next()).toString().trim();
			String[] split = currentString.split(",");
			if (split != null && split.length >= 2) {
				finalPr += Double.valueOf(split[1]);
			}else if(split.length==1){
				value = split[0] + " ";
			}
		}
		output.collect(key, new Text(value + String.valueOf(finalPr)));
	}
}
