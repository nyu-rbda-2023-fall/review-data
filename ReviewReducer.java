import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReviewReducer extends Reducer<Text, ReviewDataWritable, Text, ReviewDataWritable> {


    @Override
    public void reduce(Text key, Iterable<ReviewDataWritable> values, Context context) throws IOException, InterruptedException {
	
	double totalStars = 0;
	double totalFun = 0;
	double totalUseful = 0;
	double totalCool = 0;
        int total = 0;
	for (ReviewDataWritable value : values) {
		totalStars += value.getStars().get();
		totalFun += value.getFunny().get();
		totalUseful += value.getUseful().get();
		totalCool += value.getCool().get();
		total++;
	}
	ReviewDataWritable data = new ReviewDataWritable();
	data.setStars(new DoubleWritable(totalStars/total));
	data.setFunny(new DoubleWritable(totalFun/total));
	data.setUseful(new DoubleWritable(totalUseful/total));
	data.setCool(new DoubleWritable(totalCool/total));
	context.write(key, data);
    }
     /*
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Find the business ID with the highest average stars
        double maxStars = Double.MIN_VALUE;
        String maxBusinessId = "";

        for (Text key : context.getCurrentKey()) {
            double stars = context.getCurrentValue().get();
            if (stars > maxStars) {
                maxStars = stars;
                maxBusinessId = key.toString();
            }
        }

        // Emit the business ID with the highest average stars as the final output
        outputKey.set("Business ID with highest average stars");
        outputValue.set(maxStars);
        context.write(outputKey, outputValue);
    }*/

}
