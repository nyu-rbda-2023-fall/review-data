import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ReviewMapper extends Mapper<LongWritable, Text, NullWritable, ReviewDataWritable> {

    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            // Parse JSON record
            JsonNode jsonNode = mapper.readTree(value.toString());

            // Extract fields from JSON record
            String reviewId = jsonNode.get("review_id").asText();
            String userId = jsonNode.get("user_id").asText();
            String businessId = jsonNode.get("business_id").asText();
            double stars = jsonNode.get("stars").asDouble();
            int useful = jsonNode.get("useful").asInt();
            int funny = jsonNode.get("funny").asInt();
            int cool = jsonNode.get("cool").asInt();
            String text = jsonNode.get("text").asText();
            String date = jsonNode.get("date").asText();
	    ReviewDataWritable data = new ReviewDataWritable(new Text(reviewId), new Text(userId), new Text(businessId), new DoubleWritable(stars), new DoubleWritable(useful), new DoubleWritable(funny), new DoubleWritable(cool));
	    context.write(NullWritable.get(), data);
        } catch (Exception e) {
            // Ignore invalid JSON records
        }
    }

}


