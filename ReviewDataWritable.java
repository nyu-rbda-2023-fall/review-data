import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

public class ReviewDataWritable implements Writable {
    private Text reviewId;
    private Text userId;
    private Text businessId;
    private DoubleWritable stars;
    private DoubleWritable useful;
    private DoubleWritable funny;
    private DoubleWritable cool; 

    public ReviewDataWritable() {
	   reviewId = new Text();
	   userId = new Text();
	   businessId = new Text();
	   stars = new DoubleWritable(0); 
    	   useful = new DoubleWritable(0);
	   funny = new DoubleWritable(0);
	   cool = new DoubleWritable(0);
    }

    public ReviewDataWritable(Text reviewId, Text userId, Text businessId, DoubleWritable stars, DoubleWritable useful, DoubleWritable funny, DoubleWritable cool){
	    this.reviewId = reviewId;
	    this.userId = userId;
	    this.businessId = businessId;
	    this.stars = stars;
	    this.useful = useful;
	    this.funny = funny;
	    this.cool = cool; 
    }

    public void setReviewId(Text reviewId){
	    this.reviewId = reviewId;
    }
    public void setUserId(Text userId){
	    this.userId = userId;
    }
    public void setStars(DoubleWritable stars){
	this.stars = stars;
    }

    public void setFunny(DoubleWritable funny){
	    this.funny = funny;
    }

    public void setUseful(DoubleWritable useful){
	    this.useful = useful;
    }

    public void setCool(DoubleWritable cool){
	    this.cool = cool;
    }
    public DoubleWritable getFunny(){
	    return funny;
    }
    public DoubleWritable getUseful(){
	    return useful;
    }
    public DoubleWritable getCool(){
	    return cool;
    }
    public DoubleWritable getStars(){
	    return stars;
    }
    public void readFields(DataInput in) throws IOException {
        userId.readFields(in);
        reviewId.readFields(in);
        businessId.readFields(in);
        stars.readFields(in);
	useful.readFields(in);
	cool.readFields(in);
	funny.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        userId.write(out);
        reviewId.write(out);
        businessId.write(out);
        stars.write(out);
	useful.write(out);
	cool.write(out);
	funny.write(out);
    }

    @Override
    public String toString() {
        return reviewId.toString() + "," + userId.toString() + "," + businessId.toString() + "," + stars.toString() + "," + useful.toString() + ","+  cool.toString() + "," + funny.toString();
    }

}

