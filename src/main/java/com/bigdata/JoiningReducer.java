package com.bigdata;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * Created by shubham.kankaria on 01/02/16.
 */
public class JoiningReducer extends Reducer<TaggedKey, Text, NullWritable, Text> {

    private static enum ReduceSideCounter {
        COLLECTED_COUNT
    }

    protected long numOfValues = 0;
    protected long collected = 0;

    protected Context context;

    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private Calendar calendar = Calendar.getInstance();
    private static Splitter splitter = Splitter.on(',');

    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.context = context;
    }

    @Override
    protected void reduce(TaggedKey key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        List<Text> tagZero = new ArrayList<>();
        List<Text> tagOne = new ArrayList<>();

        System.out.println("JoinKey: " + key.joinKey.toString());

        for(Text value : values) {

            numOfValues++;

            if (this.numOfValues % 1000 == 0) {
                context.setStatus("key: " + key.joinKey.toString() + " numOfValues: " + this.numOfValues);
            }

            if(value.toString().startsWith("0")) {
                tagZero.add(new Text(value));
            }
            else if(value.toString().startsWith("1")) {
                tagOne.add(new Text(value));
            }
        }

        context.setStatus("key: " + key.joinKey.toString() + " numOfValues: " + this.numOfValues);

        if(tagZero.isEmpty() || tagOne.isEmpty()) {
            return;
        }

        try {

            for (int i = 0; i < tagZero.size(); i++) {

                List<String> zeroRow = Lists.newArrayList(splitter.split(tagZero.get(i).toString()));
                Date date1 = dateFormat.parse(zeroRow.get(5));

                for (int j = 0; j < tagOne.size(); j++) {

                    List<String> oneRow = Lists.newArrayList(splitter.split(tagOne.get(j).toString()));
                    Date date2 = dateFormat.parse(oneRow.get(5));

                    calendar.setTime(date1);
                    calendar.add(Calendar.DATE, 2);

                    if (date2.compareTo(calendar.getTime()) != 0) {
                        continue;
                    }
                    collected++;
                    context.write(NullWritable.get(), new Text(generateOutput(zeroRow, oneRow)));
                    context.getCounter(ReduceSideCounter.COLLECTED_COUNT).increment(1);
                    context.setStatus("key: " + key.joinKey.toString() + " collected: " + this.collected);
                }
            }
        } catch (ParseException pe) {
            pe.printStackTrace();
        }
    }

    public String generateOutput(List<String> record1, List<String> record2) {

        StringBuilder sb = new StringBuilder();
        sb.append(record1.get(1));
        sb.append('\t');
        sb.append(record1.get(2));
        sb.append('\t');
        sb.append(record2.get(2));
        sb.append('\t');
        sb.append(record1.get(5));
        sb.append('\t');
        sb.append(record2.get(5));
        sb.append('\t');
        sb.append(record1.get(6));
        sb.append('\t');
        sb.append(record2.get(6));
        sb.append('\t');
        sb.append(record1.get(7));
        sb.append('\t');
        sb.append(record2.get(7));
        sb.append('\t');
        sb.append(record1.get(3));
        sb.append('\t');
        sb.append(record2.get(3));
        sb.append('\t');
        sb.append(record1.get(4));
        sb.append('\t');
        sb.append(record2.get(4));
        sb.append('\t');
        sb.append(Double.valueOf(record1.get(7)) + Double.valueOf(record2.get(7)));

        return sb.toString();
    }
}
