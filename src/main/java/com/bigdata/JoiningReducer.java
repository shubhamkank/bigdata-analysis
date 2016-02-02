package com.bigdata;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
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
public class JoiningReducer extends Reducer<TaggedKey, FlightDataRecord, NullWritable, Text> {

    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private Calendar calendar = Calendar.getInstance();

    @Override
    protected void reduce(TaggedKey key, Iterable<FlightDataRecord> values, Context context)
            throws IOException, InterruptedException {

        List<FlightDataRecord> tagZero = new ArrayList<>();
        List<FlightDataRecord> tagOne = new ArrayList<>();


        for(FlightDataRecord value : values) {

            if(value.tag.toString().equals("0")) {
                tagZero.add(new FlightDataRecord(new Text(value.tag.toString()), new Text(value.origin.toString()),
                        new Text(value.dest), new Text(value.airlineId), new Text(value.uniqCarrier),
                        new Text(value.date), new IntWritable(value.depTime.get()), new DoubleWritable(value.delay.get())));
            }
            else if(value.tag.toString().equals("1")) {
                tagOne.add(new FlightDataRecord(new Text(value.tag.toString()), new Text(value.origin.toString()),
                        new Text(value.dest), new Text(value.airlineId), new Text(value.uniqCarrier),
                        new Text(value.date), new IntWritable(value.depTime.get()), new DoubleWritable(value.delay.get())));
            }
        }

        if(tagZero.isEmpty() || tagOne.isEmpty()) {
            return;
        }

        try {

            for (int i = 0; i < tagZero.size(); i++) {

                Date date1 = dateFormat.parse(tagZero.get(i).date.toString());

                for (int j = 0; j < tagOne.size(); j++) {

                    Date date2 = dateFormat.parse(tagOne.get(j).date.toString());

                    calendar.setTime(date1);
                    calendar.add(Calendar.DATE, 2);

                    if (date2.compareTo(calendar.getTime()) != 0) {
                        continue;
                    }
                    context.write(NullWritable.get(), new Text(generateOutput(tagZero.get(i), tagOne.get(j))));
                }
            }
        } catch (ParseException pe) {
            pe.printStackTrace();
        }
    }

    public String generateOutput(FlightDataRecord record1, FlightDataRecord record2) {

        return record1.origin.toString() + "\t"
                + record1.dest.toString()+ "\t"
                + record2.dest.toString() + "\t"
                + record1.date.toString() + "\t"
                + record2.date.toString() + "\t"
                + record1.depTime.toString() + "\t"
                + record2.depTime.toString() + "\t"
                + record1.delay.get() + "\t"
                + record2.delay.get() + "\t"
                + record1.airlineId.toString() + "\t"
                + record2.airlineId.toString() + "\t"
                + record1.uniqCarrier.toString() + "\t"
                + record2.uniqCarrier.toString() + "\t"
                + (record1.delay.get() + record2.delay.get());
    }
}
