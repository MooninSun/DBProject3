package edu.pitt.sis.infsci2711.wordcount;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.DataInput;
import edu.pitt.sis.infsci2711.wordcount.Recommend.FriendCountWritable;
import edu.pitt.sis.infsci2711.wordcount.Recommend.Map;
import edu.pitt.sis.infsci2711.wordcount.Recommend.Reduce;

public class Recommend extends Configured implements Tool {

	 public static void main(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      int res = ToolRunner.run(new Configuration(), new Recommend(), args);
	      
	      System.exit(res);
	   }

	   @Override
	   public int run(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      Job job = new Job(getConf(), "Recommend");
	      job.setJarByClass(Recommend.class);
	      job.setOutputKeyClass(LongWritable.class);
	      job.setOutputValueClass(FriendCountWritable.class);

	      job.setMapperClass(Map.class);
	      job.setReducerClass(Reduce.class);

	      job.setInputFormatClass(TextInputFormat.class);
	      job.setOutputFormatClass(TextOutputFormat.class);

	      FileInputFormat.addInputPath(job, new Path(args[0]));
	      FileOutputFormat.setOutputPath(job, new Path(args[1]));

	      job.waitForCompletion(true);
	      
	      return 0;
	   }
	   

		public static class Map extends Mapper<LongWritable, Text, LongWritable, FriendCountWritable> {
		    private Text word = new Text();

		    @Override
		    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		        String line[] = value.toString().split("\t");
		        Long fromUser = Long.parseLong(line[0]);
		       // System.out.println(fromUser);
		        List toUsers = new ArrayList();

		        if (line.length == 2) {
		            StringTokenizer tokenizer = new StringTokenizer(line[1], ",");
		            while (tokenizer.hasMoreTokens()) {
		                Long toUser = Long.parseLong(tokenizer.nextToken());
		                toUsers.add(toUser);
		                context.write(new LongWritable((long)fromUser),
		                        new FriendCountWritable(toUser, -1L));
		            }

		            for (int i = 0; i < toUsers.size(); i++) {
		                for (int j = i + 1; j < toUsers.size(); j++) {
		                    context.write(new LongWritable((long)toUsers.get(i)),
		                            new FriendCountWritable((long)(toUsers.get(j)), fromUser));
		                    context.write(new LongWritable((long)toUsers.get(j)),
		                            new FriendCountWritable((long)(toUsers.get(i)), fromUser));
		                }
		                }
		 
		        }
		    }
		}
		public static class Reduce extends Reducer<LongWritable, FriendCountWritable, LongWritable, Text> {
		    @Override
		    public void reduce(LongWritable key, Iterable<FriendCountWritable> values, Context context)
		            throws IOException, InterruptedException {

		        // key is the recommended friend, and value is the list of mutual friends
		        final java.util.Map<Long, List> mutualFriends = new HashMap<Long, List>();

		        for (FriendCountWritable val : values) {
		            final Boolean isAlreadyFriend = (val.mutualFriend == -1);
		            final Long toUser = val.user;
		            final Long mutualFriend = val.mutualFriend;

		            if (mutualFriends.containsKey(toUser)) {
		                if (isAlreadyFriend) {
		                    mutualFriends.put(toUser, null);
		                } 
		                else if (mutualFriends.get(toUser) != null) {
		                    mutualFriends.get(toUser).add(mutualFriend);
		                }
		            } else {
		                if (!isAlreadyFriend) {
		                    mutualFriends.put(toUser, new ArrayList() {
		                    	{
		                            add(mutualFriend);
		                        }
		                    });
		                } else {
		                    mutualFriends.put(toUser, null);
		                }
		            }
		        }

		        java.util.SortedMap<Long, List> sortedMutualFriends = new TreeMap<Long, List>(new Comparator() {
		        	@Override
		        	public int compare(Object key11, Object key22) {
		        		long key1=(long)key11;
		        		long key2=(long)key22;
		                Integer v1 = mutualFriends.get(key1).size();
		                Integer v2 = mutualFriends.get(key2).size();
		                if (v1 > v2) {
		                    return -1;
		                } else if (v1.equals(v2) && key1 < key2) {
		                    return -1;
		                } else {
		                    return 1;
		                }
		            }

					
				
		        });

		        for (java.util.Map.Entry<Long, List> entry : mutualFriends.entrySet()) {
		            if (entry.getValue() != null) {
		                sortedMutualFriends.put(entry.getKey(), entry.getValue());
		            }
		        }

		        Integer i = 0;
		        String output = "";
		        for (java.util.Map.Entry<Long, List> entry : sortedMutualFriends.entrySet()) {
		        
		            if (i == 0) {
		                output = entry.getKey().toString();
		                //output = entry.getKey().toString() + " (" + entry.getValue().size() + ": " + entry.getValue() + ")";
		            } else {
		                output += "," + entry.getKey().toString();
		            }
		            ++i;
		            if(i>9)
			        	break;
		        }
		   
		        context.write(key, new Text(output));
		        
		    }
		}
	   static public class FriendCountWritable implements Writable {
		   public Long user;
		    public Long mutualFriend;

		    public FriendCountWritable(Long user, Long mutualFriend) {
		        this.user = user;
		        this.mutualFriend = mutualFriend;
		    }

		    public FriendCountWritable() {
		        this(-1L, -1L);
		    }

		    @Override
		    public void write(DataOutput out) throws IOException {
		        out.writeLong(user);
		        out.writeLong(mutualFriend);
		    }

		    @Override
		    public void readFields(DataInput in) throws IOException {
		        user = in.readLong();
		        mutualFriend = in.readLong();
		    }

		    @Override
		    public String toString() {
		        return " toUser: "
		                + Long.toString(user) + " mutualFriend: "
		                + Long.toString(mutualFriend);
		    }
		
	   }

}
