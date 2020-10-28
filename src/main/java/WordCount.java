import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.io.WritableComparable;

public class WordCount {

  public static class TokenizerMapper
          extends Mapper<Object, Text, Text, IntWritable>{

    static enum CountersEnum { INPUT_WORDS }

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    private boolean caseSensitive;
    private Set<String> patternsToSkip = new HashSet<String>();

    private Configuration conf;
    private BufferedReader fis;

    /*context:
     * 它是mapper的一个内部类，
     * 简单的说是为了在map或是reduce任务中跟踪task的状态，记录了map执行的上下文，
     * 在mapper类中，这个context可以存储一些job conf的信息，比如运行时参数等，我们可以在map函数中处理这个信息
     */

    /*URI:
     * URL的拓展集，暂时理解为文件所在路径
     */
    @Override
    public void setup(Context context) throws IOException,
            InterruptedException {
      conf = context.getConfiguration();
      //默认忽略大小写
      caseSensitive = conf.getBoolean("wordcount.case.sensitive", false);
      //默认不跳过任何停词
      if (conf.getBoolean("wordcount.skip.patterns", false)) {
        URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
        for (URI patternsURI : patternsURIs) {
          Path patternsPath = new Path(patternsURI.getPath());
          String patternsFileName = patternsPath.getName();
          parseSkipFile(patternsFileName);
        }
      }
    }

    private void parseSkipFile(String fileName) {
      try {
        fis = new BufferedReader(new FileReader(fileName));
        String pattern = null;
        while ((pattern = fis.readLine()) != null) {
          //如果是英文单词停词表，则匹配时需要前后都不是字母,replaceAll采取正则编译
          if(fileName.equals("stop-word-list.txt")){
            patternsToSkip.add("\\b"+pattern+"\\b");
          }
          //数字直接用正则，添加在punctuation.txt中了
          else{
            patternsToSkip.add(pattern);
          }
        }
      } catch (IOException ioe) {
        System.err.println("Caught exception while parsing the cached file '"
                + StringUtils.stringifyException(ioe));
      }
    }

    @Override
    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
      String line = (caseSensitive) ?
              value.toString() : value.toString().toLowerCase();
      for (String pattern : patternsToSkip) {
        line = line.replaceAll(pattern, " ");
      }
      StringTokenizer itr = new StringTokenizer(line);
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        //单词长度>=3
        if(word.getLength()>=3) {
          context.write(word, one);
        }
        Counter counter = context.getCounter(CountersEnum.class.getName(),
                CountersEnum.INPUT_WORDS.toString());
        counter.increment(1);
      }
    }
  }

  public static class IntSumReducer
          extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  //自定义outputformat,以符合作业要求格式
  public static  class RankOutputFormat<K,V> extends TextOutputFormat<K,V>{
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
      Configuration conf = job.getConfiguration();
      boolean isCompressed = getCompressOutput(job);
      //输出时分隔key和value的改为,
      String keyValueSeparator = conf.get(SEPARATOR, ",");
      CompressionCodec codec = null;
      String extension = "";
      if (isCompressed) {
        Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, GzipCodec.class);
        codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, conf);
        extension = codec.getDefaultExtension();
      }

      Path file = this.getDefaultWorkFile(job, extension);
      FileSystem fs = file.getFileSystem(conf);
      FSDataOutputStream fileOut = fs.create(file, false);
      return isCompressed ? new LineRecordWriter(new DataOutputStream(codec.createOutputStream(fileOut)), keyValueSeparator) : new LineRecordWriter(fileOut, keyValueSeparator);
    }
    protected static class LineRecordWriter<K, V> extends RecordWriter<K, V> {
      private static final byte[] NEWLINE;
      protected DataOutputStream out;
      private final byte[] keyValueSeparator;

      public LineRecordWriter(DataOutputStream out, String keyValueSeparator) {
        this.out = out;
        this.keyValueSeparator = keyValueSeparator.getBytes(StandardCharsets.UTF_8);
      }

      public LineRecordWriter(DataOutputStream out) {
        this(out, "\t");
      }

      private void writeObject(Object o) throws IOException {
        if (o instanceof Text) {
          Text to = (Text)o;
          this.out.write(to.getBytes(), 0, to.getLength());
        } else {
          this.out.write(o.toString().getBytes(StandardCharsets.UTF_8));
        }

      }
      //直接用一个数字计数，表示名次
      public  static IntWritable rank=new IntWritable(1);
      //此函数写具体格式
      public synchronized void write(K key, V value) throws IOException {
        boolean nullKey = key == null || key instanceof NullWritable;
        boolean nullValue = value == null || value instanceof NullWritable;
        if ((!nullKey || !nullValue)&&rank.compareTo(new IntWritable(100))<=0) {
          //输出排名:
          this.writeObject(rank);
          this.writeObject(":");
          //以下输出单词,次数
          if (!nullValue) {
            this.writeObject(value);
          }
          if (!nullKey && !nullValue) {
            this.out.write(this.keyValueSeparator);
          }
          if (!nullKey) {
            this.writeObject(key);
          }

          this.out.write(NEWLINE);
        }
        rank.set(rank.get()+1);
      }

      public synchronized void close(TaskAttemptContext context) throws IOException {
        this.out.close();
      }

      static {
        NEWLINE = "\n".getBytes(StandardCharsets.UTF_8);
      }
    }

  }

  //自定义comparator改为降序（默认升序）
  private static class IntWritableDecreasingComparator extends IntWritable.Comparator {
    public int compare(WritableComparable a, WritableComparable b) {
      return -super.compare(a, b);
    }

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      return -super.compare(b1, s1, l1, b2, s2, l2);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
    String[] remainingArgs = optionParser.getRemainingArgs();
    //输入参数可以是in out也可以是in out -skip
    if (remainingArgs.length!=2&&remainingArgs.length!=3) {
      System.err.println("Usage: WordCount <in> <out> [-skip]");
      System.exit(2);
    }
    //job1:word count
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    //自定义outputformat以按特定格式输出结果
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    List<String> otherArgs = new ArrayList<String>();
    for (int i=0; i < remainingArgs.length; ++i) {
      //若输入-skip则固定跳过以下两个txt文件中的pattern,若不输入则不跳过任何pattern
      if ("-skip".equals(remainingArgs[i])) {
        job.addCacheFile(new Path("punctuation.txt").toUri());
        job.addCacheFile(new Path("stop-word-list.txt").toUri());
        job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
      } else {
        otherArgs.add(remainingArgs[i]);
      }
    }
    FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
    //建立中转文件夹，将job1的结果转给job2
    Path midRes=new Path("midRes");
    FileOutputFormat.setOutputPath(job,midRes);

    //job2: sort desc
    if(job.waitForCompletion(true))
    {
      Job sortJob =Job.getInstance(conf, "sort desc");
      sortJob.setJarByClass(WordCount.class);
      FileInputFormat.addInputPath(sortJob, midRes);
      sortJob.setInputFormatClass(SequenceFileInputFormat.class);
      /*InverseMapper由hadoop库提供，作用是实现map()之后的数据对的key和value交换*/
      sortJob.setMapperClass(InverseMapper.class);
      FileOutputFormat.setOutputPath(sortJob, new Path(otherArgs.get(1)));
      sortJob.setOutputKeyClass(IntWritable.class);
      sortJob.setOutputValueClass(Text.class);
      sortJob.setOutputFormatClass(RankOutputFormat.class);
      //IntWritableDecreasingComparator是一个降序comparator（默认升序）
      sortJob.setSortComparatorClass(IntWritableDecreasingComparator.class);
      if(sortJob.waitForCompletion(true)){
        //最后把中转文件夹删掉
        FileSystem.get(conf).deleteOnExit(midRes);
        System.exit( 0 );
      }
      else{
        System.out.println("job2 failed\n");
        System.exit(1);
      }
    }
    else{
      System.out.println("job1 failed");
      System.exit(1);
    }
  }
}