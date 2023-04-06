package org.zgg.storm.wordcount;

import org.apache.storm.shade.org.apache.commons.io.FileUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.*;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class ReadSpout extends BaseRichSpout {

    //用来收集Spout输出的Tuple
    SpoutOutputCollector collector;
    private FileReader fileReader;

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

        this.collector = collector;

        //从文件读取数据
//        try {
//            this.fileReader = new FileReader("C:\\Users\\ZGG\\Desktop\\test.txt");
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }

    }

    public void nextTuple() {
        //Utils.sleep(20000);
        collector.emit(new Values("newland in fuzhou we love newland"));
        //从文件读取数据
//        String str;
//        BufferedReader reader = new BufferedReader(fileReader);
//        try {
//            while((str = reader.readLine()) != null){
//                this.collector.emit(new Values(str));
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        //从文件夹读取多个文件
//        Collection<File>  files = FileUtils.listFiles(new File("G:/1"),
//                new String[]{"txt"},true);
//        if(files != null && files.size() >0 ){
//            for(File file :files){
//                try {
//                    List<String> lines = FileUtils.readLines(file);
//                    for (String line:lines){
//                        collector.emit(new Values(line));
//                    }
//                    FileUtils.moveFile(file,new File(file.getAbsolutePath()+"."+
//                            System.currentTimeMillis()));
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//
//            }
//        }

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("sentence"));
    }
}
