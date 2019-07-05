package com.kgc.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class HBaseConfigUtil {
    public static Configuration getHbaseConfiguration(){
        Configuration configuration= HBaseConfiguration.create();
        configuration.addResource(new Path("core-site.xml"));
        configuration.addResource(new Path("hbase-site.xml"));
        return configuration ;
    }
}
