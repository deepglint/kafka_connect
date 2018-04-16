package com.deepglint.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.Map;

public class OffsetMetadata {
    private HashMap<String, Long> OFFSETS;
    private long timeMillis;
    private static final Logger logger = LoggerFactory.getLogger(OffsetMetadata.class);

    public long getTimeMillis() {
        return timeMillis;
    }

    public OffsetMetadata(String[] topoics, Integer[] partitions, Long[] offsets, long timeMillis) {
        OFFSETS = new HashMap<String, Long>(topoics.length);
        for(int i=0; i<topoics.length; i++){
            String key = topoics[i].toString()+partitions[i].toString();
            OFFSETS.put(key, offsets[i]);
        }
        this.timeMillis = timeMillis;
    }

    public long getQPS(OffsetMetadata offsetMetadata){
        for (Map.Entry entry : offsetMetadata.OFFSETS.entrySet()){
            logger.debug("previous patition: {}, previous offset: {} ", entry.getKey(), entry.getValue());
        }
        long total = 0;
        if(offsetMetadata == null){
            logger.error("previous offset Metadata should not be null!");
            return 0;
        }else{
            for (Map.Entry entry : OFFSETS.entrySet()) {
                logger.debug("current patition: {}, current offset: {} \n", entry.getKey(), entry.getValue());
                if (offsetMetadata.OFFSETS.containsKey(entry.getKey())) {
                    long tmp = (long)entry.getValue() - offsetMetadata.OFFSETS.get(entry.getKey());
                    total += tmp;
                }
            }
            long duration = timeMillis - offsetMetadata.getTimeMillis();
            logger.debug("current time: "+timeMillis);
            logger.debug("pervious time: "+offsetMetadata.getTimeMillis());
            logger.debug("duration: "+duration);
            long qps = (1000*total) / duration;
            logger.debug("flushed [{}] messages in [{}] ns", total, duration);
            return qps;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for(Map.Entry<String, Long> entry : OFFSETS.entrySet()){
            sb.append(entry.getKey());
            sb.append('\t');
            sb.append(entry.getValue());
            sb.append('\n');
        }
        String offsetString = sb.toString();
        return "OffsetMetadata{" +
                "OFFSETS= " + offsetString +
                "\ntimeMillis=" + timeMillis +
                "\n}";
    }
}
