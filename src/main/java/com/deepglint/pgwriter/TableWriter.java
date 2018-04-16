package com.deepglint.pgwriter;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableWriter implements Appendable, Flushable, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(TableWriter.class);
    private static final String DRIVER = "org.postgresql.Driver";
    private static final String JDBC_PREFIX = "jdbc:postgresql://";
    private static final String DEFAULT_HOST = "localhost:5432";
    private static final char COLUMN_DELIMITER = '\u0002';
    private static final char ROW_DELIMITER = '\n';
    private static final char QUOTE_CHARACTER = '\u0003';
    private static final String TABLE_TOKEN = "<T>";
    private static final String COLUMNS_TOKEN = "<C>";
    private static final String COPY_TEMPLATE = "COPY <T> (<C>) FROM STDIN CSV NULL '' DELIMITER E'\u0002' QUOTE E'\u0003' ESCAPE E'\\\\' ";
    private static String [] filters;
    private final Connection fConnection;
    private final String fCopyCommand;
    private final CopyManager fCopyManager;
    private final int fLastColumnNo;
    private final int fCapacity;
    private final StringBuilder fBuffer;
    private int iWatermark;
    private int iColumnNo;
    private int filterFrom = 0;
    private boolean dofilter = true;
    private int filterColumn = 0;
    private int ftsColumn = -1;

    public TableWriter(String aHost, String aDatabase, String aUsername, String aPassword, String aTableName, String[] aColumnNames, int aCapacity, String [] filters) throws IOException {
        assert aDatabase != null;

        assert aTableName != null;

        assert aColumnNames != null;

        assert aCapacity >= 0;
        this.filters = filters;
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException var12) {
            throw new RuntimeException("Driver not found [org.postgresql.Driver]");
        }

        if (aHost == null || aHost.length() == 0) {
            aHost = "localhost:5432";
        }

        StringBuilder columns = new StringBuilder();
        columns.append(aColumnNames[0]);

        //查找写入数据库中的列号
        for(int i = 1; i < aColumnNames.length; ++i) {
            columns.append(',');
            columns.append(aColumnNames[i]);
        }
        for(int i = 0; i<aColumnNames.length; i++){
            if(aColumnNames[i].equals("sensor_id") && filters != null){
                filterColumn = i;
                logger.info("filter columns at: " + filterColumn);
            }
            if(aColumnNames[i].equals("fts")){
                ftsColumn = i;
                logger.info("add flush time columns at: " + ftsColumn);
            }
        }

        String copyCommand = "COPY <T> (<C>) FROM STDIN CSV NULL '' DELIMITER E'\u0002' QUOTE E'\u0003' ESCAPE E'\\\\' ";
        copyCommand = copyCommand.replace("<T>", aTableName);
        copyCommand = copyCommand.replace("<C>", columns.toString());
        this.fCapacity = aCapacity;
        this.fBuffer = new StringBuilder(this.fCapacity);
        this.fLastColumnNo = aColumnNames.length - 1;
        this.iColumnNo = 0;
        this.iWatermark = 0;
        this.fCopyCommand = copyCommand;

        try {
            String database = "jdbc:postgresql://" + aHost + "/" + aDatabase;
            this.fConnection = DriverManager.getConnection(database, aUsername, aPassword);
            this.fCopyManager = new CopyManager((BaseConnection)this.fConnection);
        } catch (SQLException var11) {
            throw new IOException(var11.getMessage());
        }
    }

    public Appendable append(char c) {
        assert c > 0;
        this.fBuffer.append(c);
        return this;
    }

    public Appendable append(CharSequence csq) throws IOException {
        assert csq != null;
        if(filters != null && iColumnNo == filterColumn){
            for (String tmp : filters){
                if(csq.toString().equals(tmp)){
                    dofilter=false;
                    logger.info("Filter: "+csq);
                }
            }
        }
        if(ftsColumn != -1 && iColumnNo == ftsColumn){
            this.fBuffer.append(System.currentTimeMillis());
            this.next();
        }
        this.fBuffer.append(csq);
        return this;
    }

    public Appendable append(CharSequence csq, int start, int end) {
        assert csq != null;

        this.fBuffer.append(csq, start, end);
        return this;
    }

    public int next() throws IOException {

        if (this.iColumnNo == this.fLastColumnNo) {
            if(filters != null){
                if(dofilter){
                    filterFrom = fBuffer.lastIndexOf("\n");
                    if(filterFrom == -1){
                        filterFrom = 0;
                    }
                    fBuffer.delete(filterFrom, fBuffer.length());
                    if(filterFrom != 0){
                        this.fBuffer.append('\n');
                    }
                }else{
                    this.fBuffer.append('\n');
                }
            }else{
                this.fBuffer.append('\n');
            }
            this.iWatermark = this.fBuffer.length();
            this.iColumnNo = 0;
            dofilter = true;
        } else {
            this.fBuffer.append('\u0002');
            ++this.iColumnNo;
        }

        if (this.fBuffer.length() > this.fCapacity) {
            this.flush();
        }

        return this.iColumnNo;
    }

    public boolean firstColumn() {
        return this.iColumnNo == 0;
    }

    public boolean lastColumn() {
        return this.iColumnNo == this.fLastColumnNo;
    }

    public int getColumnNo() {
        return this.iColumnNo;
    }

    public void flush() throws IOException {
        if (this.iWatermark > 0) {
            try {
                logger.info("command: " + this.fCopyCommand);
                logger.info("message: " + this.fBuffer.substring(0, this.iWatermark).toString());
                this.fCopyManager.copyIn(this.fCopyCommand, new StringReader(this.fBuffer.substring(0, this.iWatermark).toString()));
                logger.info("buffer: "  + fBuffer.toString());
                this.fBuffer.delete(0, this.iWatermark);
                this.iWatermark = 0;
            } catch (SQLException var2) {
                throw new IOException(var2.getMessage());
            }
        }

    }

    public boolean isEmpty() {
        return this.fBuffer.length() == 0;
    }

    public Connection getConnection() {
        return this.fConnection;
    }

    public void close() throws IOException {
        this.flush();
        try {
            this.fConnection.close();
        } catch (SQLException var2) {
            throw new IOException(var2.getMessage());
        }
    }
}
