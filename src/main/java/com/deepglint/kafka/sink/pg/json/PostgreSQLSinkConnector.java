package com.deepglint.kafka.sink.pg.json;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

public class PostgreSQLSinkConnector extends SinkConnector {
	
	private static final ConfigDef CONFIG_DEF = new ConfigDef()
			.define("db.host", Type.STRING, Importance.LOW, "The db.host to publish data to");
	
	
	@Override
	public ConfigDef config() {
		return CONFIG_DEF;
	}

	@Override
	public Config validate(Map<String, String> connectorConfigs) {
	    ConfigDef configDef = config();
	    List<ConfigValue> configValues = configDef.validate(connectorConfigs);
	    return new Config(configValues);
	}
	
	
  /**
   * Version of the connector
   */
  public final static String VERSION="1.0a"; 
  /**
   * Configuration properties for the connector
   */
  private Map<String,String> fProperties;
  
  /**
   * Returns version of the connector
   * @return version
   */
  @Override
  public String version() {
    
      return VERSION;//return version
      
  }//version()

  /**
   * Initialise the connector
   * @param ctx context of the connector
   */
  @Override
  public void initialize(ConnectorContext ctx) {
    //do nothing    
  }//initialize()
  
  /**
   * Initialise the connector
   * @param ctx context of the connector
   * @param taskConfigs task configuration
   */
  @Override
  public void initialize(ConnectorContext ctx, List<Map<String,String>> taskConfigs) {
    //do nothing
  }//initialize() 

  /**
   * Start the connector
   * @param props connector configuration properties
   */
  @Override
  public void start(Map<String, String> props) {
    
    fProperties=props;//set connector configuration properties
    
  }//start()

  /**
   * Stop the connector
   */
  @Override
  public void stop() {
    //do nothing
  }//stop()
  
  /**
   * Returns class of task
   * @return class of task
   */
   @Override
  public Class<? extends Task> taskClass() {
      return PostgreSQLSinkTask.class;//return task class
  }//taskClass()

  /**
   * Returns task configurations
   * @param maxTasks maximum tasks to execute
   * @return task configurations
   */
  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    
      ArrayList<Map<String, String>> configurations = new ArrayList<>();//construct list
      
      for (int i = 0; i < maxTasks; i++) {//for each task
        configurations.add(fProperties);//add connector configuration
      }//for each task
      
      return configurations;//return task configurations
      
  }//taskConfigs()

}//PostgreSQLSinkConnector{}
