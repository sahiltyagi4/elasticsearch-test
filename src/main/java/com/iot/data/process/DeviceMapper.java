package com.iot.data.process;

import java.io.FileReader;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.iot.data.databeans.IotRawDataBean;
import com.iot.data.utils.HbaseUtils;
import scala.Tuple2;
//Elasticsearch and Spark SQL
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark; 
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

public class DeviceMapper {
	
	private static Configuration hbaseConf;
	private static HTable hbaseTabName;
	// conf file settings
	private static String confFileName;
	private static String confTypeFile;
	private static JSONParser confParser = new JSONParser();
	private static Object confObj;
	private static JSONObject confJsonObject;
	private static String hbase_master_ip;
	private static String hbase_master_port;
	private static String hbase_zookeeper_port;
	private static String raw_iot_tab_name;
	private static String raw_iot_tab_colfam;
	private static Job newAPIJobConfigIotPushAdid;
	private static Job newAPIJobConfigIotPushDevAppsec;
	private static Job newAPIJobConfigIotActiveUsersPreprocess;
	private static Job newAPIJobConfigIotActiveUsersQry;
	private static Job newAPIJobConfigIotNewUsersQry;

	private static HbaseUtils hbaseUtilsObj;
	// hbase tabs 
	private static String dev_map_apps_tab_name;
	private static String dev_map_apps_tab_colfam;
	private static String push_appsecret_dev_tab_name;
	private static String push_appsecret_dev_tab_colfam;
	private static String active_users_preprocess_name;
	private static String active_users_preprocess_colfam;
	private static String active_users_tab_name;
	private static String new_users_tab_name;
	
	private static JavaSparkContext jsc;
	private static SQLContext sqlContext;
	
	private final static Logger logger = LoggerFactory.getLogger(DeviceMapper.class);
	
	public DeviceMapper() throws IOException, ParseException {
		// ###################### CONF FILE TYPE ######################
		// ###################### CONF FILE TYPE ######################
		// settings for production or testing (choose one)
		confTypeFile = "production_conf.json";
		// ###################### CONF FILE TYPE ######################
		// ###################### CONF FILE TYPE ######################
		// read conf file and corresponding params
//				dirNamefile = new File(System.getProperty("user.dir"));
//				currDirParentPath = new File(dirNamefile.getParent());
		confFileName = "/home/iot/fsociety/dal/conf/" + confTypeFile;
//				schemaFileName = "home/iot/conf/iot_data_schema.avsc";
		// read the json file and create a map of the parameters
		confObj = confParser.parse(new FileReader(
				confFileName));
        confJsonObject = (JSONObject) confObj;
        hbase_master_ip = (String) confJsonObject.get("server_ip");
        hbase_master_port = (String) confJsonObject.get("hbase_master_port");
        hbase_zookeeper_port = (String) confJsonObject.get("hbase_zookeeper_port");
	    // conf for reading from/ writing to hbase
  		hbaseConf = HBaseConfiguration.create();
 	    hbaseConf.set("hbase.master",hbase_master_ip + ":" + hbase_master_port);
 	    hbaseConf.set("hbase.zookeeper.quorum", hbase_master_ip);
 	    hbaseConf.set("hbase.zookeeper.property.clientPort", hbase_zookeeper_port);
 	    hbaseUtilsObj = new HbaseUtils(confTypeFile);
 	    // hbase table conf
 	    raw_iot_tab_name = (String) confJsonObject.get("hbase_table_primary");
        raw_iot_tab_colfam = (String) confJsonObject.get("hbase_raw_data_tab_colfam");
        dev_map_apps_tab_name = (String) confJsonObject.get("hbase_table_dev_map_apps");
        dev_map_apps_tab_colfam = (String) confJsonObject.get("hbase_dev_map_apps_tab_colfam");
        push_appsecret_dev_tab_name = (String) confJsonObject.get("hbase_dev_push_apps");
        push_appsecret_dev_tab_colfam = (String) confJsonObject.get("hbase_dev_push_apps_colfam");
        active_users_preprocess_name = (String) confJsonObject.get("hbase_activeUsers_preprocess");
        active_users_tab_name = (String) confJsonObject.get("hbase_activeUsers");
        new_users_tab_name = (String) confJsonObject.get("hbase_newUsers");
        
 	    // settings for writing to push adid table
        newAPIJobConfigIotPushAdid = Job.getInstance(hbaseConf);
        newAPIJobConfigIotPushAdid.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, dev_map_apps_tab_name);
        newAPIJobConfigIotPushAdid.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
 	    // settings for push appsecret table
        newAPIJobConfigIotPushDevAppsec = Job.getInstance(hbaseConf);
        newAPIJobConfigIotPushDevAppsec.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, push_appsecret_dev_tab_name);
        newAPIJobConfigIotPushDevAppsec.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
        
        // settings for active users preprocess and query tables
        newAPIJobConfigIotActiveUsersPreprocess = Job.getInstance(hbaseConf);
        newAPIJobConfigIotActiveUsersPreprocess.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, active_users_preprocess_name);
        newAPIJobConfigIotActiveUsersPreprocess.setOutputFormatClass(TableOutputFormat.class);
        
        newAPIJobConfigIotActiveUsersQry = Job.getInstance(hbaseConf);
        newAPIJobConfigIotActiveUsersQry.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, active_users_tab_name);
        newAPIJobConfigIotActiveUsersQry.setOutputFormatClass(TableOutputFormat.class);
        
        // settings for new users query tables
        newAPIJobConfigIotNewUsersQry = Job.getInstance(hbaseConf);
        newAPIJobConfigIotNewUsersQry.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, new_users_tab_name);
        newAPIJobConfigIotNewUsersQry.setOutputFormatClass(TableOutputFormat.class);

	}
	
	public static void mapDevs() throws Exception {
	    // set table and column names
	    hbaseConf.set(TableInputFormat.INPUT_TABLE, raw_iot_tab_name);
	    hbaseConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, raw_iot_tab_colfam); // column family 
	    String hbaseReadColList = raw_iot_tab_colfam+":app_secret " +
	    		raw_iot_tab_colfam+":packet_id " +
	    		raw_iot_tab_colfam+":device_id " +
	    		raw_iot_tab_colfam+":server_time " +
	    		raw_iot_tab_colfam+":UID__IMEI " +
	    		raw_iot_tab_colfam+":UID__WIFI_MAC_ADDRESS " +
	    		raw_iot_tab_colfam+":UID__PSEUDO_UNIQUE_ID " +
	    		raw_iot_tab_colfam+":AdIDActionAdIDKey " +
	    		raw_iot_tab_colfam+":PushActnPushKey ";
	    hbaseConf.set(TableInputFormat.SCAN_COLUMNS, hbaseReadColList); // column qualifiers
	    
	    // set up spark and spark-sql contexts
	    SparkConf sparkConf = new SparkConf().setAppName("queryDevs").setMaster("local[4]").set("spark.scheduler.mode", "FAIR");
	    jsc = new JavaSparkContext(sparkConf);
	    sqlContext = new SQLContext(jsc); 
	    // read data into rdd
	    JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = 
	            jsc.newAPIHadoopRDD(hbaseConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
	    // filter out all null valued data
	  //Get those bundles which have appSecret in the hbase tab
	    JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDDFiltered = hBaseRDD.filter(
	    		new Function<Tuple2<ImmutableBytesWritable, Result>, Boolean>() {
	    	        public Boolean call(Tuple2<ImmutableBytesWritable, Result> hbaseData) throws IOException {
	    	        	
	    	        	Result r = hbaseData._2;
	    	        	
	    	        	if (Bytes.toString(
	                		r.getValue(Bytes.toBytes(raw_iot_tab_colfam), 
	                				Bytes.toBytes("app_secret"))) == null){
	    	        		return false;
	    	        	}else{
	    	        		return true;
	    	        	}
	    	        }
	    	    }
	    	);
	    // properly format the data into a new RDD 
	    JavaRDD<IotRawDataBean> iotDatRDD = hBaseRDDFiltered.map(
	            new Function<Tuple2<ImmutableBytesWritable, Result>, IotRawDataBean>() {
	            	public IotRawDataBean call(
	            Tuple2<ImmutableBytesWritable, Result> entry) throws Exception {
	            	// get the rowid and set it as key in the pairRDD
	                Result r = entry._2;
	                String keyRow = Bytes.toString(r.getRow());
	             
	                // define java bean  
	                IotRawDataBean iotd = new IotRawDataBean();
	                // set values from hbase
	                iotd.setiotRawDataRowKey(keyRow);
	                iotd.setiotRawDataAppSecret((String) Bytes.toString(
	                		r.getValue(Bytes.toBytes(raw_iot_tab_colfam), 
	                				Bytes.toBytes("app_secret"))));
	                iotd.setiotRawDataPacketId((String) Bytes.toString(
	                		r.getValue(Bytes.toBytes(raw_iot_tab_colfam), 
	                				Bytes.toBytes("packet_id"))));
	                iotd.setiotRawDataDeviceId((String) Bytes.toString(
	                		r.getValue(Bytes.toBytes(raw_iot_tab_colfam), 
	                				Bytes.toBytes("device_id"))));
	                iotd.setiotRawDataServerTime((String) Bytes.toString(
	                		r.getValue(Bytes.toBytes(raw_iot_tab_colfam), 
	                				Bytes.toBytes("server_time"))));
	                iotd.setiotRawDataIMEI((String) Bytes.toString(
	                		r.getValue(Bytes.toBytes(raw_iot_tab_colfam), 
	                				Bytes.toBytes("UID__IMEI"))));
	                iotd.setiotRawDataWFMac((String) Bytes.toString(
	                		r.getValue(Bytes.toBytes(raw_iot_tab_colfam), 
	                				Bytes.toBytes("UID__WIFI_MAC_ADDRESS"))));
	                iotd.setiotRawDataUniqId((String) Bytes.toString(
	                		r.getValue(Bytes.toBytes(raw_iot_tab_colfam), 
	                				Bytes.toBytes("UID__PSEUDO_UNIQUE_ID"))));
	                iotd.setiotRawDataAdvertisingIdActionADVERTISINGIDKEY((String) Bytes.toString(
	                		r.getValue(Bytes.toBytes(raw_iot_tab_colfam), 
	                				Bytes.toBytes("AdIDActionAdIDKey"))));
	                iotd.setiotRawDataPUSHACTIONPUSHKEY((String) Bytes.toString(
	                		r.getValue(Bytes.toBytes(raw_iot_tab_colfam), 
	                				Bytes.toBytes("PushActnPushKey"))));

//	                 set timestamp
	                iotd.settimeStamp(Long.parseLong(Bytes.toString(
	                		r.getValue(Bytes.toBytes(raw_iot_tab_colfam), 
	                				Bytes.toBytes("server_time")))));
	                
	            return iotd;
	        }
	    });
	    
	    // convert to df
	    DataFrame iotRawDF = sqlContext.createDataFrame(iotDatRDD, IotRawDataBean.class);
	    // make it a table to execute sql queries
	    iotRawDF.registerTempTable("iotTab");
	    sqlContext.cacheTable("iotTab");
	    System.out.println("num rows-->" + iotRawDF.count());
	    System.out.println("==============================>");
	    
	    //code to read data from push key table, to map for new users
	    hbaseConf.set(TableInputFormat.INPUT_TABLE, push_appsecret_dev_tab_name);
	    hbaseConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, push_appsecret_dev_tab_colfam);
	    String hbasePusKeyColList = push_appsecret_dev_tab_colfam+":device_id " +
	    		push_appsecret_dev_tab_colfam+":app_secret " +
	    		push_appsecret_dev_tab_colfam+":push_key ";
	    hbaseConf.set(TableInputFormat.SCAN_COLUMNS, hbasePusKeyColList);
	    
	  //  JavaPairRDD<ImmutableBytesWritable, Result> pushHbasePairRDD = 
	    //												jsc.newAPIHadoopRDD(hbaseConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
	    
	    //////////////push this even before active users!!!!!!
	    //register temporary table for activeUsersPreprocess, by reading from hbase and dumping the data in dataframe and registering as temp table!!
	    //new users to hbase
	    
	    
	    
	    
	    DataFrame activeUsersPreDF = sqlContext.sql("SELECT CONCAT(a.iotRawDataAppSecret, '__', a.iotRawDataDeviceId, from_unixtime(a.timeStamp, 'Y'), '__',"
								+ " from_unixtime(a.timeStamp, 'M'), '__', from_unixtime(a.timeStamp, 'd')) AS rowkey, a.iotRawDataAppSecret AS appSecret, "
								+ " a.iotRawDataDeviceId AS deviceId, a.timeStamp, from_unixtime(a.timeStamp) AS date, from_unixtime(a.timeStamp, 'Y') AS year, "
								+ " from_unixtime(a.timeStamp, 'M') AS month, from_unixtime(a.timeStamp, 'd') AS day FROM iotTab a");
	    
	    
	    JavaEsSparkSQL.saveToEs(activeUsersPreDF, "spark/AUData");
	    
	    jsc.stop();
	    
	 }
	
	  
	public static void main(String[] args) throws Exception {
		  DeviceMapper devObj=new DeviceMapper();
		  devObj.mapDevs();
	}
	
}

