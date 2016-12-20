package com.iot.data.utils;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import com.iot.data.schema.PacketData;
import com.iot.data.schema.SerializableIotData;

public class HbaseUtils {

	private static String confFileName;
	private static JSONParser confParser = new JSONParser();
	private static Object confObj;
	private static JSONObject confJsonObject;
	// hbase params
	private static String raw_iot_tab_colfam;
	private static String push_adid_dev_map_tab_colfam;
	private static String push_appsecret_dev_tab_colfam;
	private static String active_users_preprocess_colfam;
	private static String active_users_tab_colfam;
	private static String new_users_preprocess_colfam;
	private static String eventDB_event_count_tab_colfam;
	
	private static String hbaseTableEvents2_colfam;
	private static String hbaseTableEventsAU_colfam;
	private static String hbaseTableEventsNU_colfam;
	
	private static String new_users_tab_colfam;
	private static String hbase_master_ip;
	private static String hbase_master_port;
	private static String hbase_zookeeper_port;
	
	private static Configuration hbaseIotConf;
	private static Put p;
	
	private final static Logger logger = LoggerFactory
			.getLogger(HbaseUtils.class);
	
	public HbaseUtils(String confTypeFile) throws IOException, ParseException {
		// read conf file and corresponding params
		confFileName = "/home/iot/fsociety/dal/conf/" + confTypeFile;
		//confFileName = "/Users/sahil/Desktop/conf/" + confTypeFile;
		// read the json file and create a map of the parameters
		confObj = confParser.parse(new FileReader(confFileName));
        confJsonObject = (JSONObject) confObj;
        // read parameters from conf file
        
        // hbase params
        raw_iot_tab_colfam = (String) confJsonObject.get("hbase_raw_data_tab_colfam");
        push_adid_dev_map_tab_colfam = (String) confJsonObject.get("hbase_dev_map_apps_tab_colfam");       
        push_appsecret_dev_tab_colfam = (String) confJsonObject.get("hbase_dev_push_apps_colfam");      
        active_users_preprocess_colfam = (String) confJsonObject.get("hbase_activeUsers_preprocess_colfam");       
        active_users_tab_colfam = (String) confJsonObject.get("hbase_activeUsers_colfam");     
        new_users_preprocess_colfam = (String) confJsonObject.get("hbase_newUsers_preprocess_colfam");
        new_users_tab_colfam = (String) confJsonObject.get("hbase_newUsers_colfam");

        eventDB_event_count_tab_colfam = (String) confJsonObject.get("hbase_eventsDBevents_colfam");
        
        hbaseTableEvents2_colfam = (String) confJsonObject.get("hbase_table_events2_colfam");
        hbaseTableEventsAU_colfam = (String) confJsonObject.get("hbase_table_events_AU_colfam");
        hbaseTableEventsNU_colfam = (String) confJsonObject.get("hbase_table_events_NU_colfam");

        hbase_master_ip = (String) confJsonObject.get("server_ip");
        hbase_master_port = (String) confJsonObject.get("hbase_master_port");
        hbase_zookeeper_port = (String) confJsonObject.get("hbase_zookeeper_port");
        
        // set up hbase conn
        hbaseIotConf = HBaseConfiguration.create();
        hbaseIotConf.set("hbase.master",hbase_master_ip + ":" + hbase_master_port);
        hbaseIotConf.set("hbase.zookeeper.quorum", hbase_master_ip);
        hbaseIotConf.set("hbase.zookeeper.property.clientPort", hbase_zookeeper_port);	
	}
	
	@SuppressWarnings("deprecation")
	public Tuple2<ImmutableBytesWritable, Put> cnvrtIotRawDataToPut(SerializableIotData iotRawPacket) {
		// put the data
		String currIotRowKey = iotRawPacket.getAppSecret() + "__" + iotRawPacket.getPacketId();
		//Put p = new Put(Bytes.toBytes(currIotRowKey));
		p = new Put(Bytes.toBytes(currIotRowKey));
		// add basic data to corresponding columns
		p.add(Bytes.toBytes(raw_iot_tab_colfam), Bytes.toBytes("packet_id"), Bytes.toBytes(iotRawPacket.getPacketId()));
		p.add(Bytes.toBytes(raw_iot_tab_colfam), Bytes.toBytes("app_secret"), Bytes.toBytes(iotRawPacket.getAppSecret()));
		p.add(Bytes.toBytes(raw_iot_tab_colfam), Bytes.toBytes("device_id"), Bytes.toBytes(iotRawPacket.getDeviceId()));
		p.add(Bytes.toBytes(raw_iot_tab_colfam), Bytes.toBytes("library"), Bytes.toBytes(iotRawPacket.getLibrary()));
		p.add(Bytes.toBytes(raw_iot_tab_colfam), Bytes.toBytes("library_version"), Bytes.toBytes(iotRawPacket.getLibraryVersion()));
		p.add(Bytes.toBytes(raw_iot_tab_colfam), Bytes.toBytes("server_ip"), Bytes.toBytes(iotRawPacket.getServerIp()));
		p.add(Bytes.toBytes(raw_iot_tab_colfam), Bytes.toBytes("server_time"), Bytes.toBytes(iotRawPacket.getServerTime()));
		// get unique_id_action in a hashmap and put the data
	    Map<String,String> currUidDataMap = new HashMap<String,String>();
	    currUidDataMap = iotRawPacket.getUNIQUEIDACTION();
	        	for (Map.Entry<String, String> entry : currUidDataMap.entrySet()) {
	        		p.add(Bytes.toBytes(raw_iot_tab_colfam),
							Bytes.toBytes("UID__"+entry.getKey()), Bytes.toBytes(entry.getValue()));
	        	}
	        	// Loop through "packet" and put the data
	        	// setup a few counts
	        	int countScreens = 0;
	        	int countEvents = 0;

	        	Map<String,HashMap<String,String>> finEventDataMap = new HashMap<String,HashMap<String,String>>();
	        	for (PacketData currPd : iotRawPacket.getPacket()) {
	        		// Install Referrer Data
	        		if ( currPd.getInstallReferrer() != null ){
	        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
								Bytes.toBytes("hasIRdata"), Bytes.toBytes(true));
	        			if ( currPd.getInstallReferrer().getAction() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("IRAction"), Bytes.toBytes(currPd.getInstallReferrer().getAction()));
	        			}
	        			if ( currPd.getInstallReferrer().getAppSessionId() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("IRAppSessionID"), Bytes.toBytes(currPd.getInstallReferrer().getAppSessionId()));
	        			}
	        			if ( currPd.getInstallReferrer().getReferrerString() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("IRRefString"), Bytes.toBytes(currPd.getInstallReferrer().getReferrerString()));
	        			}
	        			if ( currPd.getInstallReferrer().getTimestamp() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("IRTimestamp"), Bytes.toBytes(currPd.getInstallReferrer().getTimestamp()));
	        			}
	        			if ( currPd.getInstallReferrer().getUserId() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("IRUserId"), Bytes.toBytes(currPd.getInstallReferrer().getUserId()));
	        			}
	    			}
	        		// start session data
	        		if (currPd.getStartSession() != null){
	        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
								Bytes.toBytes("hasStartSessn"), Bytes.toBytes(true));
	        			if ( currPd.getStartSession().getAction() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("StartSessnAction"), Bytes.toBytes(currPd.getStartSession().getAction()));
	        			}
	        			if ( currPd.getStartSession().getAppSessionId() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("StartSessnAppSessionID"), Bytes.toBytes(currPd.getStartSession().getAppSessionId()));
	        			}
	        			if ( currPd.getStartSession().getScreenName() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("StartSessnScreenName"), Bytes.toBytes(currPd.getStartSession().getScreenName()));
	        			}
	        			if ( currPd.getStartSession().getTimestamp() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("StartSessnTimestamp"), Bytes.toBytes(currPd.getStartSession().getTimestamp()));
	        			}
	        			if ( currPd.getStartSession().getSessionId() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("StartSessnSessionID"), Bytes.toBytes(currPd.getStartSession().getSessionId()));
	        			}
	    			}
	        		// stop session data
	        		if ( currPd.getStopSession() != null ){
	        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
								Bytes.toBytes("hasStopSessn"), Bytes.toBytes(true));
	        			if ( currPd.getStopSession().getAction() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("StopSessnAction"), Bytes.toBytes(currPd.getStopSession().getAction()));
	        			}
	        			if ( currPd.getStopSession().getAppSessionId() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("StopSessnAppSessionID"), Bytes.toBytes(currPd.getStopSession().getAppSessionId()));
	        			}
	        			if ( currPd.getStopSession().getDuration() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("StopSessnDuration"), Bytes.toBytes(currPd.getStopSession().getDuration()));
	        			}
	        			if ( currPd.getStopSession().getTimestamp() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("StopSessnTimestamp"), Bytes.toBytes(currPd.getStopSession().getTimestamp()));
	        			}
	        			if ( currPd.getStopSession().getSessionId() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("StopSessnSessionID"), Bytes.toBytes(currPd.getStopSession().getSessionId()));
	        			}
	        			if ( currPd.getStopSession().getUserId() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("StopSessnUserID"), Bytes.toBytes(currPd.getStopSession().getUserId()));
	        			}
	    			}
	        		// screen data
                    if (currPd.getScreen() != null){
                    	// increment screen count and set up hbase cols according to count
                    	countScreens += 1;
                    	String curScreenCntStr = "__" + Integer.toString(countScreens);
                        p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                Bytes.toBytes("hasScreen"), Bytes.toBytes(true));
                        if ( currPd.getScreen().getAction() != null ){
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("ScreenAction" + curScreenCntStr), Bytes.toBytes(currPd.getScreen().getAction()));
                        }
                        if ( currPd.getScreen().getAppSessionId() != null ){
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("ScreenAppSessionID" + curScreenCntStr), Bytes.toBytes(currPd.getScreen().getAppSessionId()));
                        }
                        if ( currPd.getScreen().getTimestamp() != null ){
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("ScreenTimeStamp" + curScreenCntStr), Bytes.toBytes(currPd.getScreen().getTimestamp()));
                        }
                        if ( currPd.getScreen().getSessionId() != null ){
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("ScreenSessionID" + curScreenCntStr), Bytes.toBytes(currPd.getScreen().getSessionId()));
                        }
                        if ( currPd.getScreen().getScreenId() != null ){
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("ScreenScreenID" + curScreenCntStr), Bytes.toBytes(currPd.getScreen().getScreenId()));
                        }
                        // get properties in a hashmap and put the data
                        if ( currPd.getScreen().getProperties() != null ){
                            Map<String,String> currScreenMap = new HashMap<String,String>();
                            currScreenMap = currPd.getScreen().getProperties();
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("hasScreenProps"), Bytes.toBytes(true));
                            for (Map.Entry<String, String> entry : currScreenMap.entrySet())
                            {
                                p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                        Bytes.toBytes("ScreenProp__" + curScreenCntStr + "__" + entry.getKey()),
                                        	Bytes.toBytes(entry.getValue()));
                            }
                        }
                    }
                    // identity data
                    if (currPd.getIdentity() != null){
                        p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                Bytes.toBytes("hasID"), Bytes.toBytes(true));
                        if ( currPd.getIdentity().getAction() != null ){
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("IDAction"), Bytes.toBytes(currPd.getIdentity().getAction()));
                        }
                        if ( currPd.getIdentity().getAppSessionId() != null ){
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("IDAppSessionID"), Bytes.toBytes(currPd.getIdentity().getAppSessionId()));
                        }
                        if ( currPd.getIdentity().getTimestamp() != null ){
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("IDTimeStamp"), Bytes.toBytes(currPd.getIdentity().getTimestamp()));
                        }
                        if ( currPd.getIdentity().getSessionId() != null ){
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("IDSessionID"), Bytes.toBytes(currPd.getIdentity().getSessionId()));
                        }
                        if ( currPd.getIdentity().getUserId() != null ){
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("IDUserID"), Bytes.toBytes(currPd.getIdentity().getUserId()));
                        }
                        // get properties in a hashmap and put the data
                        if ( currPd.getIdentity().getProperties() != null ){
                            Map<String,String> currIdentityMap = new HashMap<String,String>();
                            currIdentityMap = currPd.getIdentity().getProperties();
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("hasIDProps"), Bytes.toBytes(true));
                            for (Map.Entry<String, String> entry : currIdentityMap.entrySet())
                            {
                                p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                        Bytes.toBytes("IDProp__"+entry.getKey()), Bytes.toBytes(entry.getValue()));
                            }
                        }
                    }
                    // events data
                    if (currPd.getEvents() != null){
                    	// increment events count and set up hbase cols according to count
                    	countEvents += 1;
                    	String curEventCntStr = "__" + Integer.toString(countEvents);
                    	// store the details in a map
                    	HashMap<String, String> currEventDataMap = new HashMap<String,String>();
                        p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                Bytes.toBytes("hasEvents"), Bytes.toBytes(true));
                        if ( currPd.getEvents().getAction() != null ){
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("EventsAction" + curEventCntStr), Bytes.toBytes(currPd.getEvents().getAction()));
                            currEventDataMap.put("EventsAction", currPd.getEvents().getAction());
                        }
                        if ( currPd.getEvents().getAppSessionId() != null ){
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("EventsAppSessionID" + curEventCntStr), Bytes.toBytes(currPd.getEvents().getAppSessionId()));
                            currEventDataMap.put("EventsAppSessionID", currPd.getEvents().getAppSessionId());
                        }
                        if ( currPd.getEvents().getTimestamp() != null ){
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("EventsTimeStamp" + curEventCntStr), Bytes.toBytes(currPd.getEvents().getTimestamp()));
                            currEventDataMap.put("EventsTimeStamp", currPd.getEvents().getTimestamp());
                        }
                        if ( currPd.getEvents().getSessionId() != null ){
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("EventsSessionID" + curEventCntStr), Bytes.toBytes(currPd.getEvents().getSessionId()));
                            currEventDataMap.put("EventsSessionID", currPd.getEvents().getSessionId());
                        }
                        if ( currPd.getEvents().getUserId() != null ){
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("EventsUserID" + curEventCntStr), Bytes.toBytes(currPd.getEvents().getUserId()));
                            currEventDataMap.put("EventsUserID", currPd.getEvents().getUserId());
                        }
                        if ( currPd.getEvents().getEvent() != null ){
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("Events" + curEventCntStr),
                                    Bytes.toBytes(currPd.getEvents().getEvent()));
                            currEventDataMap.put("Event", currPd.getEvents().getEvent());
                        }
                        if ( currPd.getEvents().getPosition() != null ){
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("EventsPosition"), Bytes.toBytes(currPd.getEvents().getPosition()));
                            currEventDataMap.put("EventsPosition", Long.toString( currPd.getEvents().getPosition() ));
                        }
                        // get properties in a hashmap and put the data
                        if ( currPd.getEvents().getProperties() != null ){
                            Map<String,String> currEventsMap = new HashMap<String,String>();
                            currEventsMap = currPd.getEvents().getProperties();
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("hasEventsProps"), Bytes.toBytes(true));
                            currEventDataMap.put("EventsProp", currPd.getEvents().getProperties().toString());
                            for (Map.Entry<String, String> entry : currEventsMap.entrySet())
                            {
                                p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                        Bytes.toBytes("EventsProp"+ curEventCntStr + "__" + entry.getKey()), Bytes.toBytes(entry.getValue()));
                            }
                        }
                        finEventDataMap.put("Event" + curEventCntStr, currEventDataMap);
                    }
	        		// push action data
	        		if (currPd.getPUSHACTION() != null){
	        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
								Bytes.toBytes("hasPushActn"), Bytes.toBytes(true));
	        			if ( currPd.getPUSHACTION().getAction() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("PushActnActionName"), Bytes.toBytes(currPd.getPUSHACTION().getAction()));
	        			}
	        			if ( currPd.getPUSHACTION().getAppSessionId() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("PushActnAppSessionID"), Bytes.toBytes(currPd.getPUSHACTION().getAppSessionId()));
	        			}
	        			if ( currPd.getPUSHACTION().getPUSHKEY() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("PushActnPushKey"), Bytes.toBytes(currPd.getPUSHACTION().getPUSHKEY()));
	        			}
	        			if ( currPd.getPUSHACTION().getTimestamp() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("PushActnTimestamp"), Bytes.toBytes(currPd.getPUSHACTION().getTimestamp()));
	        			}
	        			if ( currPd.getPUSHACTION().getSessionId() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("PushActnSessionID"), Bytes.toBytes(currPd.getPUSHACTION().getSessionId()));
	        			}
	        			if ( currPd.getPUSHACTION().getUserId() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("PushActnUserID"), Bytes.toBytes(currPd.getPUSHACTION().getUserId()));
	        			}
	    			}
	        		// adv_id action data
	        		if (currPd.getADVERTISINGIDACTION() != null){
	        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
								Bytes.toBytes("hasAdIDAction"), Bytes.toBytes(true));
	        			if ( currPd.getADVERTISINGIDACTION().getAction() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("AdIDActionActionName"), Bytes.toBytes(currPd.getADVERTISINGIDACTION().getAction()));
	        			}
	        			if ( currPd.getADVERTISINGIDACTION().getAppSessionId() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("AdIDActionAppSessionID"), Bytes.toBytes(currPd.getADVERTISINGIDACTION().getAppSessionId()));
	        			}
	        			if ( currPd.getADVERTISINGIDACTION().getADVERTISINGIDKEY() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("AdIDActionAdIDKey"), Bytes.toBytes(currPd.getADVERTISINGIDACTION().getADVERTISINGIDKEY()));
	        			}
	        			if ( currPd.getADVERTISINGIDACTION().getTimestamp() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("AdIDActionTimestamp"), Bytes.toBytes(currPd.getADVERTISINGIDACTION().getTimestamp()));
	        			}
	        			if ( currPd.getADVERTISINGIDACTION().getSessionId() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("AdIDActionSessionID"), Bytes.toBytes(currPd.getADVERTISINGIDACTION().getSessionId()));
	        			}
	        			if ( currPd.getADVERTISINGIDACTION().getADVERTISINGIDOPTOUT() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("AdIDActionAdIDOptOut"), Bytes.toBytes(currPd.getADVERTISINGIDACTION().getADVERTISINGIDOPTOUT()));
	        			}
	    			}
	        		// new device data
	        		if (currPd.getNewDevice() != null){
	        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
								Bytes.toBytes("hasNewDev"), Bytes.toBytes(true));
	        			if ( currPd.getNewDevice().getAction() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("NewDevAction"), Bytes.toBytes(currPd.getNewDevice().getAction()));
	        			}
	        			if ( currPd.getNewDevice().getTimestamp() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("NewDevTimeStamp"), Bytes.toBytes(currPd.getNewDevice().getTimestamp()));
	        			}
	        			if ( currPd.getNewDevice().getContext() != null ){
	        				p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("hasNewDevCxt"), Bytes.toBytes(true));
	        				// context features data
	        				if ( currPd.getNewDevice().getContext().getFeatures() != null ){
	        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasNewDevCxtFeatures"), Bytes.toBytes(true));
	        					if ( currPd.getNewDevice().getContext().getFeatures().getHasNFC() != null ){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("NewDevCxtFeaturesNFC"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasNFC()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getFeatures().getHasTelephony() != null ){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("NewDevCxtFeaturesTelephony"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasTelephony()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getFeatures().getHasGPS() != null ){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("hasNewDevCxtFeaturesGPS"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasGPS()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getFeatures().getHasAccelerometer() != null ){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("hasNewDevCxtFeaturesAcclroMtr"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasAccelerometer()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getFeatures().getHasBarometer() != null ){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("hasNewDevCxtFeaturesBaromtr"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasBarometer()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getFeatures().getHasCompass() != null ){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("hasNewDevCxtFeaturesCompass"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasCompass()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getFeatures().getHasGyroscope() != null ){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("hasNewDevCxtFeaturesGyro"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasGyroscope()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getFeatures().getHasLightsensor() != null ){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("hasNewDevCxtFeaturesLightSensr"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasLightsensor()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getFeatures().getHasProximity() != null ){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("hasNewDevCxtFeaturesProxmty"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasProximity()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getFeatures().getBluetoothVersion() != null ){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("hasNewDevCxtFeaturesBTVrsn"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getBluetoothVersion()));
	        					}
	        					
	        				}
	        				// context display data
	        				if ( currPd.getNewDevice().getContext().getDisplay() != null ){
	        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasNewDevCxtDisplay"), Bytes.toBytes(true));
	        					if ( currPd.getNewDevice().getContext().getDisplay().getDisplayHeight() != null ){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("NewDevCxtDisplayHeight"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getDisplay().getDisplayHeight()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getDisplay().getDisplayWidth() != null ){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("NewDevCxtDisplayWidth"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getDisplay().getDisplayWidth()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getDisplay().getDisplayDensity() != null ){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("NewDevCxtDisplayDensity"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getDisplay().getDisplayDensity()));
	        					}
	        				}
	        				// context total mem info data
	        				if ( currPd.getNewDevice().getContext().getTotalMemoryInfo() != null ){
	        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasNewDevCxtTotalMemry"), Bytes.toBytes(true));
	        					if ( currPd.getNewDevice().getContext().getTotalMemoryInfo().getTotalRAM() != null ){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("NewDevCxtTotalMemryRAM"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getTotalMemoryInfo().getTotalRAM()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getTotalMemoryInfo().getTotalStorage() != null ){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("NewDevCxtTotalMemryStorage"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getTotalMemoryInfo().getTotalStorage()));
	        					}
	        				}
	        			}
	        			
	        		}
	        		// device info data
	        		if (currPd.getDeviceInfo() != null){
	        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
								Bytes.toBytes("hasDevInfo"), Bytes.toBytes(true));
	        			if (currPd.getDeviceInfo().getAction() != null){
	        				p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("DevInfoAction"), 
									Bytes.toBytes(currPd.getDeviceInfo().getAction()));
	        			}
	        			if (currPd.getDeviceInfo().getTimestamp() != null){
	        				p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("DevInfoTimestamp"), 
									Bytes.toBytes(currPd.getDeviceInfo().getTimestamp()));
	        			}
	        			if (currPd.getDeviceInfo().getAppSessionId() != null){
	        				p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("DevInfoAppSessionID"), 
									Bytes.toBytes(currPd.getDeviceInfo().getAppSessionId()));
	        			}
	        			if (currPd.getDeviceInfo().getSessionId() != null){
	        				p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("DevInfoSessionID"), 
									Bytes.toBytes(currPd.getDeviceInfo().getSessionId()));
	        			}
	        			if (currPd.getDeviceInfo().getContext() != null){
	        				p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("hasDevInfoCxt"), Bytes.toBytes(true));
	        				if (currPd.getDeviceInfo().getContext().getAppBuild() != null){
	        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasDevInfoCxtAppBuild"), Bytes.toBytes(true));
	        					if (currPd.getDeviceInfo().getContext().getAppBuild().getPackageName() != null){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtAppBuildPackageName"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getAppBuild().getPackageName()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getAppBuild().getVersionCode() != null){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtAppBuildVrsnCode"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getAppBuild().getVersionCode()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getAppBuild().getVersionName() != null){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtAppBuildVrsnName"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getAppBuild().getVersionName()));
	        					}
	        				}
	        				// device info context device
	        				if (currPd.getDeviceInfo().getContext().getDevice() != null){
	        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasDevInfoCxtDevice"), Bytes.toBytes(true));
	        					if (currPd.getDeviceInfo().getContext().getDevice().getSdkVersion() != null){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtDeviceSDKVrsn"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getDevice().getSdkVersion()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getDevice().getReleaseVersion() != null){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtDeviceReleaseVrsn"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getDevice().getReleaseVersion()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getDevice().getDeviceBrand() != null){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtDeviceBrand"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getDevice().getDeviceBrand()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getDevice().getDeviceManufacturer() != null){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtDeviceManfactrer"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getDevice().getDeviceManufacturer()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getDevice().getDeviceModel() != null){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtDeviceModel"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getDevice().getDeviceModel()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getDevice().getDeviceBoard() != null){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtDeviceBoard"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getDevice().getDeviceBoard()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getDevice().getDeviceProduct() != null){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtDeviceProduct"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getDevice().getDeviceProduct()));
	        					}
	        				}
	        				// device info context locale
	        				if (currPd.getDeviceInfo().getContext().getLocale() != null){
	        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasDevInfoCxtLocale"), Bytes.toBytes(true));
	        					if (currPd.getDeviceInfo().getContext().getLocale().getDeviceCountry() != null){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtLocaleDevCountry"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getLocale().getDeviceCountry()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getLocale().getDeviceLanguage() != null){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtLocaleDevLang"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getLocale().getDeviceLanguage()));
	        					}
	        				}
	        				// device info context location
	        				if (currPd.getDeviceInfo().getContext().getLocation() != null){
	        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasDevInfoCxtLocation"), Bytes.toBytes(true));
	        					if (currPd.getDeviceInfo().getContext().getLocation().getLatitude() != null){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtLocationLat"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getLocation().getLatitude()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getLocation().getLongitude() != null){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtLocationLong"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getLocation().getLongitude()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getLocation().getSpeed() != null){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtLocationSpeed"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getLocation().getSpeed()));
	        					}
	        				}
	        				// device info context telephone
	        				if (currPd.getDeviceInfo().getContext().getTelephone() != null){
	        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasDevInfoCxtTelephone"), Bytes.toBytes(true));
	        					if (currPd.getDeviceInfo().getContext().getTelephone().getPhoneCarrier() != null){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtTelephonePhnCarrier"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getTelephone().getPhoneCarrier()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getTelephone().getPhoneRadio() != null){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtTelephonePhnRadio"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getTelephone().getPhoneRadio()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getTelephone().getInRoaming() != null){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtTelephoneInRoaming"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getTelephone().getInRoaming()));
	        					}
	        				}
	        				// device info context wifi
	        				if (currPd.getDeviceInfo().getContext().getWifi() != null){
	        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasDevInfoCxtWifi"), Bytes.toBytes(true));
	        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("DevInfoCxtWifi"), 
										Bytes.toBytes(currPd.getDeviceInfo().getContext().getWifi().toString()));
	        				}
	        				// device info context bluetoothInfo
	        				if (currPd.getDeviceInfo().getContext().getBluetoothInfo() != null){
	        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasDevInfoCxtBTInfo"), Bytes.toBytes(true));
	        					if ( currPd.getDeviceInfo().getContext().getBluetoothInfo().getBluetoothStatus() != null ){
		        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtBTInfoBTStatus"), 
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getBluetoothInfo().getBluetoothStatus()));
	        					}
	        				}
	        				// device info context availableMemoryInfo
	        				if (currPd.getDeviceInfo().getContext().getAvailableMemoryInfo() != null){
	        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasDevInfoCxtAvailbleMemryInfo"), Bytes.toBytes(true));
	        					if ( currPd.getDeviceInfo().getContext().getAvailableMemoryInfo().getAvailableRAM() != null ){
		        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtAvailbleMemryInfoAvailRAM"), 
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getAvailableMemoryInfo().getAvailableRAM()));
	        					}
	        					if ( currPd.getDeviceInfo().getContext().getAvailableMemoryInfo().getAvailableStorage() != null ){
		        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtAvailbleMemryInfoAvailStorage"), 
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getAvailableMemoryInfo().getAvailableStorage()));
	        					}
	        				}
	        				// device info context cpuInfo
	        				if (currPd.getDeviceInfo().getContext().getCpuInfo() != null){
	        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasDevInfoCxtCPUInfo"), Bytes.toBytes(true));
	        					if ( currPd.getDeviceInfo().getContext().getCpuInfo().getCpuTotal() != null ){
		        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtCPUInfoTotal"), 
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getCpuInfo().getCpuTotal()));
	        					}
	        					if ( currPd.getDeviceInfo().getContext().getCpuInfo().getCpuIdle() != null ){
		        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtCPUInfoIdle"), 
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getCpuInfo().getCpuIdle()));
	        					}
	        					if ( currPd.getDeviceInfo().getContext().getCpuInfo().getCpuUsage() != null ){
		        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtCPUInfoUsage"), 
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getCpuInfo().getCpuUsage()));
	        					}
	        				}
	        				// device info context USER_AGENT_ACTION
	        				if (currPd.getDeviceInfo().getContext().getUSERAGENTACTION() != null){
	        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasDevInfoCxtUsrAgntActn"), Bytes.toBytes(true));
	        					if ( currPd.getDeviceInfo().getContext().getUSERAGENTACTION().getUserAgent() != null ){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtUsrAgntActnUsrAgnt"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getUSERAGENTACTION().getUserAgent()));
	        					}
	        				}
	        			}
	        		}
	        	}
	        	
	        	p.add(Bytes.toBytes(raw_iot_tab_colfam),
						Bytes.toBytes("eventDataMap"), Bytes.toBytes(finEventDataMap.toString()));
	        	// return the put object
	        	return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(Bytes.toBytes(currIotRowKey)), p);
	}
	
	
	@SuppressWarnings("deprecation")
	public Tuple2<ImmutableBytesWritable, Put> cnvrtPushDevDatatoPut(String deviceId, String appSecret, String PushKey) {				
		// put the data
		String currRowKey = deviceId;
		//Put p = new Put(Bytes.toBytes(currRowKey));
		p = new Put(Bytes.toBytes(currRowKey));
		// add basic data to corresponding columns
		p.add(Bytes.toBytes(push_adid_dev_map_tab_colfam), Bytes.toBytes(appSecret + "__PUSH"), Bytes.toBytes(PushKey));
				
		// return the put object
	    return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(Bytes.toBytes(currRowKey)), p);
	}
	
	@SuppressWarnings("deprecation")
	public Tuple2<ImmutableBytesWritable, Put> cnvrtEventDBEventDatatoPut(String eventPrimary, String eventSecondary, String appSecret,
																			String dateStr, String countEvents) {	
		// put the data
		String currRowKey = appSecret + "__" + eventPrimary + "__" + dateStr;
		//Put p = new Put(Bytes.toBytes(currRowKey));
		p = new Put(Bytes.toBytes(currRowKey));
		// add basic data to corresponding columns
		p.add(Bytes.toBytes(eventDB_event_count_tab_colfam), Bytes.toBytes(eventSecondary), Bytes.toBytes(countEvents));
				
		// return the put object
	    return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(Bytes.toBytes(currRowKey)), p);
	}
	
	@SuppressWarnings("deprecation")
	public Tuple2<ImmutableBytesWritable, Put> cnvrtAdidDevDatatoPut(String deviceId, String appSecret, String adid, String idType) {
		// put the data
		String currRowKey = deviceId;
		//Put p = new Put(Bytes.toBytes(currRowKey));
		p = new Put(Bytes.toBytes(currRowKey));
		// add basic data to corresponding columns
		p.add(Bytes.toBytes(push_adid_dev_map_tab_colfam), Bytes.toBytes(appSecret + "__" + idType), Bytes.toBytes(adid));
				
		// return the put object
		return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(Bytes.toBytes(currRowKey)), p);
	}
	
	@SuppressWarnings("deprecation")
	public Tuple2<ImmutableBytesWritable, Put> cnvrtPushDatatoPut(String deviceId, String appSecret, String PushKey) {
		// put the data
		String currRowKey = appSecret + "__" + deviceId;
		//Put p = new Put(Bytes.toBytes(currRowKey));
		p = new Put(Bytes.toBytes(currRowKey));
		// add basic data to corresponding columns
		p.add(Bytes.toBytes(push_appsecret_dev_tab_colfam), Bytes.toBytes("push_key"), Bytes.toBytes(PushKey));
				
		// return the put object
	    return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(Bytes.toBytes(currRowKey)), p);
	}
	
	@SuppressWarnings("deprecation")
	public Tuple2<ImmutableBytesWritable, Put> activeUsersPreprocess(String rowkey, String appsecret, String deviceId, long timestamp, String date, 
			String year, String month, String day) {
		//put the data
		//Put p = new Put(Bytes.toBytes(rowkey));
		p = new Put(Bytes.toBytes(rowkey));
		//add active users preprocess data to hbase
		p.add(Bytes.toBytes(active_users_preprocess_colfam), Bytes.toBytes("app_secret"), Bytes.toBytes(appsecret));
		p.add(Bytes.toBytes(active_users_preprocess_colfam), Bytes.toBytes("device_id"), Bytes.toBytes(deviceId));
		p.add(Bytes.toBytes(active_users_preprocess_colfam), Bytes.toBytes("timestamp"), Bytes.toBytes(timestamp));
		p.add(Bytes.toBytes(active_users_preprocess_colfam), Bytes.toBytes("date"), Bytes.toBytes(date));
		p.add(Bytes.toBytes(active_users_preprocess_colfam), Bytes.toBytes("year"), Bytes.toBytes(year));
		p.add(Bytes.toBytes(active_users_preprocess_colfam), Bytes.toBytes("month"), Bytes.toBytes(month));
		p.add(Bytes.toBytes(active_users_preprocess_colfam), Bytes.toBytes("day"), Bytes.toBytes(day));

		//return the put object
		return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), p);
	}
	
	@SuppressWarnings("deprecation")
	public Tuple2<ImmutableBytesWritable, Put> newUsersPreprocess(String rowkey, String appsecret, String deviceId, long timestamp, String date, 
			String year, String month, String day) {
		//put the data
		//Put p = new Put(Bytes.toBytes(rowkey));
		p = new Put(Bytes.toBytes(rowkey));
		//add new users preprocess data to hbase
		p.add(Bytes.toBytes(new_users_preprocess_colfam), Bytes.toBytes("app_secret"), Bytes.toBytes(appsecret));
		p.add(Bytes.toBytes(new_users_preprocess_colfam), Bytes.toBytes("device_id"), Bytes.toBytes(deviceId));
		p.add(Bytes.toBytes(new_users_preprocess_colfam), Bytes.toBytes("timestamp"), Bytes.toBytes(timestamp));
		p.add(Bytes.toBytes(new_users_preprocess_colfam), Bytes.toBytes("date"), Bytes.toBytes(date));
		p.add(Bytes.toBytes(new_users_preprocess_colfam), Bytes.toBytes("year"), Bytes.toBytes(year));
		p.add(Bytes.toBytes(new_users_preprocess_colfam), Bytes.toBytes("month"), Bytes.toBytes(month));
		p.add(Bytes.toBytes(new_users_preprocess_colfam), Bytes.toBytes("day"), Bytes.toBytes(day));
			
		//return the put object
		return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), p);
	}
	
	@SuppressWarnings("deprecation")
	public Tuple2<ImmutableBytesWritable, Put> getActiveUsers(String rowKey, String appSecret, long activeUsers) {
		//put the data
		//Put p = new Put(Bytes.toBytes(rowKey));
		p = new Put(Bytes.toBytes(rowKey));
		//add active users data to hbase
		p.add(Bytes.toBytes(active_users_tab_colfam), Bytes.toBytes("app_secret"), Bytes.toBytes(appSecret));
		p.add(Bytes.toBytes(active_users_tab_colfam), Bytes.toBytes("active_users"), Bytes.toBytes(activeUsers));
			
		//return put object
		return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(Bytes.toBytes(rowKey)), p);
	}
	
	@SuppressWarnings("deprecation")
	public Tuple2<ImmutableBytesWritable, Put> getNewUsers(String rowKey, String appSecret, long newUsers) {
		//put the data
		//Put p = new Put(Bytes.toBytes(rowKey));
		p = new Put(Bytes.toBytes(rowKey));
		//add new users data to hbase
		p.add(Bytes.toBytes(new_users_tab_colfam), Bytes.toBytes("app_secret"), Bytes.toBytes(appSecret));
		p.add(Bytes.toBytes(new_users_tab_colfam), Bytes.toBytes("new_users"), Bytes.toBytes(newUsers));
			
		//return put object
		return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(Bytes.toBytes(rowKey)), p);
	}
	
	@SuppressWarnings("deprecation")
	public Tuple2<ImmutableBytesWritable, Put> getCumulativeEventsCount(String rowkey, String appsecret, String eventname, Long eventcount) {
		//put the data
		//Put p = new Put(Bytes.toBytes(rowkey));
		p = new Put(Bytes.toBytes(rowkey));
		//add data for cumulative event counts to hbase
		p.add(Bytes.toBytes(hbaseTableEvents2_colfam), Bytes.toBytes("app_secret"), Bytes.toBytes(appsecret));
		p.add(Bytes.toBytes(hbaseTableEvents2_colfam), Bytes.toBytes(eventname), Bytes.toBytes(eventcount));
			
		//return object
		return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), p);
	}
	
	//event wise active users data dump to hbase
	@SuppressWarnings("deprecation")
	public Tuple2<ImmutableBytesWritable, Put> doEventwiseSegmentationAU(String rowkey, String appsecret, String eventname, long userCount) {
		//HTable hbasetabName = new HTable(hbaseIotConf, hbaseTableEventsAU);
		//put the data
		//Put p = new Put(Bytes.toBytes(rowkey));
		p = new Put(Bytes.toBytes(rowkey));
		p.add(Bytes.toBytes(hbaseTableEventsAU_colfam), Bytes.toBytes("app_secret"), Bytes.toBytes(appsecret));
		logger.info("AU eventwise segmentation with eventname ---> " + eventname + " and eventcount as ---> " + userCount);
		p.add(Bytes.toBytes(hbaseTableEventsAU_colfam), Bytes.toBytes(eventname), Bytes.toBytes(userCount));
				
		//return put object
		return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), p);
	}
	
	//event wise new users data dump to hbase
	@SuppressWarnings("deprecation")
	public Tuple2<ImmutableBytesWritable, Put> doEventwiseSegmentationNU(String rowkey, String appsecret, String eventname, long userCount) {
		//HTable hbasetabName = new HTable(hbaseIotConf, hbaseTableEventsNU);
		//put the data
		//Put p = new Put(Bytes.toBytes(rowkey));
		p = new Put(Bytes.toBytes(rowkey));
		p.add(Bytes.toBytes(hbaseTableEventsNU_colfam), Bytes.toBytes("app_secret"), Bytes.toBytes(appsecret));
		logger.info("AU eventwise segmentation with eventname ---> " + eventname + " and eventcount as ---> " + userCount);
		p.add(Bytes.toBytes(hbaseTableEventsNU_colfam), Bytes.toBytes(eventname), Bytes.toBytes(userCount));
					
		//return put object
		return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), p);
				
		// if something fails!!
		//return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(Bytes.toBytes("PUT-FAILED")), new Put(Bytes.toBytes("PUT-FAILED")));
		}
	
}