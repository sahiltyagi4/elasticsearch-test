package com.iot.data.esqueries;
//com.iot.data.esqueries.ElasticUtils

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;


public class ElasticUtils {

	private static TransportClient client;
	private static AdminClient adminClient;
	private static IndicesAdminClient indicesAdminClient;
	private static String preProcessAUIndex;
	private static String preProcessNUIndex;

	public ElasticUtils() throws IOException {

		client = TransportClient.builder().build()
				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("138.201.83.148"), 9300));

		adminClient = client.admin();
		indicesAdminClient = client.admin().indices();
		preProcessAUIndex = "activeusers";
		preProcessNUIndex = "newusers";
		System.out.println("END OF CONST.");
	}

	public static void createAUIndex() throws Exception {
		
		/**indicesAdminClient.prepareCreate(preProcessAUIndex)   
        .addMapping("activeusers", "{\n" +                
                "    \"activeusers\": {\n" +
                "      \"properties\": {\n" +
                "        \"appSecret\": {\n" +
                "          \"type\": \"string\"\n" +
                "        },\n" +
                "        \"timestamp\": {\n" +
                "          \"type\": \"date\"\n" +
                "        },\n" +
                "        \"date\": {\n" +
                "          \"type\": \"date\" , \"format\": \"YYYY-MM-dd HH:mm:ss\"\n" +
                "        },\n" +
                "        \"deviceId\": {\n" +
                "          \"type\": \"string\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }")
        .get();**/
		
		indicesAdminClient.prepareCreate(preProcessAUIndex)   
        .addMapping("activeusers", "{\n" +                
                "    \"activeusers\": {\n" +
                "      \"properties\": {\n" +
                "        \"app_secret\": {\n" +
                "          \"type\": \"string\"\n" +
                "        },\n" +
                "        \"timestamp\": {\n" +
                "          \"type\": \"date\"\n" +
                "        },\n" +
                "        \"date\": {\n" +
                "          \"type\": \"date\" , \"format\": \"YYYY-MM-dd HH:mm:ss\"\n" +
                "        },\n" +
                "        \"device_id\": {\n" +
                "          \"type\": \"string\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }")
        .get();
		
	}

	public static void createNUIndex() throws Exception {

		indicesAdminClient.prepareCreate(preProcessNUIndex)   
		.addMapping("newusers", "{\n" +                
				"    \"newusers\": {\n" +
				"      \"properties\": {\n" +
				"        \"appSecret\": {\n" +
				"          \"type\": \"string\"\n" +
				"        },\n" +
				"        \"timestamp\": {\n" +
				"          \"type\": \"date\"\n" +
				"        },\n" +
				"        \"date\": {\n" +
				"          \"type\": \"date\" , \"format\": \"YYYY-MM-dd HH:mm:ss\"\n" +
				"        },\n" +
				"        \"deviceId\": {\n" +
				"          \"type\": \"string\"\n" +
				"        }\n" +
				"      }\n" +
				"    }\n" +
				"  }")
		.get();

	}

	public static void getAllAUData() throws Exception {

		SearchResponse sr = client.prepareSearch(preProcessAUIndex).execute().actionGet();

		for (SearchHit hit : sr.getHits()) {
			String deviceId = hit.getSource().get("iotRawDataDeviceId").toString();
			String appSecret = hit.getSource().get("iotRawDataAppSecret").toString();
			String date = hit.getSource().get("date").toString();
			System.out.println("deviceId, appSecret, date-->" + deviceId + ", " + appSecret + ", " + date);
		}
	}

	@SuppressWarnings("deprecation")
	public static void getDateRangeAggregates() throws Exception {
		//		SearchResponse response = client.prepareSearch(preProcessAUIndex)
		//				.addAggregation(
		//						AggregationBuilders
		//		                .dateRange("agg")
		//		                .field("date")
		//		                .format("yyyy-MM-dd")
		//		                .addRange("2016-11-05", "2016-11-11")
		//						)
		//		        .setTypes("activeUsers")
		//		        .setQuery(QueryBuilders.termQuery("appSecret", "app1"))                 // Query
		//		        .setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18))     // Filter
		//		        .setFrom(0).setSize(60).setExplain(true)
		//		        .execute()
		//		        .actionGet();

		//		System.out.println("/n/n/n/n/n/n"+response.getHits().toString()+"/n/n/n/n/n");

		//		SearchResponse response = client.prepareSearch(preProcessAUIndex)
		//			    .addAggregation(
		//			    		AggregationBuilders
		//		                .dateRange("agg")
		//		                .field("date")
		//		                .format("yyyy-MM-dd")
		//		                .addRange("2016-08-24", "2016-10-26")
		//			        ).execute().actionGet();
		//		
		//		SearchResponse resp = client.prepareSearch(preProcessAUIndex)
		//			    .addAggregation(
		//			    		AggregationBuilders.range("agg")
		//			    		.field("timestamp")
		//			    		.addRange(1472083199, 1472169599)
		//			    		.subAggregation(AggregationBuilders
		//			    				.terms("iotRawDataAppSecret")))
		//			    .execute().actionGet();

		SearchRequestBuilder searchReq = client.prepareSearch(preProcessAUIndex);
		searchReq.setTypes("activeUsers");
		TermsBuilder termsB = AggregationBuilders.terms("AppSecret").field("iotRawDataAppSecret");
		RangeQueryBuilder rangeFB = QueryBuilders.rangeQuery("timestamp").from("1478304000").to("1479427200");
		searchReq.setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), rangeFB)).addAggregation(termsB);
		SearchResponse searchRes = searchReq.execute().actionGet();

		Terms fieldATerms = searchRes.getAggregations().get("AppSecret");
		for (Terms.Bucket filedABucket : fieldATerms.getBuckets()) {
			//fieldA
			String fieldAValue = filedABucket.getKey().toString();

			//COUNT(fieldA)
			long fieldACount = filedABucket.getDocCount();
			System.out.println("fieldAValue : " + fieldAValue + "  " +"fieldACount : " + fieldACount + "\n\n\n\n\n");
		}

		//		Terms  terms = searchRes.getAggregations().get("agg");
		//		Collection<Terms.Bucket> buckets = terms.getBuckets();
		//		for (Bucket bucket : buckets) {
		//		    System.out.println(bucket.getKeyAsString() +" ("+bucket.getDocCount()+")");
		//		}

		//FRESH TRY FOR QUERYING DATA - Works !!

		//		 SearchResponse response = 
		//		            client.prepareSearch(preProcessAUIndex).setTypes("activeUsers").execute().actionGet();
		//		        SearchHit[] hits = searchResponse.getHits().getHits();
		//		        	System.out.println(hits.length);
		//		            Map<String, Object> attributes = new HashMap<String, Object>();
		//		            attributes.put("results", hits);
		//		            

		//		Terms fieldATerms = searchRes.getAggregations().get("agg");
		//		for (Terms.Bucket filedABucket : fieldATerms.getBuckets()) {
		//			//fieldA
		//			String fieldAValue = filedABucket.getKey().toString();
		//			
		//			//COUNT(fieldA)
		//		long fieldACount = filedABucket.getDocCount();
		//		System.out.println("fieldAValue : " + fieldAValue + "  " +"fieldACount : " + fieldACount);
		//		}

		//		Range agg = searchRes.getAggregations().get("agg");
		//		// For each entry
		//		for (Range.Bucket entry : agg.getBuckets()) {
		//		    String key = entry.getKeyAsString();       
		//		    System.out.println("key, doc count-->" + key + ", " + entry.getDocCount() );
		//		}

	}
	
	public static void runDateRangeQry() {
		SearchRequestBuilder srchBuildr = client.prepareSearch("activeusers").setTypes("testing").setQuery(QueryBuilders.filteredQuery(
				QueryBuilders.matchAllQuery(), QueryBuilders.rangeQuery("timestamp").from(1472083199).to(1472169599)))
				.addAggregation(AggregationBuilders.terms("AppSecret").field("iotRawDataAppSecret"));
		
		
	}

	public static void main(String[] args) throws Exception {
		ElasticUtils esObj=new ElasticUtils();
		//    	  esObj.createAUIndex();
		//    	  esObj.createNUIndex();
		//    	  esObj.getAllAUData();
		esObj.getDateRangeAggregates();
		// close the client connection.
		esObj.client.close();
	}

}
