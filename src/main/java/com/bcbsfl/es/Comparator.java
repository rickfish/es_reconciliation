package com.bcbsfl.es;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.bcbsfl.mail.EmailSender;
import com.fasterxml.jackson.databind.ObjectMapper;

abstract public class Comparator<T> extends ReconcileApp {
	private PrintWriter unmatchedObjectsWriter = null;
	private PrintWriter unmatchedObjectsWithReasonWriter = null;
    private Map<String, String> nonMatches = new HashMap<String, String>();
    private boolean logEachRecord = Utils.getBooleanProperty("log.each.record");
    private boolean insertIfNotInES = Utils.getBooleanProperty("insert.if.not.in.es");
    
    protected EmailSender emailSender = new EmailSender();

	public Comparator(String doctype) {
		super(doctype);
        this.indexName = "conductor_" + Utils.getProperty("env") + "_" + doctype;
	}

	abstract protected T getObjectFromJson(String json) throws Exception;
	abstract protected String getStatus(T o);
	abstract protected String getId(T o);
	abstract protected String getName(T o);
	abstract protected long getUpdateTime(T o);
	abstract protected String getElasticIdAttribute(T o);
	abstract protected void setRecCount(long recCount);
	abstract protected long getRecCount();

	public void compare() throws Exception {
		createDataSource();
        this.unmatchedObjectsWriter = createPrintWriter(this.outputDirectory + "/" + this.env + "_unmatched" + 
        	this.doctype.substring(0,1).toUpperCase() + doctype.substring(1) + "s_" + getFileSuffix() + ".txt");
		this.unmatchedObjectsWithReasonWriter = createPrintWriter(this.outputDirectory + "/" + this.env + "_unmatched" + 
        	this.doctype.substring(0,1).toUpperCase() + doctype.substring(1) + "sWithReason_" + getFileSuffix() + ".txt");
		System.out.println("[" + new Date().toString() + "] Getting total records to process...");
		setRecCount(getRecordCount());
		String endTimeframe = Utils.getProperty("db.endTimeframe");
		System.out.println("[" + new Date().toString() + "] There are " + (endTimeframe == null ? "approximately " : "") + getRecCount() + " " + this.doctype + "s to process"); 
		if(endTimeframe == null) {
			System.out.println("NOTE THAT this count is approximate because no endTimeframe was specified");
		}
		int offset = this.rowOffset;
		Connection con = null;
		Statement st = null;
		ResultSet rs = null;
		List<ObjectInfo> objectsToProcess = null;
		int count = 0;
		while(true) {
			objectsToProcess = new ArrayList<ObjectInfo>();
			try {
				con = getDatabaseConnection();
				if(con == null) {
					throw new Exception("Could not get database connection");
				}
				con.setAutoCommit(false);
				st = con.createStatement();

				// Turn use of the cursor on.
				st.setFetchSize(50);
				String query = "SELECT json_data FROM " + this.doctype + " where modified_on " + getDatePredicate();
				if(this.rowLimit > 0) {
					query += (" order by modified_on limit " + rowLimit);
					if(offset > 0) {
						query += " offset " + offset;
					}
				}
				System.out.println("Database query: " + query);
				rs = st.executeQuery(query);
				String json = null;
				int recsAdded = 0;
				while(rs.next()) {
					ObjectInfo objectInfo = new ObjectInfo();
					json = rs.getString("json_data");
					if(json != null) {
						objectInfo.objectToProcess = getObjectFromJson(json);
						objectInfo.status = getStatus(objectInfo.objectToProcess);
						objectInfo.objectId = getId(objectInfo.objectToProcess);
						objectsToProcess.add(objectInfo);
					}
					if(++recsAdded  % 1000 == 0) {
						System.out.println("So far added " + recsAdded + " to the list of objects to process");
					}
				}
				if(recsAdded == 0) {
					System.out.println("DONE");
					break;
				}
				// Turn the cursor off.
				st.setFetchSize(0);
			} catch(Exception e) {
				e.printStackTrace();
			} finally {
				if(rs != null) {
					try {
						rs.close();
					} catch(Exception e) {
						e.printStackTrace();
					}
				}
				if(st != null) {
					try {
						st.close();
					} catch(Exception e) {
						e.printStackTrace();
					}
				}
				if(con != null) {
					try {
						con.close();
					} catch(Exception e) {
						e.printStackTrace();
					}
				}
			}
			int objectCount = objectsToProcess.size();
			System.out.println("[" + new Date().toString() + "] Found " + objectCount + " objects to process");
			if(objectCount > 0) {
				for(ObjectInfo objectInfo : objectsToProcess) {
					try {
						count++;
						if(this.logEachRecord) {
							System.out.print("Record number " + count + " - " + doctype + " Id: " + objectInfo.objectId + ", status: " + objectInfo.status + ", updateTime: " + new Date(getUpdateTime(objectInfo.objectToProcess)).toString());
						}
						this.compareToElasticSearch(objectInfo.objectToProcess, objectInfo.objectId, objectMapper);
						if(count % 1000 == 0) {
							if(!this.logEachRecord) {
								double percentage = (double)((double)count * 100 / (double)getRecCount());
								System.out.println("[" + new Date().toString() + "] So far processed " + count + " " + this.doctype + "s out of " + getRecCount() + " total (" + (Math.round(percentage * 10) / 10.0) + "%) and found " + this.nonMatches.size() + " nonMatches");
							}
						}
					} catch(Exception e) {
						if(this.logEachRecord) {
							System.out.println("");
						}
						String smallErrorMsg = (e.getMessage().length() > 50 ? e.getMessage().substring(0, 50) : e.getMessage());
						this.emailSender.sendExceptionEmail(smallErrorMsg, e);
						System.err.println("Record number " + count + ": exception mapping json for " + this.doctype + ": " + smallErrorMsg);
					}
				}
			}
			if(this.rowLimit > 0 && (objectCount == this.rowLimit) && (count < getRecCount())) {
				offset += this.rowLimit;
			} else {
				break;
			}
		}
		closeDataSource();
		System.out.println("[" + new Date().toString() + "] Completed processing " + count + " records and found " + this.nonMatches.size() + " nonMatches");
		System.out.println("**************************** NonMatches *****************************");
		System.out.println("Total nonmatches: " + this.nonMatches.size());
		for(String id: this.nonMatches.keySet()) {
			System.out.println(this.doctype + "Id: " + id + ", msg: " + this.nonMatches.get(id));
		}
		System.out.println("**************************** NonMatches *****************************");
		this.unmatchedObjectsWithReasonWriter.close();
		this.unmatchedObjectsWriter.close();
	}

	private PrintWriter createPrintWriter(String filename) throws Exception {
		PrintWriter pw = null;
		try {
			pw = new PrintWriter(filename);
		} catch(FileNotFoundException e) {
			e.printStackTrace();
			System.out.println("Got the above exception. Removing the '" + filename + "'' file and trying again.");
			new File(filename).delete();
			pw = new PrintWriter(filename);
		}
   		return pw;
	}
	
	public long getRecordCount() throws Exception {
		Connection con = null;
		Statement st = null;
		ResultSet rs = null;
		long count = 0;
		try {
			con = getDatabaseConnection();
			if(con == null) {
				throw new Exception("Could not get database connection");
			}
			con.setAutoCommit(false);
			st = con.createStatement();
			String query = "SELECT count(*) AS recCount FROM " + this.doctype + " where modified_on " + getDatePredicate();
			System.out.println("Database query for count(*): " + query);
			rs = st.executeQuery(query);
			rs.next();
			count = rs.getLong("recCount");		
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			if(rs != null) {
				try {
					rs.close();
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
			if(st != null) {
				try {
					st.close();
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
			if(con != null) {
				try {
					con.close();
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
		}
		return count;
	}

	private void compareToElasticSearch(T objectToProcess, String id, ObjectMapper objectMapper) throws Exception {
    	String databaseStatus = getStatus(objectToProcess);
    	BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
    	QueryBuilder qb = QueryBuilders.termQuery(getElasticIdAttribute(objectToProcess), id);
    	queryBuilder.must(qb);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(queryBuilder);
        searchSourceBuilder.size(20);

        // Generate the actual request to send to ES.
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(searchSourceBuilder);
        SearchResponse response = this.elasticSearchClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        Map<String, Object> latestSourceAsMap = null;
        @SuppressWarnings("unused")
		String latestSourceAsString = null;
    	long latestTimestamp = 0;
        if(hits.getHits().length == 1) {
        	latestSourceAsMap = hits.getHits()[0].getSourceAsMap();
        	latestSourceAsString = hits.getHits()[0].getSourceAsString();
        } else {
            for(SearchHit hit : hits.getHits()) {
	            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
	            Object o = sourceAsMap.get("@timestamp");
	            if(o != null) {
	            	try {
            			Date date = ES_TIMESTAMP_FORMAT.parse((String)o);
            			if(date.getTime() > latestTimestamp) {
            				latestTimestamp = date.getTime();
            				latestSourceAsMap = sourceAsMap;
            	        	latestSourceAsString = hit.getSourceAsString();
            			}
            		} catch(Exception e) {
            		}
	            }
        	}
            if(latestSourceAsMap == null) {
                for(SearchHit hit : hits.getHits()) {
    	            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
    	            Object o = sourceAsMap.get("updateTime");
    	            if(o != null) {
    	            	try {
                			Date date = ES_TIMESTAMP_FORMAT.parse((String)o);
                			if(date.getTime() > latestTimestamp) {
                				latestTimestamp = date.getTime();
                				latestSourceAsMap = sourceAsMap;
                	        	latestSourceAsString = hit.getSourceAsString();
                			}
                		} catch(Exception e) {
                		}
    	            }
            	}
            }
            if(latestSourceAsMap == null) {
                for(SearchHit hit : hits.getHits()) {
    	            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
    	            Object o = sourceAsMap.get("startTime");
    	            if(o != null) {
    	            	try {
                			Date date = ES_TIMESTAMP_FORMAT.parse((String)o);
                			if(date.getTime() > latestTimestamp) {
                				latestTimestamp = date.getTime();
                				latestSourceAsMap = sourceAsMap;
                	        	latestSourceAsString = hit.getSourceAsString();
                			}
                		} catch(Exception e) {
                		}
    	            }
            	}
            }
        }
        if(latestSourceAsMap != null) {
			if(this.logEachRecord) {
				System.out.print(", found Elastic Search document");
			}
        	Object esObjectStatus = latestSourceAsMap.get("status");
        	if(esObjectStatus != null) {
            	String esStatus = esObjectStatus.toString();
         		if(esStatus.equals(databaseStatus)) {
					if(this.logEachRecord) {
						System.out.println(" - statuses are the same");
					}
        		} else {
        			String msg = "For '" + getName(objectToProcess) + "' " + this.doctype + "Type, DatabaseStatus (" + databaseStatus + ") is not the same as the ES Status(" + esStatus + ")";
        			this.unmatchedObjectsWriter.append(id + "|" + esStatus + "|" + msg + "\n");
        			this.unmatchedObjectsWriter.flush();
        			this.unmatchedObjectsWithReasonWriter.append(id + ", " + msg +"\n");
        			this.unmatchedObjectsWithReasonWriter.flush();
					if(this.logEachRecord) {
						System.out.println(msg);
					}
        			this.nonMatches.put(id, msg);
        		}
        	}
        } else {
        	if(this.insertIfNotInES) {
            	String addlMsg = hits.getHits().length == 0 ? "NO ELASTIC SEARCH DOCUMENT" : "NO ELASTIC SEARCH DOCUMENT out of " + hits.getHits().length + " documents found";
    			String msg = "For '" + getName(objectToProcess) + "' " + this.doctype + "Type, DatabaseStatus (" + databaseStatus + "), " + addlMsg + ", this will be inserted into ElasticSearch";
    			this.unmatchedObjectsWriter.append(id + "|NO_ES_DOC|" + msg + "\n");
    			this.unmatchedObjectsWriter.flush();
    			this.unmatchedObjectsWithReasonWriter.append(id + ", " + msg +"\n");
    			this.unmatchedObjectsWithReasonWriter.flush();
    			if(this.logEachRecord) {
    				System.out.println(msg);
    			}
    			this.nonMatches.put(id, msg);
        	} else {
    			if(this.logEachRecord) {
    	            if(hits.getHits().length == 0) {
    	            	System.out.println(", NO ELASTIC SEARCH DOCUMENT");
    	            } else {
    	            	System.out.println(", NO ELASTIC SEARCH DOCUMENT out of " + hits.getHits().length + " documents found");
    	            }
    			}
        	}
    	}
	}
	
	private String getDatePredicate() {
		StringBuffer sb = new StringBuffer();
		String start = Utils.getProperty("db.startTimeframe");
		String end = Utils.getProperty("db.endTimeframe");
		if(StringUtils.isNotBlank(start)) {
			if(StringUtils.isNotBlank(end)) {
				sb.append("BETWEEN '");
				sb.append(start);
				sb.append("' AND '");
				sb.append(end);
				sb.append("'");
			} else {
				sb.append("> '");
				sb.append(start);
				sb.append("'");
			}
		} else if(StringUtils.isNotBlank(end)) {
			sb.append("< '");
			sb.append(end);
			sb.append("'");
		}
		return sb.toString();
	}
	private class ObjectInfo {
		public T objectToProcess = null;
		public String status = null;
		public String objectId = null;
	}
}