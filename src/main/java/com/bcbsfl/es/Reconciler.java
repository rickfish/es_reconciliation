package com.bcbsfl.es;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.bcbsfl.mail.EmailSender;
import com.fasterxml.jackson.core.JsonProcessingException;

 abstract public class Reconciler<T> extends ReconcileApp {
	public static final String ACTIVE_INDEX_SUFFIX = "active";
	public static final String INVALID_SECROLE_INDEX_SUFFIX = "invalid_secrole";
    protected static final String GEN_SECROLE = "GEN";
    
    static protected List<String> validSecRoles = new ArrayList<String>();
    static {
    	validSecRoles.add(GEN_SECROLE);
    	validSecRoles.add("FEP");
    	validSecRoles.add("BCH");
    	validSecRoles.add("BLU");
    	validSecRoles.add("EMP");
    	validSecRoles.add("SAO");
    	validSecRoles.add("SEN");
    }

    private List<String> idsToReconcile = new ArrayList<String>();
    protected EmailSender emailSender = new EmailSender();
    protected boolean updateElasticSearch = true;
    protected boolean updateScheduledTasks = true;
    File[] filesToReconcile = null;
	public Reconciler(String doctype) {
		super(doctype);
		this.updateElasticSearch = Utils.getBooleanProperty("update.elastic.search");
		this.updateScheduledTasks = Utils.getBooleanProperty("update.scheduled.tasks");
		try {
	        File dir = new File(this.outputDirectory);
	        String fileFilterSpec = this.env + "_unmatched" + 
	        	this.doctype.substring(0,1).toUpperCase() + doctype.substring(1) + "s_*.txt";
	        System.out.println("File spec for finding files: " + fileFilterSpec);
	        FileFilter fileFilter = new WildcardFileFilter(fileFilterSpec);
	        this.filesToReconcile = dir.listFiles(fileFilter);
	        BufferedReader reader = null;
	        for (File file : this.filesToReconcile) {
		        try {
		        	reader = new BufferedReader(new FileReader(file));
		        	long records = 0;
		        	String line = reader.readLine();
		        	while(line != null) {
		        		records++;
		        		idsToReconcile.add(line);
		        		line = reader.readLine();
		        	}
		        	System.out.println("file '" + file.getName() + "' has " + records + " records");
		        } catch (IOException ex) {
		            throw ex;
		        } finally {
		        	reader.close();
		        }
	        }
	
	        System.out.println("[" + new Date().toString() + "] Processing " + this.filesToReconcile.length + " files with a total of " + idsToReconcile.size() + " records");
	        this.indexName = "conductor_" + Utils.getProperty("env") + "_" + doctype;
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	abstract protected T getObjectFromJson(String json) throws Exception;
	abstract protected String getStatus(T o);
	abstract protected String getId(T o);
	abstract protected long getUpdateTime(T o);
	abstract protected String getElasticIdAttribute(T o);
	abstract protected void updateObjectInES(T o) throws Exception;
	
	protected void reconcile() throws Exception {
		createDataSource();
		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			con = getDatabaseConnection();
			if(con == null) {
				throw new Exception("Could not get database connection");
			}
			con.setAutoCommit(false);
			st = con.prepareStatement("SELECT * FROM " + doctype + " where " + doctype + "_id = ?");
			String json = null;
			T objectToProcess = null;
			String status = null;
			String objectId = null;
			String id = null;
			String esStatus = null;
			String msg = null;
			int firstPipeLoc = -1, secondPipeLoc = -1;
			int count = 0;
			Map<String, String> nonMatches = new HashMap<String, String>();
			for(String line : idsToReconcile) {
				firstPipeLoc = line.indexOf("|");
				id = line.substring(0, firstPipeLoc);
				secondPipeLoc = line.indexOf("|", firstPipeLoc + 1);
				esStatus = line.substring(firstPipeLoc + 1, secondPipeLoc);
				msg = line.substring(secondPipeLoc + 1);
				st.setString(1, id);
				if(rs != null) {
					rs.close();
				}
				rs = st.executeQuery();
				if(rs.next()) {
					json = rs.getString("json_data");
					try {
						objectToProcess = getObjectFromJson(json);
						status = getStatus(objectToProcess);
						objectId = getId(objectToProcess);
						if(esStatus != null && esStatus.equals(status)) {
							System.out.println("Record number: " + (++count) + " - " + doctype + " Id: " + objectId + ", status: " + status + ", updateTime: " + new Date(getUpdateTime(objectToProcess)).toString() + ", nothing to do...database must have caught up with ES.");
						} else {
							System.out.print("Record number: " + (++count) + " - " + doctype + " Id: " + objectId + ", status: " + status + ", updateTime: " + new Date(getUpdateTime(objectToProcess)).toString());
							nonMatches.put(id, msg);
							if(this.updateElasticSearch) {
								if(esStatus.equals("SCHEDULED")) {
									if(this.updateScheduledTasks) {
										updateObjectInES(objectToProcess);
									} else {
								        System.out.println(", NOT updated because update.scheduled.tasks is false.");
									}
								} else {
									updateObjectInES(objectToProcess);
								}
							} else {
						        System.out.println(", NOT updated because update.elastic.search property is false.");
							}
						}
					} catch(Exception e) {
						System.err.println("Exception mapping json for " + this.doctype + " " + json + " " + e.getMessage());
					}
				} else {
					System.err.println("******** Could not find " + this.doctype + " '" + objectId + "' in the database!!");
				}
			}
	
			if(nonMatches.size() > 0) {
				long recCount = TASK_DOC_TYPE.equals(this.doctype) ? TaskComparator.recCount : WorkflowComparator.recCount;
				this.emailSender.sendNonMatchesEmail(recCount, this.doctype, nonMatches);
			}
			
			renameFiles();
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
		closeDataSource();
		System.out.println("[" + new Date().toString() + "] Completed");
	}
	
	protected void indexObject(final String index, final String docType, final String docId, final Object doc) {
	    byte[] docBytes;
	    try {
	        docBytes = objectMapper.writeValueAsBytes(doc);
	    } catch (JsonProcessingException e) {
	        System.err.println("Failed to convert " + docType + "'" + docId + "'  to byte string");
	        return;
	    }
	
	    IndexRequest request = new IndexRequest(index);
	    request.id(docId);
	    request.source(docBytes, XContentType.JSON);
	
	    try {
	        elasticSearchClient.index(request, RequestOptions.DEFAULT);
	        System.out.println(", updated");
	    } catch (Exception e) {
	        e.printStackTrace();
	    }
	}

	protected void removeObjectFromIndices(String id, String elasticIdAttribute, String dontRemoveFromThisIndex) {
		try {
	    	BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
	    	QueryBuilder qb = QueryBuilders.termQuery(elasticIdAttribute, id);
	    	queryBuilder.must(qb);
			SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
	        searchSourceBuilder.query(queryBuilder);
	        searchSourceBuilder.size(20);
	
	        // Generate the actual request to send to ES.
	        SearchRequest searchRequest = new SearchRequest(indexName);
	        searchRequest.source(searchSourceBuilder);
	        SearchResponse response = elasticSearchClient.search(searchRequest, RequestOptions.DEFAULT);
	        SearchHits hits = response.getHits();
	        if(hits.getHits().length > 0) {
	        	for(SearchHit hit : hits.getHits()) {
	        		if(!hit.getIndex().equals(dontRemoveFromThisIndex)) {
	        			removeObjectFromIndex(hit.getIndex(), id);
	        		}
	        	}
	        }
		} catch(Exception e) {
			
		}
	}
	
    protected void removeObjectFromIndex(final String index, String docId) {
		ActionListener<DeleteResponse> listener = new ActionListener<DeleteResponse>() {
		    @Override
		    public void onResponse(DeleteResponse deleteResponse) {
		    }

		    @Override
		    public void onFailure(Exception e) {
	        	System.err.println("Failed to remove workflow '" + docId + "' from index '" + index + "'");
		    }
		};
        try {
    		DeleteRequest request = new DeleteRequest(index, docId);
    		elasticSearchClient.deleteAsync(request, RequestOptions.DEFAULT, listener);    		
        } catch (Exception e) {
        	System.err.println("Failed to remove workflow '" + docId + "' from index '" + index + "'");
            e.printStackTrace();
        }
	}

    private void renameFiles() {
		File renamedFile = null;
		String renamedFileName = null;
	    for (File file : this.filesToReconcile) {
	        try {
	        	renamedFileName = this.outputDirectory + "/updated_" + file.getName();
	        	renamedFile = new File(renamedFileName);
	        	if(!file.renameTo(renamedFile)) {
	        		renamedFile.delete();
	        		file.renameTo(renamedFile);
	        	}
	        } catch (Exception e) {
	        	e.printStackTrace();
	        }
	    }
	}

	protected String getCompleteIndexName(String secRole, boolean useActiveIndex) {
		String lastIndexSegment = null;
		if(useActiveIndex) {
			lastIndexSegment = ACTIVE_INDEX_SUFFIX;
		}
		String completeIndexName = this.indexName;
		if(secRole == null) {
			completeIndexName += "_" + INVALID_SECROLE_INDEX_SUFFIX;
			if(useActiveIndex) {
				completeIndexName += "_" + ACTIVE_INDEX_SUFFIX;
			}
		} else {
			secRole = secRole.toUpperCase();
			if(validSecRoles.contains(secRole)) {
				completeIndexName += "_" + secRole.toLowerCase();
				if(GEN_SECROLE.equals(secRole)) {
					if(lastIndexSegment == null) {
						lastIndexSegment = SIMPLE_MONTHLY_DATE_FORMAT.format(new Date());
					}
					completeIndexName += "_" + lastIndexSegment;
				} else {
					if(lastIndexSegment == null) {
						lastIndexSegment = SIMPLE_YEARLY_DATE_FORMAT.format(new Date());
					}
					if(useActiveIndex) {
						completeIndexName += "_" + lastIndexSegment;
					} else {
						completeIndexName += "_" + lastIndexSegment.substring(0, 4);
					}
				}
			} else {
				completeIndexName += "_" + INVALID_SECROLE_INDEX_SUFFIX;
				if(useActiveIndex) {
					completeIndexName += "_" + ACTIVE_INDEX_SUFFIX;
				}
			}
		}
		return completeIndexName;
	}
}