package com.bcbsfl.es;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class FBElasticSearchDocTagger {
	/**
	 * The list of Florida Blue attribute names that will be moved from inputData, outputData to a 'tags'
	 * section of the document sent to ElasticSearch
	 */
	private List<String> fbTags = null;
			
	public FBElasticSearchDocTagger() {
		loadFBTags();
	}

    /**
     * Load the fb_attrs.json file which contains Florida Blue attribute names
     * that will be pulled out of the inputData and outputData sections of the
     * conductor JSONs and placed in a root-level 'tags' section.
     */
    private void loadFBTags() {
    	if(this.fbTags == null) {
    		try {
	    		InputStream fbTagsStream = this.getClass().getResourceAsStream("/fb_tags.json");
	    		if(fbTagsStream != null) {
	    			this.fbTags = new ArrayList<String>();
		    		ObjectMapper mapper = new ObjectMapper();
		    		JsonNode tree = mapper.readTree(fbTagsStream);
		    		/*
		    		 * There are one or more nodes in the JSON, get all of the string arrays 
		    		 * in each node and load them all into the tags we look for. The JSON looks
		    		 * something like this:
		    		 * 
					 * {
					 *     	"enterprise" : [
					 * 			"memberId",
					 * 			"providerId"
					 * 		],
					 * 		"domain" : [
					 * 			"claimId"
					 * 		]
					 * }
					 * 
					 * The 'enterprise' node contains all the enterprise-wide attributes, all other
					 * nodes are domain specific.
					 */
		    		tree.forEach(node -> {
			    		node.forEach(attr -> {
			    			this.fbTags.add(attr.asText());
			    		});
		    		});
	    		}
    		} catch(Throwable t) {
    			fbTags = null;
    			System.err.println("Failed to load fbAttrs.json");
    			t.printStackTrace();
    		}
    	}
    }
    
    public void populateFBTags(Map<String, Object> tags, Map<String, Object> input, Map<String, Object> output) {
		if(this.fbTags != null && this.fbTags.size() >  0) {
			if(input != null && input.size() > 0) {
				populateFBTagsFromMap(tags, input); 
			}
			if(output != null && output.size() > 0) {
				populateFBTagsFromMap(tags, output); 
			}
		}
	}
	
	private void populateFBTagsFromMap(Map<String, Object> tags, Map<String, Object> theMap) {
		theMap.keySet().forEach(key -> {
			processAttrValue(tags, key, theMap.get(key)); 
		});
	}
	
	@SuppressWarnings("unchecked")
	private void processAttrValue(Map<String, Object> tags, String tag, Object attrValue) {
		if(attrValue != null) {
			if(attrValue instanceof Map) {
				populateFBTagsFromMap(tags, (Map<String, Object>) attrValue);
			} else if(attrValue.getClass().isArray()) {
				Object[] theArray = ((Object[]) attrValue);
				for(int i = 0; i < theArray.length; i++) {
					if(theArray[i] instanceof Map) {
						populateFBTagsFromMap(tags, (Map<String, Object>) attrValue);
					} else {
						addTagIfFbTag(tags, tag, theArray[i].toString());
					}						
				}
			} else if(attrValue instanceof List) {
				List<Object> theList = (List<Object>) attrValue;
				theList.forEach(listEntry -> {
					if(listEntry instanceof Map) {
						populateFBTagsFromMap(tags, (Map<String, Object>) listEntry);
					} else {
						addTagIfFbTag(tags, tag, (listEntry == null ? null : listEntry.toString()));
					}						
				});
			} else {
				addTagIfFbTag(tags, tag, attrValue.toString());
			}
		}
	}

	private void addTagIfFbTag(Map<String, Object> tags, String key, String value) {
		if(this.fbTags.contains(key)) {
			if(tags.containsKey(key)) {
				Object currentValue = tags.get(key);
				if(currentValue instanceof String) {
					if(StringUtils.isNotBlank(value) && !currentValue.equals(value)) {
						List<String> newValue = new ArrayList<String>();
						newValue.add(currentValue.toString());
						newValue.add(value);
						tags.put(key, newValue);
					}
				} else if(currentValue instanceof List){
					boolean exists = false;
					@SuppressWarnings("unchecked")
					List<String> currentValues = (List<String>)currentValue; 
					for(String s : currentValues) {
						if(s.equals(value)) {
							exists = true;
							break;
						}
					}
					if(!exists) {
						currentValues.add(value);
					}
				}
			} else {
				if(StringUtils.isNotBlank(value)) {
					tags.put(key, value);
				}
			}
		}
	}
}
