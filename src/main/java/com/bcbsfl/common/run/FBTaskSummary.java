/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * 
 */
package com.bcbsfl.common.run;

import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRawValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.TaskSummary;


@JsonInclude(Include.NON_NULL)
public class FBTaskSummary extends TaskSummary{

	@JsonIgnore
	private Map<String, Object> tags = new LinkedHashMap<String, Object>();
	private ObjectMapper mapper = new ObjectMapper();
	private Task task;
	@JsonProperty("@timestamp")
	public String timestampValue;
	public boolean reconciled = true;

	public FBTaskSummary(Task task) {
		super(task);
		this.task = task;
		
	}

	public FBTaskSummary() {
	}

	@Override
	@JsonRawValue
	public String getInput() {
		String input = task.getInputData() == null ? "{}" : mapper.valueToTree(task.getInputData()).toString();
		return input;
	}
	
	@Override
	@JsonRawValue
	public String getOutput() {
		String output = task.getOutputData() == null ? "{}" : mapper.valueToTree(task.getOutputData()).toString();
		return output;
	}
	
	public void addTag(String key, String value) {
		this.tags.put(key, value);
	}

	public Map<String, Object> getTags() {
		return tags;
	}
	@JsonProperty("tags")
	@JsonRawValue
	public String getTagsAsJson() {
		return mapper.valueToTree(this.tags).toString();
	}
}
