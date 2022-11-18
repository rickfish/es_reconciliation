package com.bcbsfl.es;

import com.netflix.conductor.common.run.Workflow;

public class WorkflowComparator extends Comparator<Workflow>{
    static protected long recCount = 0;
	public WorkflowComparator() {
		super(WORKFLOW_DOC_TYPE);
	}
	@Override
	protected Workflow getObjectFromJson(String json) throws Exception {
		return objectMapper.readValue(json,  Workflow.class);
	}
	
	@Override
	protected String getStatus(Workflow workflow) {
		return workflow.getStatus().name();
	}
	@Override
	protected String getId(Workflow workflow) {
		return workflow.getWorkflowId();
	}
	@Override
	protected String getName(Workflow workflow) {
		return workflow.getWorkflowName();
	}
	@Override
	protected long getUpdateTime(Workflow workflow) {
		return workflow.getUpdateTime();
	}

	protected String getElasticIdAttribute(Workflow workflow) {
		return "workflowId";
	}
	@Override
	protected void setRecCount(long count) {
		recCount = count;
	}
	@Override
	protected long getRecCount() {
		return recCount;
	}
}