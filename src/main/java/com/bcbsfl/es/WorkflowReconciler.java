package com.bcbsfl.es;

import java.util.Date;

import com.bcbsfl.common.run.FBWorkflowSummary;
import com.netflix.conductor.common.run.Workflow;

public class WorkflowReconciler extends Reconciler<Workflow> {
	public WorkflowReconciler() {
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
	protected long getUpdateTime(Workflow workflow) {
		return workflow.getUpdateTime();
	}
	
	@Override
	protected String getElasticIdAttribute(Workflow workflow) {
		return "workflowId";
	}

	@Override
	protected void updateObjectInES(Workflow workflow) throws Exception {
        FBWorkflowSummary summary = new FBWorkflowSummary(workflow);
        fbDocTagger.populateFBTags(summary.getTags(), workflow.getInput(), workflow.getOutput());
        summary.timestampValue = ES_TIMESTAMP_FORMAT.format(new Date());
        String secRole = workflow.getInput() == null ? null : (String) workflow.getInput().get("secRole");
        String theIndex = this.getCompleteIndexName(secRole, !workflow.getStatus().isTerminal());
       	indexObject(theIndex, WORKFLOW_DOC_TYPE, workflow.getWorkflowId(), summary);
       	removeObjectFromIndices(workflow.getWorkflowId(), "workflowId", theIndex);
	}
}