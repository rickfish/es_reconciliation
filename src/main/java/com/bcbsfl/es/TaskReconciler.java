package com.bcbsfl.es;

import java.util.Date;

import com.bcbsfl.common.run.FBTaskSummary;
import com.netflix.conductor.common.metadata.tasks.Task;

public class TaskReconciler extends Reconciler<Task> {
	public TaskReconciler() {
		super(TASK_DOC_TYPE);
	}
	
	@Override
	protected Task getObjectFromJson(String json) throws Exception {
		return objectMapper.readValue(json,  Task.class);
	}
	
	@Override
	protected String getStatus(Task task) {
		return task.getStatus().name();
	}
	@Override
	protected String getId(Task task) {
		return task.getTaskId();
	}
	@Override
	protected long getUpdateTime(Task task) {
		return task.getUpdateTime();
	}

	@Override
	protected String getElasticIdAttribute(Task task) {
		return "taskId";
	}

	@Override
	protected void updateObjectInES(Task task) throws Exception {
        FBTaskSummary summary = new FBTaskSummary(task);
        fbDocTagger.populateFBTags(summary.getTags(), task.getInputData(), task.getOutputData());
        summary.timestampValue = ES_TIMESTAMP_FORMAT.format(new Date());
        String secRole = task.getInputData() == null ? null : (String) task.getInputData().get("secRole");
        String theIndex = this.getCompleteIndexName(secRole, !task.getStatus().isTerminal());
       	indexObject(theIndex, TASK_DOC_TYPE, task.getTaskId(), summary);
       	removeObjectFromIndices(task.getTaskId(), "taskId", theIndex);
	}
}