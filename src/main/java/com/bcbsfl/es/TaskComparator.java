package com.bcbsfl.es;

import com.netflix.conductor.common.metadata.tasks.Task;

public class TaskComparator extends Comparator<Task>{
    static protected long recCount = 0;
	public TaskComparator() {
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
	protected String getName(Task task) {
		return ("WAIT".equals(task.getTaskType()) ? 
			task.getTaskDefName() : task.getTaskType());
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
	protected void setRecCount(long count) {
		recCount = count;
	}
	@Override
	protected long getRecCount() {
		return recCount;
	}
}