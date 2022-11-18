package com.bcbsfl.es;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;

public class Main {

	public static void main(String[] args) {
		Utils.init();
		String appToRun = Utils.getProperty("which.app");
		System.out.println("Running " + appToRun);
		try {
			switch(appToRun) {
			case "TaskComparator":
				if(Utils.getBooleanProperty("run.continuously")) {
					runContinuously();
				} else {
					new TaskComparator().compare();
				}
				break;
			case "WorkflowComparator":
				if(Utils.getBooleanProperty("run.continuously")) {
					runContinuously();
				} else {
					new WorkflowComparator().compare();
				}
				break;
			case "TaskReconciler":
				if(Utils.getBooleanProperty("run.continuously")) {
					runContinuously();
				} else {
					new TaskReconciler().reconcile();
				}
				break;
			case "WorkflowReconciler":
				if(Utils.getBooleanProperty("run.continuously")) {
					runContinuously();
				} else {
					new WorkflowReconciler().reconcile();
				}
				break;
			default:
				if(Utils.getBooleanProperty("run.continuously")) {
					runContinuously(appToRun);
				} else {
					System.out.println("******* The which.app variable is set to '" + appToRun + "' which is not a valid option.");
				}
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
		while(true) {
			System.out.println("Since it is possible that we are running in Openshift, we will keep sleeping for an hour to keep the pod running...");
			try {
				Thread.sleep(3600000);
			} catch(Exception e) {
			}
		}
	}
	static private void runContinuously() throws Exception {
		runContinuously(null);	
	}

	static private void runContinuously(String resourceType) throws Exception {
		System.out.println("This will be run contiunously");
		String startDateString = Utils.getProperty("db.startTimeframe");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Calendar startDate = null;
		Calendar endDate = null;
		if(StringUtils.isBlank(startDateString)) {
			startDate = Calendar.getInstance();
			startDate.add(Calendar.DATE, -1);
			startDate.set(Calendar.HOUR_OF_DAY, 0);
			startDate.set(Calendar.MINUTE, 0);
			startDate.set(Calendar.SECOND, 0);
			startDate.set(Calendar.MILLISECOND, 0);
			startDateString = sdf.format(startDate.getTime());
			System.setProperty("db.startTimeframe", startDateString);
			System.out.println("No db.startTimeframe found and run.continuously is true so we will use yesterday as the start date");
		}
		String dateComponents[] = startDateString.split("-");
		if(dateComponents.length != 3) {
			throw new Exception("db.startTimeframe needs to be specified in yyyy-mm-dd format to run continuously");
		}
		if(startDate == null) {
			startDate = Calendar.getInstance();
			startDate.setTime(sdf.parse(startDateString));
			startDate.set(Calendar.HOUR_OF_DAY, 0);
			startDate.set(Calendar.MINUTE, 0);
			startDate.set(Calendar.SECOND, 0);
			startDate.set(Calendar.MILLISECOND, 0);
		}
		while(true) {
			endDate = Calendar.getInstance();
			endDate.setTime(startDate.getTime());
			endDate.add(Calendar.DATE, 1);
			while(true) {
				Calendar today = Calendar.getInstance();
				today.set(Calendar.HOUR_OF_DAY, 0);
				today.set(Calendar.MINUTE, 0);
				today.set(Calendar.SECOND, 0);
				today.set(Calendar.MILLISECOND, 0);
				if(today.after(startDate)) {
					System.setProperty("db.startTimeframe", sdf.format(startDate.getTime()));
					System.setProperty("db.endTimeframe", sdf.format(endDate.getTime()));
					break;
				}
				System.out.println("[" + new Date().toString() + "] Today is not greater than " + startDate.getTime().toString() + ", waiting for an hour, will check if it is time to run again.");
				try {
					Thread.sleep(3600000);
				} catch(Exception e) {
				}
			}
			try {
				Comparator<?> comparator = null;
				Reconciler<?> reconciler = null;
				long start = System.currentTimeMillis();
				long startApp = 0, elapsedTaskCompare = 0, elapsedWorkflowCompare = 0, elapsedTaskReconcile = 0, elapsedWorkflowReconcile = 0;
				if(!"Workflows".equals(resourceType)) {
					startApp = System.currentTimeMillis();
					comparator = new TaskComparator();
					comparator.compare();
					elapsedTaskCompare = System.currentTimeMillis() - startApp;
				}
				if(!"Tasks".equals(resourceType)) {
					startApp = System.currentTimeMillis();
					comparator = new WorkflowComparator();
					comparator.compare();
					elapsedWorkflowCompare = System.currentTimeMillis() - startApp;
				}
				if(!"Workflows".equals(resourceType)) {
					startApp = System.currentTimeMillis();
					reconciler = new TaskReconciler();
					reconciler.reconcile();
					elapsedTaskReconcile = System.currentTimeMillis() - startApp;
				}
				if(!"Tasks".equals(resourceType)) {
					startApp = System.currentTimeMillis();
					reconciler = new WorkflowReconciler();
					reconciler.reconcile();
					elapsedWorkflowReconcile = System.currentTimeMillis() - startApp;
				}
				System.out.println("*************************************************************************");
				if(elapsedTaskCompare > 0) {
					System.out.println("TaskComparator elapsed time: " + Utils.convertMillisecondsToTimeString(elapsedTaskCompare));
				}
				if(elapsedWorkflowCompare > 0) {
					System.out.println("WorkflowComparator elapsed time: " + Utils.convertMillisecondsToTimeString(elapsedWorkflowCompare));
				}
				if(elapsedTaskReconcile > 0) {
					System.out.println("TaskReconciler elapsed time: " + Utils.convertMillisecondsToTimeString(elapsedTaskReconcile));
				}
				if(elapsedWorkflowReconcile > 0) {
					System.out.println("WorkflowReconciler elapsed time: " + Utils.convertMillisecondsToTimeString(elapsedWorkflowReconcile));
				}
				System.out.println("Total elapsed time for this cycle: " + Utils.convertMillisecondsToTimeString(System.currentTimeMillis() - start));
				System.out.println("*************************************************************************");
				startDate.add(Calendar.DATE, 1);
			} catch(Exception e) {
				System.out.println("[" + new Date().toString() + "] Got the below exception, waiting 1 hour, will try again then.");
				e.printStackTrace();
				try {
					Thread.sleep(3600000);
				} catch(Exception e2) {
				}
			}
		}
	}
}
