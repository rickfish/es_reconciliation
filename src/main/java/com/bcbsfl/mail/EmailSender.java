package com.bcbsfl.mail;

import java.net.InetAddress;
import java.text.DecimalFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

import javax.mail.Message;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bcbsfl.es.Utils;

/**
 * Sends standard emails to Conductor interested parties
 */
public class EmailSender {
    private static final Logger logger = LoggerFactory.getLogger(EmailSender.class);
    /**
     * Maximum number of stacktrace lines printed for the first exception in the nested list of exceptions
     */
    private static final int MAX_FIRST_EXCEPTION_STACKELEMENTS = 30;
    /**
     * Maximum number of stacktrace lines printed for the 'caused by' exceptions
     */
    private static final int MAX_ADDL_EXCEPTION_STACKELEMENTS = 15;

    private Session mailSession = null;
	private String smtpHost = null;
	private String from = null;
	private String replyTo = null;
	private String to = null;
	private String environment = null;
	
	public EmailSender() {
		this.smtpHost = getTrimmedString(Utils.getProperty("email.smtp.host"));
		if(this.smtpHost != null) {
			this.from = getTrimmedString(Utils.getProperty("email.address.from"));
			this.replyTo = getTrimmedString(Utils.getProperty("email.address.replyto"));
			this.to = getTrimmedString(Utils.getProperty("email.address.to"));
			Properties props = System.getProperties();
			props.put("mail.smtp.host", this.smtpHost);
			this.mailSession = Session.getInstance(props, null);
			this.environment = Utils.getProperty("env");
		}
	}
	
	/**
	 * Send an email that tells the recipient(s) that there was an exception in Conductor. For this email, we have the 
	 * error message to be displayed and the exception that was generated
	 * @param message a message to display in the email describing the exception context
	 * @param exception the exception
	 */
	public void sendExceptionEmail(String message, Throwable exception) {
		if(this.smtpHost == null) {
			return;
		}
		try {
			StringBuffer sb = new StringBuffer("<html><body><table><div style='font-family:Arial,Helvetica,sans-serif;font-size:11px;'>");
			sb.append("<div>Host: <span style='font-weight:bold;'>" + InetAddress.getLocalHost().getHostName() + "</span></div>");
			/*
			 * Have the lambda function add the specific email message
			 */
			sb.append("</div><div style='color:black;font-size:11px;margin-top:25px;font-family:Arial,Helvetica,sans-serif;font-style:italic'></div>");
			if(exception != null) {
				sb.append("<div>Message: <span style='font-weight:bold;'>" + exception.getMessage() + "</span></div>");
				sb.append("<div style='text-decoration:underline;margin-top:5px;margin-bottom:2px;font-size:9px;font-weight:bold;font-family:Arial Black,Gadget,sans-serif'>STACK TRACE</div>");
				sb.append("<div style='font-size:10px;font-family:Courier New,Courier,monospace'>");
				addExceptionStackTrace(true, 1, sb, exception);
			} else {
				sb.append("<div>Message: <span style='font-weight:bold;'>" + message + "</span></div>");
			}
			sb.append("</div></body></html>");
			sendEmail(message, sb.toString());
		} catch(Exception e) {
			logger.error("Error sending email for message '" + message + "'. Got this exception", e);
		}
	}
	
	public void sendNonMatchesEmail(long recCount, String doctype, Map<String, String> nonMatches) {
		String message = "Found " + nonMatches.size() + " " + doctype + "s whose database status was different than the ElasticSearch status";
		try {
			StringBuffer sb = new StringBuffer("<html><body><table><div style='color:black;font-family:Arial,Helvetica,sans-serif;font-size:11px;'>");
			sb.append("<div style='font-weight:bold;font-size:12px;font-style:italic;margin-bottom:10px;'>");
			sb.append("Note that the ElasticSearch documents were updated to the database documents so this is just a notification");
			sb.append("</div>");
			sb.append("<div style='font-weight:normal;font-size:12px;margin-bottom:10px;'>");
			sb.append("Processed " + DecimalFormat.getInstance().format(recCount) + " " + doctype + "s modified on " + System.getProperty("db.startTimeframe"));
			sb.append("</div>");
			for(String id: nonMatches.keySet()) {
				sb.append(doctype + "_id " + id + ": " + nonMatches.get(id) + "<br/>");
			}
			sb.append("</div></body></html>");
			sendEmail(message, sb.toString());
		} catch(Exception e) {
			logger.error("Error sending email for message '" + message + "'. Got this exception", e);
		}
	}
	
	/**
	 * Send an email in html format 
	 * @param message a message to display in the email describing the context
	 * @param body the body of the email
	 */
	public void sendEmail(String message, String body) {
		if(this.smtpHost == null) {
			return;
		}
		try {
			MimeMessage msg = getMessage();
			msg.setSubject("[" + this.environment + "] Conductor Reconciliation Error: " + message, "UTF-8");
			msg.setText(body, "utf-8", "html");
			Transport.send(msg);
		} catch(Exception e) {
			logger.error("Error sending email for message '" + message + "'. Got this exception", e);
		}
	}
	
	/**
	 * Print a stacktrace in the body of the email
	 * @param first whether or not this is the first email in a specified time period
	 * @param exceptionCount the number of exceptions of this type that occurred in the time period
	 * @param sb the StringBuffer containing the body of the email
	 * @param e the exception for which to print the stack trace
	 */
	private void addExceptionStackTrace(boolean first, int exceptionCount, StringBuffer sb, Throwable e) {
		if(e != null) {
			StackTraceElement steArray[] = e.getStackTrace();
			int elementsLeft = steArray.length;
			int elementNumber = 0;
			int maxElements = (first ? MAX_FIRST_EXCEPTION_STACKELEMENTS : MAX_ADDL_EXCEPTION_STACKELEMENTS);
			sb.append("<span style='font-weight: bold;color:red'>");
			if(!first) {
				sb.append("<span style='font-weight: bold;color:red'>Caused by: </span>");
			}
			sb.append("<span style='font-weight: bold;color:blue'>" + e.getClass().getName() + "</span>");
			if(e.getMessage() != null) {
				sb.append("<span style='font-weight: bold;color:red'>: " + e.getMessage() + "</span>");
			}
			sb.append("</br>");
			for(StackTraceElement ste: steArray) {
				elementNumber++;
				elementsLeft--;
				sb.append("<span style='color:red;font-weight:normal'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;at " + this.getFullStackTraceElementMethodName(ste) 
					+ "(</span><span style='color:blue;font-weight:normal'>" + ste.getFileName());
				if(ste.getLineNumber() > 0) {
					sb.append(":" + ste.getLineNumber());
				}
				sb.append("</span><span style='color:red;font-weight:normal'>)</span></br>");
				if(elementNumber > maxElements && elementsLeft > 0) {
					sb.append("<span style='color:red;font-weight:normal'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;... " + elementsLeft + " more</span></br>");
					break;
				}
			}
			if(exceptionCount < 5 && e.getCause() != null) {
				addExceptionStackTrace(false, exceptionCount + 1, sb, e.getCause());
			}
		}
	}
	
	private String getFullStackTraceElementMethodName(StackTraceElement ste) {
		String methodName = StringUtils.isNotBlank(ste.getMethodName()) ? ste.getMethodName().replace("<", "&lt;") : "";
		methodName = methodName.replace(">",  "&gt;");
		return ste.getClassName() + (StringUtils.isNotBlank(methodName) ? "." + methodName : "") ;
	}
	
	private MimeMessage getMessage() throws Exception {
		MimeMessage msg = new MimeMessage(this.mailSession);
		msg.addHeader("format", "flowed");
		msg.addHeader("Content-Transfer-Encoding", "8bit");
		if(StringUtils.isNotEmpty(this.from)) {
			msg.setFrom(new InternetAddress(this.from));
		}
		if(StringUtils.isNotEmpty(this.replyTo)) {
			msg.setReplyTo(InternetAddress.parse(this.replyTo, false));
		}
		msg.setSentDate(new Date());
		String recipients = this.to;
		msg.addRecipients(Message.RecipientType.TO, InternetAddress.parse(recipients));
		return msg;
	}

	public String getTrimmedString(String s) {
		return s == null ? null : s.trim();
	}

	public String getSmtpHost() {
		return smtpHost;
	}
	public void setSmtpHost(String smtpHost) {
		this.smtpHost = smtpHost;
	}
	public String getFrom() {
		return from;
	}
	public void setFrom(String from) {
		this.from = from;
	}
	public String getReplyTo() {
		return replyTo;
	}
	public void setReplyTo(String replyTo) {
		this.replyTo = replyTo;
	}
	public String getTo() {
		return to;
	}
	public void setTo(String to) {
		this.to = to;
	}
}
