package utils.plugins;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;

import play.Logger;
import play.Play;
import play.PlayPlugin;
import play.exceptions.PlayException;
import play.exceptions.UnexpectedException;
import play.jobs.Every;
import play.jobs.On;
import play.jobs.OnApplicationStart;
import play.jobs.OnApplicationStop;
import play.libs.Expression;
import play.libs.Time;
import play.libs.Time.CronExpression;
import play.utils.Java;

public class DividedJobsPlugin extends PlayPlugin {

	private static Map<String, ScheduledThreadPoolExecutor> executorMap = new HashMap<String, ScheduledThreadPoolExecutor>();
	static {
		executorMap.put(null, new ScheduledThreadPoolExecutor(10,
				new ThreadPoolExecutor.AbortPolicy()));
	}
	public static List<DividedJob> scheduledJobs = null;

	@Override
	public String getStatus() {
		StringWriter sw = new StringWriter();
		PrintWriter out = new PrintWriter(sw);
		if (executorMap.isEmpty()) {
			out.println("DividedJobs execution pool:");
			out.println("~~~~~~~~~~~~~~~~~~~");
			out.println("(not yet started)");
			return sw.toString();
		}
		out.println("DividedJobs execution pool:");
		out.println("~~~~~~~~~~~~~~~~~~~");
		for (Entry<String, ScheduledThreadPoolExecutor> entry : executorMap
				.entrySet()) {
			ScheduledThreadPoolExecutor executor = entry.getValue();
			String poolName = entry.getKey();
			out.println(poolName == null ? "Default Pool" : "Pool name: "
					+ poolName);
			out.println("Pool size: " + executor.getPoolSize());
			out.println("Active count: " + executor.getActiveCount());
			out.println("Scheduled task count: " + executor.getTaskCount());
			out.println("Queue size: " + executor.getQueue().size());
			if (!executor.getQueue().isEmpty()) {
				out.println("Waiting jobs:");
				for (Object o : executor.getQueue()) {
					ScheduledFuture task = (ScheduledFuture) o;
					out.println(Java
							.extractUnderlyingCallable((FutureTask) task)
							+ " will run in "
							+ task.getDelay(TimeUnit.SECONDS)
							+ " seconds");
				}
			}
			out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~");
		}
		SimpleDateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
		if (!scheduledJobs.isEmpty()) {
			out.println();
			out.println("Scheduled jobs (" + scheduledJobs.size() + "):");
			out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~");
			for (DividedJob job : scheduledJobs) {
				out.print(job.getClass().getName());
				if (job.getClass()
						.isAnnotationPresent(OnApplicationStart.class)) {
					out.print(" run at application start.");
				}
				if (job.getClass().isAnnotationPresent(On.class)) {
					out.print(" run with cron expression "
							+ job.getClass().getAnnotation(On.class).value()
							+ ".");
				}
				if (job.getClass().isAnnotationPresent(Every.class)) {
					out.print(" run every "
							+ job.getClass().getAnnotation(Every.class).value()
							+ ".");
				}
				if (job.lastRun > 0) {
					out.print(" (last run at "
							+ df.format(new Date(job.lastRun)));
					if (job.wasError) {
						out.print(" with error)");
					} else {
						out.print(")");
					}
				} else {
					out.print(" (has never run)");
				}
				out.println();
			}
		}
		return sw.toString();
	}

	@Override
	public void afterApplicationStart() {
		List<Class<?>> jobs = new ArrayList<Class<?>>();
		for (Class clazz : Play.classloader.getAllClasses()) {
			if (DividedJob.class.isAssignableFrom(clazz)) {
				jobs.add(clazz);
			}
		}
		scheduledJobs = new ArrayList<DividedJob>();
		for (final Class<?> clazz : jobs) {
			ScheduledThreadPoolExecutor executor = null;
			if (clazz.isAnnotationPresent(JobPool.class)) {
				executor = getExecutorByName(clazz.getAnnotation(JobPool.class)
						.value());
			} else {
				executor = getExecutorByName(null);
			}
			// @OnApplicationStart
			if (clazz.isAnnotationPresent(OnApplicationStart.class)) {
				// check if we're going to run the job sync or async
				OnApplicationStart appStartAnnotation = clazz
						.getAnnotation(OnApplicationStart.class);
				if (!appStartAnnotation.async()) {
					// run job sync
					try {
						DividedJob<?> job = ((DividedJob<?>) clazz
								.newInstance());
						scheduledJobs.add(job);
						job.run();
						if (job.wasError) {
							if (job.lastException != null) {
								throw job.lastException;
							}
							throw new RuntimeException(
									"@OnApplicationStart Job has failed");
						}
					} catch (InstantiationException e) {
						throw new UnexpectedException(
								"Job could not be instantiated", e);
					} catch (IllegalAccessException e) {
						throw new UnexpectedException(
								"Job could not be instantiated", e);
					} catch (Throwable ex) {
						if (ex instanceof PlayException) {
							throw (PlayException) ex;
						}
						throw new UnexpectedException(ex);
					}
				} else {
					// run job async
					try {
						DividedJob<?> job = ((DividedJob<?>) clazz
								.newInstance());
						scheduledJobs.add(job);
						// start running job now in the background
						@SuppressWarnings("unchecked")
						Callable<DividedJob> callable = (Callable<DividedJob>) job;
						executor.submit(callable);
					} catch (InstantiationException ex) {
						throw new UnexpectedException("Cannot instanciate Job "
								+ clazz.getName());
					} catch (IllegalAccessException ex) {
						throw new UnexpectedException("Cannot instanciate Job "
								+ clazz.getName());
					}
				}
			}
			// @On
			if (clazz.isAnnotationPresent(On.class)) {
				try {
					DividedJob<?> job = ((DividedJob<?>) clazz.newInstance());
					scheduledJobs.add(job);
					scheduleForCRON(job, executor);
				} catch (InstantiationException ex) {
					throw new UnexpectedException("Cannot instanciate Job "
							+ clazz.getName());
				} catch (IllegalAccessException ex) {
					throw new UnexpectedException("Cannot instanciate Job "
							+ clazz.getName());
				}
			}
			// @Every
			if (clazz.isAnnotationPresent(Every.class)) {
				try {
					DividedJob job = (DividedJob) clazz.newInstance();
					scheduledJobs.add(job);
					String value = job.getClass().getAnnotation(Every.class)
							.value();
					if (value.startsWith("cron.")) {
						value = Play.configuration.getProperty(value);
					}
					value = Expression.evaluate(value, value).toString();
					if (!"never".equalsIgnoreCase(value)) {
						executor.scheduleWithFixedDelay(job,
								Time.parseDuration(value),
								Time.parseDuration(value), TimeUnit.SECONDS);
					}
				} catch (InstantiationException ex) {
					throw new UnexpectedException("Cannot instanciate Job "
							+ clazz.getName());
				} catch (IllegalAccessException ex) {
					throw new UnexpectedException("Cannot instanciate Job "
							+ clazz.getName());
				}
			}
		}
	}

	@Override
	public void onApplicationStart() {
		try {
			String dividedJobsProperty = Play.configuration
					.getProperty("dividedJobs");
			if (StringUtils.isNotBlank(dividedJobsProperty)) {
				String[] executors = dividedJobsProperty.split(",");
				for (String executor : executors) {
					String[] executorDetail = executor.split(":");
					executorMap.put(
							executorDetail[0],
							new ScheduledThreadPoolExecutor(Integer
									.parseInt(executorDetail[1]),
									new ThreadPoolExecutor.AbortPolicy()));
				}
			}
		} catch (Exception e) {
			throw new UnexpectedException(
					"Your property has error,DividedJobPlugin is not start Successfully!\ne.g.\tdividedJobs=name1:size1,name2:size2");
		}
	}

	public static <V> void scheduleForCRON(DividedJob<V> job,
			ScheduledThreadPoolExecutor executor) {
		if (!job.getClass().isAnnotationPresent(On.class)) {
			return;
		}
		String cron = job.getClass().getAnnotation(On.class).value();
		if (cron.startsWith("cron.")) {
			cron = Play.configuration.getProperty(cron);
		}
		cron = Expression.evaluate(cron, cron).toString();
		if (cron == null || "".equals(cron) || "never".equalsIgnoreCase(cron)) {
			Logger.info("Skipping job %s, cron expression is not defined", job
					.getClass().getName());
			return;
		}
		try {
			Date now = new Date();
			cron = Expression.evaluate(cron, cron).toString();
			CronExpression cronExp = new CronExpression(cron);
			Date nextDate = cronExp.getNextValidTimeAfter(now);
			if (nextDate == null) {
				Logger.warn(
						"The cron expression for job %s doesn't have any match in the future, will never be executed",
						job.getClass().getName());
				return;
			}
			if (nextDate.equals(job.nextPlannedExecution)) {
				Date nextInvalid = cronExp.getNextInvalidTimeAfter(nextDate);
				nextDate = cronExp.getNextValidTimeAfter(nextInvalid);
			}
			job.nextPlannedExecution = nextDate;
			executor.schedule((Callable<V>) job,
					nextDate.getTime() - now.getTime(), TimeUnit.MILLISECONDS);
			job.executor = executor;
		} catch (Exception ex) {
			throw new UnexpectedException(ex);
		}
	}

	@Override
	public void onApplicationStop() {
		List<Class> jobs = Play.classloader
				.getAssignableClasses(DividedJob.class);
		for (final Class clazz : jobs) {
			// @OnApplicationStop
			if (clazz.isAnnotationPresent(OnApplicationStop.class)) {
				try {
					DividedJob<?> job = ((DividedJob<?>) clazz.newInstance());
					scheduledJobs.add(job);
					job.run();
					if (job.wasError) {
						if (job.lastException != null) {
							throw job.lastException;
						}
						throw new RuntimeException(
								"@OnApplicationStop Job has failed");
					}
				} catch (InstantiationException e) {
					throw new UnexpectedException(
							"Job could not be instantiated", e);
				} catch (IllegalAccessException e) {
					throw new UnexpectedException(
							"Job could not be instantiated", e);
				} catch (Throwable ex) {
					if (ex instanceof PlayException) {
						throw (PlayException) ex;
					}
					throw new UnexpectedException(ex);
				}
			}
		}

		for (ScheduledThreadPoolExecutor executor : executorMap.values()) {
			executor.shutdownNow();
			executor.getQueue().clear();
		}
	}

	public static ScheduledThreadPoolExecutor getExecutorByName(String name) {
		return executorMap.get(name);
	}
}
