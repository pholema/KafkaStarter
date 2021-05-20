package com.pholema.tool.starter.kafka;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import com.pholema.tool.starter.kafka.util.KafkaConfigProperties;
import com.pholema.tool.starter.kafka.util.PerformanceCalculator;

public class KafkaInstance implements Runnable {

	private static Logger logger = Logger.getLogger(KafkaInstance.class);

	private boolean stopped = false;
	private KafkaConsumer<?, ?> consumer = null;
	private List<ConsumerRecordHandler> consumerRecordHandlers = new ArrayList<>();
	private String whno;
	private String autoCommit;
	private String pollTimeout;
	private Boolean DEBUG;
	private boolean printPollTraceDebug;
	private Boolean enableThreadPool;
	private int debugThreadReplicateNumber;
	private Set<Future<Object>> outset;
	private ExecutorService execSvc;

	public KafkaInstance(KafkaConfigProperties kafkaConfigProperties, String whno) {
		this.whno = whno;
		Properties properties = kafkaConfigProperties.getConsumerProperties(whno);
		this.autoCommit = properties.getProperty("enable.auto.commit");
		this.pollTimeout = kafkaConfigProperties.getProperty("kafka.consumer.poll.timeout");
		this.DEBUG = Boolean.valueOf(kafkaConfigProperties.getProperty("kafka.consumer.poll.debug"));
		getInstance(properties, kafkaConfigProperties.getProperty("kafka.consumer.topic"));
		initThreadPool(kafkaConfigProperties);
	}

	public KafkaInstance setPrintPollTraceDebug(boolean printPollTraceDebug) {
		this.printPollTraceDebug = printPollTraceDebug;
		return this;
	}

	private void initThreadPool(KafkaConfigProperties kafkaConfigProperties) {
		this.enableThreadPool = Boolean.valueOf(kafkaConfigProperties.getProperty("kafka.thread.pool.enable"));
		if (enableThreadPool) {
			int threadPoolNumber = Integer.parseInt(kafkaConfigProperties.getProperty("kafka.thread.pool"));
			this.debugThreadReplicateNumber = Integer.parseInt(kafkaConfigProperties.getProperty("kafka.thread.replicate.debug"));
			outset = Collections.newSetFromMap(new HashMap<Future<Object>, Boolean>());
			execSvc = Executors.newFixedThreadPool(threadPoolNumber);
		}
	}

	protected KafkaConsumer getInstance(Properties properties, String topic) {
		if (consumer == null) {
			this.consumer = new KafkaConsumer<>(properties);
			consumer.subscribe(Collections.singletonList(topic));
		}
		return consumer;
	}

	@Override
	public void run() {
		try {
			while (!stopped) {
				ConsumerRecords<?, ?> entries = consumer.poll(Integer.valueOf(pollTimeout));
				long startTime = System.currentTimeMillis();
				int count = 0;
				for (ConsumerRecord<?, ?> entry : entries) {
					try {
						if (printPollTraceDebug) System.out.println("whno=" + this.whno + " --> " + entry.partition() + " " + entry.value().toString());
						for (ConsumerRecordHandler consumerRecordHandler : consumerRecordHandlers) {
							if (enableThreadPool) {
								// ======= executor service =======
								for (int i = 0; debugThreadReplicateNumber > i; i++) {
									outset.add(execSvc.submit(new Callable<Object>() {
										@Override
										public String call() {
											boolean executed = consumerRecordHandler.handleRecord(entry.value().toString());
											return Boolean.toString(executed);
										}
									}));
								}
							} else {
								boolean executed = consumerRecordHandler.handleRecord(entry.value().toString());
								if (executed) {
									count++;
								}
							}
						}
					} catch (Exception e) {
						StringWriter sw = new StringWriter();
						e.printStackTrace(new PrintWriter(sw));
						logger.error(sw.toString());
					}
				}

				if (enableThreadPool) {
					// ======= executor service =======
					for (Future<Object> future : outset) {
						try {
							Object exec = future.get(240, TimeUnit.SECONDS);
							if (exec != null && Boolean.valueOf(exec.toString())) {
								count++;
							}
						} catch (Exception e) {
							future.cancel(true);
						}
					}
					outset.clear();
				}

				if (DEBUG && count > 0) {
					PerformanceCalculator.printLogExecutedTime(whno, count + "/" + entries.count(), startTime);
					PerformanceCalculator.plusSuccess(count);
				}
				PerformanceCalculator.plus(entries.count());
				if (!Boolean.valueOf(autoCommit)) {
					consumer.commitAsync();
				}

				// notice
				for (ConsumerRecordHandler consumerRecordHandler : consumerRecordHandlers) {
					consumerRecordHandler.pollFetchFinish();
				}
			}
		} catch (Exception ex) {
			logger.error(ex.getMessage(), ex);
		} finally {
			consumer.close();
		}
	}

	public void start() {
		Thread thread = new Thread(this);
		thread.start();
	}

	public void stop() {
		this.stopped = true;
	}

	/**
	 * register poll consumer record handler
	 */
	public KafkaInstance addConsumerRecordHandler(ConsumerRecordHandler consumerRecordHandler) {
		this.consumerRecordHandlers.add(consumerRecordHandler);
		return this;
	}

	public interface ConsumerRecordHandler {

		boolean handleRecord(String record);
		void pollFetchFinish();
	}
}
