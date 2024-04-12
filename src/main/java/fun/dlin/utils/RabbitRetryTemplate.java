package fun.dlin.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

/**
 * 使用这个工具时，消息确认模式必须是自动消息确认机.
 * 所有处理逻辑应该包裹在RabbitRetryTemplate.RetryCallback#execute(org.springframework.amqp.core.Message)
 * 方法中
 */
@Component
public class RabbitRetryTemplate {

	private final Logger log = LoggerFactory.getLogger(RabbitRetryTemplate.class);

	public static final String HEADER_RETRY_LEVEL = "Retry-Level";
	private final String DELAY_EXCHANGE_NAME = "rabbitmq_retry_template.exchange.delayed";
	public static final String DEAD_LETTER_SUFFIX = ".dead-letter";

	private final RabbitAdmin rabbitAdmin;

	// 重试间隔等级：10s、30s、1m、5m、10m、30m、1h、1h30m、2h
	private final int[] retryLevel = {10_000, 30_000, 60_000, 300_000, 600_000, 1_800_000, 3_600_000, 5_400_000, 7_200_000};

	public RabbitRetryTemplate(RabbitTemplate template) {
		rabbitAdmin = new RabbitAdmin(template);
		rabbitAdmin.declareExchange(ExchangeBuilder.directExchange(DELAY_EXCHANGE_NAME)
				.delayed()
				.autoDelete()
				.build());
	}


	public void execute(Message originalMessage, RetryCallback retryCallback) {

		MDC.put("RetryMsgId", originalMessage.getMessageProperties().getMessageId());

		log.info("RetryMsgId:{},RetryLevel:{}", MDC.get("RetryMsgId"), getRetryLevel(originalMessage));

		boolean flag = true;
		try {
			if (!retryCallback.execute()) {
				flag = sendDelayMessage(originalMessage);
			}
		} catch (Exception e) {
			flag = sendDelayMessage(originalMessage);
		} finally {
			if (!flag) {
				sendDeadlineMessage(originalMessage);
			}

			MDC.remove("RetryMsgId");

		}

	}

	/**
	 * 发送延迟消息
	 *
	 * @param originalMessage 消息
	 * @return true 表示发送成功,
	 * false 表示发送失败, 此时需要将消息发送到死信队列
	 */
	private boolean sendDelayMessage(Message originalMessage) {
		Integer originalLevel = getRetryLevel(originalMessage);

		if (originalLevel >= retryLevel.length - 1) {
			return false;
		}

		int newLevel = originalLevel + 1;

		int delayTime = retryLevel[newLevel];
		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setDelay(delayTime);
		String consumerQueue = originalMessage.getMessageProperties().getConsumerQueue();

		bindToDelayExchange(consumerQueue);

		rabbitAdmin.getRabbitTemplate().send(DELAY_EXCHANGE_NAME,
				consumerQueue,
				MessageBuilder.fromMessage(originalMessage)
						.andProperties(messageProperties)
						.setHeader(HEADER_RETRY_LEVEL, newLevel)
						.build()
		);
		return true;
	}

	private static int getRetryLevel(Message originalMessage) {
		Integer originalLevel = originalMessage.getMessageProperties().getHeader(HEADER_RETRY_LEVEL);

		if (originalLevel == null) {
			originalLevel = -1;
		}
		return originalLevel;
	}

	private void bindToDelayExchange(String consumerQueue) {
		rabbitAdmin.declareBinding(BindingBuilder
				.bind(QueueBuilder.durable(consumerQueue).build())
				.to((DirectExchange) ExchangeBuilder.directExchange(DELAY_EXCHANGE_NAME).delayed().build())
				.withQueueName()
		);
	}

	/**
	 * 发送死信
	 *
	 * @param originalMessage 消息
	 */
	private void sendDeadlineMessage(Message originalMessage) {
		String deadLetterQueueName = originalMessage.getMessageProperties().getConsumerQueue() + DEAD_LETTER_SUFFIX;
		rabbitAdmin.declareQueue(QueueBuilder
				.durable(deadLetterQueueName).build());

		rabbitAdmin.getRabbitTemplate().send(deadLetterQueueName, originalMessage);
	}


	public interface RetryCallback {

		/**
		 * 执行方法
		 *
		 * @return true 表示执行成功，false 表示执行失败，将进行重试
		 */
		boolean execute() throws Exception;
	}
}
