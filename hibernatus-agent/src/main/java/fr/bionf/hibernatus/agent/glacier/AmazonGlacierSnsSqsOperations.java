package fr.bionf.hibernatus.agent.glacier;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.actions.SQSActions;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.*;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


class AmazonGlacierSnsSqsOperations {
    private static final Logger logger = LoggerFactory.getLogger(AmazonGlacierSnsSqsOperations.class);

    private String sqsQueueARN;
    String sqsQueueURL;
    String snsTopicARN;
    private String snsSubscriptionARN;
    AmazonSQS sqsClient;
    AmazonSNS snsClient;

    AmazonGlacierSnsSqsOperations(AWSCredentialsProvider credentials) {
        sqsClient = AmazonSQSClientBuilder.standard()
                .withCredentials(credentials)
                .withRegion(Regions.EU_WEST_3)
                .build();
        snsClient = AmazonSNSClientBuilder.standard()
                .withCredentials(credentials)
                .withRegion(Regions.EU_WEST_3)
                .build();
    }

    void setupSQS(String sqsQueueName) {
        logger.info("Create queue {}", sqsQueueName);
        CreateQueueRequest request = new CreateQueueRequest()
                .withQueueName(sqsQueueName);
        CreateQueueResult result = sqsClient.createQueue(request);
        sqsQueueURL = result.getQueueUrl();

        GetQueueAttributesRequest qRequest = new GetQueueAttributesRequest()
                .withQueueUrl(sqsQueueURL)
                .withAttributeNames("QueueArn");

        GetQueueAttributesResult qResult = sqsClient.getQueueAttributes(qRequest);
        sqsQueueARN = qResult.getAttributes().get("QueueArn");
        logger.debug("Queue {} created with ARN {}", sqsQueueName, sqsQueueARN);

        Policy sqsPolicy =
                new Policy().withStatements(
                        new Statement(Statement.Effect.Allow)
                                .withPrincipals(Principal.AllUsers)
                                .withActions(SQSActions.SendMessage)
                                .withResources(new Resource(sqsQueueARN)));
        Map<String, String> queueAttributes = new HashMap<>();
        queueAttributes.put("Policy", sqsPolicy.toJson());
        sqsClient.setQueueAttributes(new SetQueueAttributesRequest(sqsQueueURL, queueAttributes));
    }

    void createSNS(String snsTopicName) {
        logger.info("Create topic {}", snsTopicName);
        CreateTopicRequest request = new CreateTopicRequest()
                .withName(snsTopicName);
        CreateTopicResult result = snsClient.createTopic(request);
        snsTopicARN = result.getTopicArn();
        logger.debug("Topic created with ARN {}", snsTopicName, snsTopicARN);
    }

    void setupSNS() {
        logger.info("Subscribe queue {} to topic {}", sqsQueueARN, snsTopicARN);
        SubscribeRequest request = new SubscribeRequest()
                .withTopicArn(snsTopicARN)
                .withEndpoint(sqsQueueARN)
                .withProtocol("sqs");
        SubscribeResult result = snsClient.subscribe(request);

        snsSubscriptionARN = result.getSubscriptionArn();
        logger.debug("Subscription created with ARN {}", snsSubscriptionARN);
    }

    void cleanUp() {
        snsClient.unsubscribe(new UnsubscribeRequest(snsSubscriptionARN));
        snsClient.deleteTopic(new DeleteTopicRequest(snsTopicARN));
        sqsClient.deleteQueue(new DeleteQueueRequest(sqsQueueURL));
    }

}
