package fr.bionf.hibernatus.agent.glacier;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.glacier.AmazonGlacier;
import com.amazonaws.services.glacier.model.*;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import fr.bionf.hibernatus.agent.conf.AgentConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;

import static fr.bionf.hibernatus.agent.conf.AgentConfig.AGENT_BACKUP_VAULT_NAME;


public class AmazonGlacierVaultOperations {
    private static final Logger logger = LoggerFactory.getLogger(AmazonGlacierVaultOperations.class);

    private final AgentConfig agentConfig = new AgentConfig();
    private final String vaultName = agentConfig.getString(AGENT_BACKUP_VAULT_NAME);
    private final AmazonGlacierSnsSqsOperations amazonGlacierSNSOperations;

    private AmazonGlacier client;


    public AmazonGlacierVaultOperations(AmazonGlacier client, AWSCredentialsProvider credentials) throws IOException {
        this.client = client;
        amazonGlacierSNSOperations = new AmazonGlacierSnsSqsOperations(credentials);
    }

    public void initVault() {
        createVault();
        setVaultNotifications();
    }

    private void createVault() {
        CreateVaultRequest createVaultRequest = new CreateVaultRequest()
                .withVaultName(vaultName);
        CreateVaultResult createVaultResult = client.createVault(createVaultRequest);

        logger.info("Created vault successfully: " + createVaultResult.getLocation());
    }

    private void setVaultNotifications() {
        String snsTopicName = vaultName + "-sns";
        amazonGlacierSNSOperations.createSNS(snsTopicName);

        VaultNotificationConfig config = new VaultNotificationConfig()
                .withSNSTopic(amazonGlacierSNSOperations.snsTopicARN)
                .withEvents("ArchiveRetrievalCompleted", "InventoryRetrievalCompleted");

        SetVaultNotificationsRequest request = new SetVaultNotificationsRequest()
                .withVaultName(vaultName)
                .withVaultNotificationConfig(config);

        client.setVaultNotifications(request);
        logger.info("Notification configured for vault: " + vaultName);
    }

    public void getVaultNotifications() {
        VaultNotificationConfig notificationConfig = null;
        GetVaultNotificationsRequest request = new GetVaultNotificationsRequest()
                .withVaultName(vaultName);
        GetVaultNotificationsResult result = client.getVaultNotifications(request);
        notificationConfig = result.getVaultNotificationConfig();

        logger.info("Notifications configuration for vault: "
                + vaultName);
        logger.info("Topic: " + notificationConfig.getSNSTopic());
        logger.info("Events: " + notificationConfig.getEvents());
    }

    public void describeVault() {
        DescribeVaultRequest describeVaultRequest = new DescribeVaultRequest()
                .withVaultName(vaultName);
        DescribeVaultResult describeVaultResult = client.describeVault(describeVaultRequest);

        logger.info("Describing the vault: " + vaultName);
        logger.info(
                "CreationDate: " + describeVaultResult.getCreationDate() +
                        "\nLastInventoryDate: " + describeVaultResult.getLastInventoryDate() +
                        "\nNumberOfArchives: " + describeVaultResult.getNumberOfArchives() +
                        "\nSizeInBytes: " + describeVaultResult.getSizeInBytes() +
                        "\nVaultARN: " + describeVaultResult.getVaultARN() +
                        "\nVaultName: " + describeVaultResult.getVaultName());
    }

    public void listVaults() {
        ListVaultsRequest listVaultsRequest = new ListVaultsRequest();
        ListVaultsResult listVaultsResult = client.listVaults(listVaultsRequest);

        List<DescribeVaultOutput> vaultList = listVaultsResult.getVaultList();
        logger.info("\nDescribing all vaults (vault list):");
        for (DescribeVaultOutput vault : vaultList) {
            logger.info(
                    "\nCreationDate: " + vault.getCreationDate() +
                            "\nLastInventoryDate: " + vault.getLastInventoryDate() +
                            "\nNumberOfArchives: " + vault.getNumberOfArchives() +
                            "\nSizeInBytes: " + vault.getSizeInBytes() +
                            "\nVaultARN: " + vault.getVaultARN() +
                            "\nVaultName: " + vault.getVaultName());
        }
    }

    public void deleteVault() {
        amazonGlacierSNSOperations.cleanUp();
        DeleteVaultRequest request = new DeleteVaultRequest()
                .withVaultName(vaultName);
        client.deleteVault(request);
        logger.info("Deleted vault: " + vaultName);
    }

    public void downloadInventory() throws Exception {
        String sqsQueueName = vaultName + "-sqs";
        amazonGlacierSNSOperations.setupSQS(sqsQueueName);

        String snsTopicName = vaultName + "-sns";
        amazonGlacierSNSOperations.setupSNS();

        String jobId = initiateJobRequest();
        logger.info("Jobid = " + jobId);

        Boolean success = waitForJobToComplete(jobId, amazonGlacierSNSOperations.sqsQueueURL);
        if (!success) {
            throw new Exception("Job did not complete successfully.");
        }

        downloadJobOutput(jobId);

        amazonGlacierSNSOperations.cleanUp();
    }

    private String initiateJobRequest() {

        JobParameters jobParameters = new JobParameters()
                .withType("inventory-retrieval")
                .withSNSTopic(amazonGlacierSNSOperations.snsTopicARN);

        InitiateJobRequest request = new InitiateJobRequest()
                .withVaultName(vaultName)
                .withJobParameters(jobParameters);

        InitiateJobResult response = client.initiateJob(request);

        return response.getJobId();
    }

    private Boolean waitForJobToComplete(String jobId, String sqsQueueUrl) throws InterruptedException, IOException {

        Boolean messageFound = false;
        Boolean jobSuccessful = false;
        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();

        while (!messageFound) {
            List<Message> msgs = amazonGlacierSNSOperations.sqsClient.receiveMessage(
                    new ReceiveMessageRequest(sqsQueueUrl).withMaxNumberOfMessages(10)).getMessages();

            if (msgs.size() > 0) {
                for (Message m : msgs) {
                    JsonParser jpMessage = factory.createParser(m.getBody());
                    JsonNode jobMessageNode = mapper.readTree(jpMessage);
                    String jobMessage = jobMessageNode.get("Message").textValue();

                    JsonParser jpDesc = factory.createParser(jobMessage);
                    JsonNode jobDescNode = mapper.readTree(jpDesc);
                    String retrievedJobId = jobDescNode.get("JobId").textValue();
                    String statusCode = jobDescNode.get("StatusCode").textValue();
                    if (retrievedJobId.equals(jobId)) {
                        messageFound = true;
                        if (statusCode.equals("Succeeded")) {
                            jobSuccessful = true;
                        }
                    }
                }

            } else {
                long sleepTime = 600;
                Thread.sleep(sleepTime * 1000);
            }
        }
        return (jobSuccessful);
    }

    private void downloadJobOutput(String jobId) throws IOException {

        GetJobOutputRequest getJobOutputRequest = new GetJobOutputRequest()
                .withVaultName(vaultName)
                .withJobId(jobId);
        GetJobOutputResult getJobOutputResult = client.getJobOutput(getJobOutputRequest);

        String fileName = "/tmp/" + vaultName;
        FileWriter fstream = new FileWriter(fileName);
        BufferedWriter out = new BufferedWriter(fstream);
        BufferedReader in = new BufferedReader(new InputStreamReader(getJobOutputResult.getBody()));
        String inputLine;
        try {
            while ((inputLine = in.readLine()) != null) {
                out.write(inputLine);
            }
        } catch (IOException e) {
            throw new AmazonClientException("Unable to save archive", e);
        } finally {
            try {
                in.close();
            } catch (Exception ignored) {
            }
            try {
                out.close();
            } catch (Exception ignored) {
            }
        }
        logger.info("Retrieved inventory to " + fileName);
    }

}
