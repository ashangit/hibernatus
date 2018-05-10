package fr.bionf.hibernatus.agent.glacier;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.glacier.AmazonGlacier;
import com.amazonaws.services.glacier.model.DeleteArchiveRequest;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManager;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManagerBuilder;
import com.amazonaws.services.glacier.transfer.UploadResult;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sqs.AmazonSQS;
import fr.bionf.hibernatus.agent.conf.AgentConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Date;

import static fr.bionf.hibernatus.agent.conf.AgentConfig.AGENT_METADATA_PATH_KEY;


public class AmazonGlacierArchiveOperations {
    private static final Logger logger = LoggerFactory.getLogger(AmazonGlacierArchiveOperations.class);

    private final ArchiveTransferManager atm;
    private final AmazonGlacier client;

    private final AgentConfig agentConfig = new AgentConfig();
    private final String vaultName = agentConfig.getString(AGENT_METADATA_PATH_KEY);

    public AmazonGlacierArchiveOperations(AmazonGlacier client, AWSCredentialsProvider credentials) throws IOException {
        this.client = client;

        AmazonGlacierSnsSqsOperations amazonGlacierSnsSqsOperations = new AmazonGlacierSnsSqsOperations(credentials);
        AmazonSNS snsClient = amazonGlacierSnsSqsOperations.snsClient;
        AmazonSQS sqsClient = amazonGlacierSnsSqsOperations.sqsClient;

        atm = new ArchiveTransferManagerBuilder()
                .withGlacierClient(client)
                .withSnsClient(snsClient)
                .withSqsClient(sqsClient)
                .build();
    }

    public String upload(String archiveToUpload) throws FileNotFoundException {
        try {
            logger.debug("Upload {}", archiveToUpload);
            UploadResult result = atm.upload(vaultName, "my archive " + (new Date()), new File(archiveToUpload));
            System.out.println("Archive ID: " + result.getArchiveId());
            return result.getArchiveId();
        } catch (Exception e) {
            logger.error("", e);
            throw e;
        }
    }

    public void deleteArchive(String archiveId) {
        client.deleteArchive(new DeleteArchiveRequest()
                .withVaultName(vaultName)
                .withArchiveId(archiveId));

        System.out.println("Deleted archive successfully.");
    }

    public void delete(String archiveId) {
        try {
            client.deleteArchive(new DeleteArchiveRequest()
                    .withVaultName(vaultName)
                    .withArchiveId(archiveId));
        } catch (Exception e) {
            logger.error("", e);
            throw e;
        }
    }

}
