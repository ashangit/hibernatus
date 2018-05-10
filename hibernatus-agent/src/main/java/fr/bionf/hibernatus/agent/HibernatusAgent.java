package fr.bionf.hibernatus.agent;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.glacier.AmazonGlacier;
import com.amazonaws.services.glacier.AmazonGlacierClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import fr.bionf.hibernatus.agent.conf.AgentConfig;
import fr.bionf.hibernatus.agent.executor.BackupExecutor;
import fr.bionf.hibernatus.agent.executor.BackupMetaExecutor;
import fr.bionf.hibernatus.agent.executor.ListFileExecutor;
import fr.bionf.hibernatus.agent.executor.PurgeExecutor;
import fr.bionf.hibernatus.agent.db.DbUtils;
import fr.bionf.hibernatus.agent.glacier.AmazonGlacierVaultOperations;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static fr.bionf.hibernatus.agent.conf.AgentConfig.*;
import static fr.bionf.hibernatus.agent.conf.Constants.AWS_PROFILE_NAME;
import static fr.bionf.hibernatus.agent.conf.Constants.INTERVAL_BACKUP;
import static fr.bionf.hibernatus.agent.conf.Constants.INTERVAL_BACKUP_META;

// TODO: create client to list, restore, delete
// TODO: do admin/maintenance operation on rocksDB
// TODO: backup rocksDB in AWS
//              3. send tgz to S3 (we must keep latest-2, latest-1 and latest in S3
// TODO: restore rocksDB from S3
//              1. Et db initialisation if no db available locally check on S3 in latest or latest-1 or latest-2 is available
// TODO: rocksDB read options (compression...)
// TODO: add tests

// TODO use vault inventory + rocksdb snapshot to detect diff

// TODO upload files in big archive of 256 Mo each (big tgz)

// TODO add transactions to write in rocksdb and to delete (optimistic should be sufficient)

// TODO: see to use one event loop for all treatment

// TODO use proto to bck data in rocksdb instead of java serialized object

// Each schedule will just submit a runnable in netty event loop https://netty.io/wiki/using-as-a-generic-library.html
public class HibernatusAgent {
    private static final Logger logger = LoggerFactory.getLogger(HibernatusAgent.class);
    // Use .aws/credentials file with hibernatus profile
    private final ProfileCredentialsProvider credentials = new ProfileCredentialsProvider(AWS_PROFILE_NAME);
    private final AgentConfig agentConfig = new AgentConfig();
    private static AmazonGlacier client;
    private DbUtils dbUtils;

    private HibernatusAgent() throws IOException {
    }

    private void initVault() throws IOException {
        client = AmazonGlacierClientBuilder.standard()
                .withCredentials(credentials)
                .withRegion(Regions.EU_WEST_3)
                .build();

        try {
            AmazonGlacierVaultOperations amazonGlacierVaultOperations = new AmazonGlacierVaultOperations(client, credentials);
            amazonGlacierVaultOperations.initVault();
        } catch (Exception e) {
            logger.error("Vault operation failed: ", e);
            throw e;
        }
    }

    private void initMetadata() throws IOException, RocksDBException {
        String metaPath = agentConfig.getString(AGENT_METADATA_PATH_KEY);
        dbUtils = new DbUtils(metaPath);
        try {
            dbUtils.open();
        } catch (RocksDBException e) {
            logger.error("", e);
            dbUtils.close();
            throw e;
        }
    }

    private void initListScheduler() throws IOException {
        long interval = agentConfig.getLong(AGENT_BACKUP_INTERVAL_KEY);
        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(new ListFileExecutor(dbUtils),
                0, interval, TimeUnit.SECONDS);
    }

    private void initBackupScheduler() throws IOException {
        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(
                new BackupExecutor(dbUtils, client, credentials),
                0, INTERVAL_BACKUP, TimeUnit.SECONDS);
    }

    private void initPurgeScheduler() throws IOException {
        long interval = agentConfig.getLong(AGENT_PURGE_INTERVAL_KEY);
        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(
                new PurgeExecutor(dbUtils, client, credentials),
                0, interval, TimeUnit.SECONDS);
    }

    private void initBackupMetaScheduler() throws IOException {
        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(
                new BackupMetaExecutor(dbUtils),
                0, INTERVAL_BACKUP_META, TimeUnit.DAYS);
    }

    private void close() {
        dbUtils.close();
    }

    public static void main(String[] args) throws Exception {
        HibernatusAgent hServer = new HibernatusAgent();

        // Init bck threads
        try {
            // Check/Create AWS Glacier Vault
            hServer.initVault();
            // Init metadata
            hServer.initMetadata();
            // Init list file threads
            hServer.initListScheduler();
            // Init threads reading new file to treat
            hServer.initBackupScheduler();
            // Init threads purge
            hServer.initPurgeScheduler();
            // Init backup meta thread
            hServer.initBackupMetaScheduler();

            while (true) TimeUnit.SECONDS.sleep(60);
        } finally {
            hServer.close();
        }
    }
}