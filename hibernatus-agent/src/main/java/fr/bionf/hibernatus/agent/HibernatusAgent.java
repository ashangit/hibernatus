package fr.bionf.hibernatus.agent;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.glacier.AmazonGlacier;
import com.amazonaws.services.glacier.AmazonGlacierClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import fr.bionf.hibernatus.agent.executor.BackupExecutor;
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

// TODO: create client to list, restore, delete
// TODO: do admin/maintenance operation on rocksDB
// TODO: backup rocksDB in AWS
// TODO: rocksDB read options (compression...)
// TODO: add tests

// TODO use vault inventory + rocksdb snapshot to detect diff

// TODO upload files in big archive of 256 Mo each (big tgz)

// TODO add transactions to write in rocksdb and to delete (optimistic should be sufficient)

// TODO: see to use one event loop for all treatment
// Each schedule will just submit a runnable in netty event loop https://netty.io/wiki/using-as-a-generic-library.html
public class HibernatusAgent {
    private static final Logger logger = LoggerFactory.getLogger(ListFileExecutor.class);
    // Use .aws/credentials file with hibernatus profile
    private final ProfileCredentialsProvider credentials = new ProfileCredentialsProvider("hibernatus");
    private final String vaultName = "hibernatus-" + InetAddress.getLocalHost().getHostName();
    private final HashMap config;
    private static AmazonGlacier client;
    private DbUtils dbUtils;

    private HibernatusAgent(HashMap config) throws UnknownHostException {
        this.config = config;
    }

    private void initVault() {
        client = AmazonGlacierClientBuilder.standard()
                .withCredentials(credentials)
                .withRegion(Regions.EU_WEST_3)
                .build();

        try {
            AmazonGlacierVaultOperations amazonGlacierVaultOperations = new AmazonGlacierVaultOperations(client, credentials);
            amazonGlacierVaultOperations.initVault(vaultName);
        } catch (Exception e) {
            logger.error("Vault operation failed: ", e);
            throw e;
        }
    }

    private void initMetadata() throws IOException, RocksDBException {
        String dbPath = (String) config.get("metadata.path");
        dbUtils = new DbUtils(dbPath);
        try {
            dbUtils.open();
        } catch (RocksDBException | IOException e) {
            logger.error("", e);
            dbUtils.close();
            throw e;
        }
    }

    private void initListScheduler() {
        long interval = ((Number) config.get("backup.interval")).longValue();
        ArrayList<String> folders = (ArrayList<String>) config.get("backup.folders");
        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(new ListFileExecutor(dbUtils, folders),
                0, interval, TimeUnit.SECONDS);
    }

    private void initBackupScheduler() {
        long INTERVAL_BACKUP = 5;
        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(
                new BackupExecutor(dbUtils, client, credentials, vaultName),
                0, INTERVAL_BACKUP, TimeUnit.SECONDS);
    }

    private void initPurgeScheduler() {
        long interval = ((Number) config.get("purge.interval")).longValue();
        int retention = (int) config.get("purge.retention");
        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(new PurgeExecutor(dbUtils, retention),
                0, interval, TimeUnit.SECONDS);
    }

    private void close() throws RocksDBException {
        dbUtils.close();
    }

    public static void main(String[] args) throws Exception {
        // Load config
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        HashMap config = new HashMap();
        try (InputStream streamPropFilePath = HibernatusAgent.class.getResourceAsStream("/config.yml")) {
            config.putAll(mapper.readValue(streamPropFilePath, HashMap.class));
        } catch (Exception e) {
            logger.error("Issue reading config file");
            System.exit(1);
        }

        HibernatusAgent hServer = new HibernatusAgent(config);

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

            while (true) TimeUnit.SECONDS.sleep(60);
        } finally {
            hServer.close();
        }
    }
}