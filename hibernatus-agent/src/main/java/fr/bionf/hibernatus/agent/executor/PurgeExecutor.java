package fr.bionf.hibernatus.agent.executor;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.glacier.AmazonGlacier;
import fr.bionf.hibernatus.agent.db.DbUtils;
import fr.bionf.hibernatus.agent.db.FileBackup;
import fr.bionf.hibernatus.agent.db.SerializationUtil;
import fr.bionf.hibernatus.agent.glacier.AmazonGlacierArchiveOperations;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class PurgeExecutor implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(PurgeExecutor.class);

    private final DbUtils dbUtils;
    private final AmazonGlacierArchiveOperations amazonGlacierArchiveOperations;

    public PurgeExecutor(DbUtils dbUtils, AmazonGlacier client, ProfileCredentialsProvider credentials, String vaultName) {
        this.dbUtils = dbUtils;
        this.amazonGlacierArchiveOperations = new AmazonGlacierArchiveOperations(client, credentials, vaultName);
    }

    @Override
    public void run() {
        long now = System.currentTimeMillis();
        logger.info("Start delete old backup");
        RocksIterator iterator = dbUtils.iteratorFileBackup();
        // Iterate on file to backup
        for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
            byte[] file = iterator.key();
            try {
                FileBackup fileBackup = (FileBackup) SerializationUtil.deserialize(iterator.value());
                for (Map.Entry<Long, FileBackup.AwsFile> entry : fileBackup.references.entrySet()) {
                    FileBackup.AwsFile awsFile = entry.getValue();
                    if (awsFile.deleteTimestamp < now) {
                        logger.info("Delete backup {} for file {}", awsFile.deleteTimestamp, new String(file));
                        fileBackup.deleteReference(amazonGlacierArchiveOperations, dbUtils, entry.getKey());
                    }
                }

            } catch (IOException | ClassNotFoundException | RocksDBException e) {
                logger.error("", e);
            }
        }
        logger.info("End delete old backup");
    }
}
