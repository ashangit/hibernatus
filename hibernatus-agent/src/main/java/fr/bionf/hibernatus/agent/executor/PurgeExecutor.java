package fr.bionf.hibernatus.agent.executor;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.glacier.AmazonGlacier;
import fr.bionf.hibernatus.agent.conf.AgentConfig;
import fr.bionf.hibernatus.agent.db.DbUtils;
import fr.bionf.hibernatus.agent.db.FileBackup;
import fr.bionf.hibernatus.agent.db.SerializationUtil;
import fr.bionf.hibernatus.agent.glacier.AmazonGlacierArchiveOperations;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static fr.bionf.hibernatus.agent.conf.AgentConfig.AGENT_BACKUP_RETENTION_KEY;

public class PurgeExecutor implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(PurgeExecutor.class);

    private final DbUtils dbUtils;
    private final AmazonGlacierArchiveOperations amazonGlacierArchiveOperations;
    private final int retention;

    public PurgeExecutor(DbUtils dbUtils, AmazonGlacier client, ProfileCredentialsProvider credentials) throws IOException {
        this.dbUtils = dbUtils;
        this.amazonGlacierArchiveOperations = new AmazonGlacierArchiveOperations(client, credentials);
        AgentConfig agentConfig = new AgentConfig();
        retention = agentConfig.getInt(AGENT_BACKUP_RETENTION_KEY);
    }

    @Override
    public void run() {
        long now = System.currentTimeMillis();
        logger.info("Start delete old backup");
        RocksIterator iterator = dbUtils.iteratorFileBackup();
        // Iterate on file to backup
        for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
            try {
                FileBackup fileBackup = (FileBackup) SerializationUtil.deserialize(iterator.value());
                fileBackup.purge(amazonGlacierArchiveOperations, dbUtils, now, retention);
            } catch (IOException | ClassNotFoundException | RocksDBException e) {
                logger.error("", e);
            }
        }
        logger.info("End delete old backup");
    }
}
