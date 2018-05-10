package fr.bionf.hibernatus.agent.executor;

import fr.bionf.hibernatus.agent.conf.AgentConfig;
import fr.bionf.hibernatus.agent.db.DbUtils;
import fr.bionf.hibernatus.agent.utils.TarFolder;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

import static fr.bionf.hibernatus.agent.conf.AgentConfig.AGENT_METADATA_PATH_KEY;
import static fr.bionf.hibernatus.agent.conf.Constants.BACKUP_PREFIX_NAME;

public class BackupMetaExecutor implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(BackupMetaExecutor.class);

    private final DbUtils dbUtils;
    private final String metaPath;
    public TarFolder tarFolder;

    public BackupMetaExecutor(DbUtils dbUtils) throws IOException {
        AgentConfig agentConfig = new AgentConfig();
        this.dbUtils = dbUtils;
        this.metaPath = agentConfig.getString(AGENT_METADATA_PATH_KEY);
        this.tarFolder = new TarFolder(dbUtils.getBackupPath(), metaPath + File.separator + BACKUP_PREFIX_NAME +
                InetAddress.getLocalHost().getHostName() + ".tbz2");
    }

    @Override
    public void run() {
        logger.info("Backup metadata");
        try {
            dbUtils.backup();
            tarFolder.compress();
            // TODO bck in S3
        } catch (RocksDBException | IOException e) {
            logger.error("", e);
        }
    }
}
