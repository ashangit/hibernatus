package fr.bionf.hibernatus.agent.executor;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.glacier.AmazonGlacier;
import fr.bionf.hibernatus.agent.conf.AgentConfig;
import fr.bionf.hibernatus.agent.db.DbUtils;
import fr.bionf.hibernatus.agent.db.FileBackupAction;
import fr.bionf.hibernatus.agent.glacier.AmazonGlacierArchiveOperations;
import fr.bionf.hibernatus.agent.proto.Db;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static fr.bionf.hibernatus.agent.conf.AgentConfig.AGENT_BACKUP_RETENTION_KEY;

public class BackupExecutor implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(BackupExecutor.class);

    private final DbUtils dbUtils;
    private final AmazonGlacierArchiveOperations amazonGlacierArchiveOperations;
    private final long retention;

    public BackupExecutor(DbUtils dbUtils, AmazonGlacier client, ProfileCredentialsProvider credentials) throws IOException {
        this.dbUtils = dbUtils;
        this.amazonGlacierArchiveOperations = new AmazonGlacierArchiveOperations(client, credentials);
        AgentConfig agentConfig = new AgentConfig();
        this.retention = TimeUnit.DAYS.toMillis(agentConfig.getLong(AGENT_BACKUP_RETENTION_KEY));
    }

    private void backup(Db.FileToTreat fileToTreat, FileBackupAction fileBackuped) throws IOException, RocksDBException {
        logger.info("Backup file {}", fileToTreat.getFilename());
        fileBackuped.backupReference(
                amazonGlacierArchiveOperations,
                dbUtils,
                fileToTreat,
                retention);
    }

    @Override
    public void run() {
        logger.info("Backup files to glacier");
        RocksIterator iterator = dbUtils.iteratorFileToTreat();
        // Iterate on file to backup
        for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
            byte[] file = iterator.key();
            try {
                Db.FileToTreat fileToTreat = Db.FileToTreat.parseFrom(iterator.value());
                byte[] value = dbUtils.getFileBackup(file);

                // Check if file already backup
                if (value == null) {
                    backup(fileToTreat, new FileBackupAction(fileToTreat.getFilename()));
                } else {
                    // Check if file has been modified
                    FileBackupAction fileBackuped = new FileBackupAction(Db.FileBackup.parseFrom(value));
                    Db.FileBackup.AwsFile awsFile = fileBackuped.getReferences().get(fileBackuped.getReferences().lastKey());
                    if (awsFile.getLastModified() != fileToTreat.getMtime() || awsFile.getLength() != fileToTreat.getLength()) {
                        backup(fileToTreat, fileBackuped);
                    }
                }
            } catch (IOException | RocksDBException e) {
                logger.error("", e);
            }

            dbUtils.deleteFileToTreat(file);
        }
        logger.debug("End backup new/modifies files");
    }
}
