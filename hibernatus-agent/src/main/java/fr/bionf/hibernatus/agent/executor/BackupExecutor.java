package fr.bionf.hibernatus.agent.executor;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.glacier.AmazonGlacier;
import fr.bionf.hibernatus.agent.db.DbUtils;
import fr.bionf.hibernatus.agent.db.FileBackup;
import fr.bionf.hibernatus.agent.db.FileToTreat;
import fr.bionf.hibernatus.agent.db.SerializationUtil;
import fr.bionf.hibernatus.agent.glacier.AmazonGlacierArchiveOperations;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class BackupExecutor implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(BackupExecutor.class);

    private final DbUtils dbUtils;
    private final AmazonGlacierArchiveOperations amazonGlacierArchiveOperations;
    private final String vaultName;
    private final long retention;

    public BackupExecutor(DbUtils dbUtils, AmazonGlacier client, ProfileCredentialsProvider credentials, String vaultName, long retention) {
        this.dbUtils = dbUtils;
        this.amazonGlacierArchiveOperations = new AmazonGlacierArchiveOperations(client, credentials);
        this.vaultName = vaultName;
        this.retention = TimeUnit.DAYS.toMillis(retention);
    }

    private void backup(byte[] file, FileToTreat fileToTreat, FileBackup fileBackuped) throws IOException, RocksDBException {
        logger.info("Backup file {}", new String(file));
        if (fileBackuped == null) {
            fileBackuped = new FileBackup();
        }
        String archiveId = amazonGlacierArchiveOperations.upload("examplevaultfordelete", new String(file));
        long now = System.currentTimeMillis();
        fileBackuped.addReference(
                fileToTreat.mtime,
                fileToTreat.length,
                now,
                now + retention,
                archiveId);
        dbUtils.writeFileBackup(file, SerializationUtil.serialize(fileBackuped));
    }

    @Override
    public void run() {
        logger.info("Start backup new/modifies files");
        RocksIterator iterator = dbUtils.iteratorFileToTreat();
        // Iterate on file to backup
        for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
            byte[] file = iterator.key();
            //if (!key.startsWith(""))
            //    break;

            try {
                FileToTreat fileToTreat = (FileToTreat) SerializationUtil.deserialize(iterator.value());
                byte[] value = dbUtils.getFileBackup(file);

                // Check if file already backup
                if (value == null) {
                    backup(file, fileToTreat, null);
                } else {
                    // Check if file has been modified
                    FileBackup fileBackuped = (FileBackup) SerializationUtil.deserialize(value);
                    FileBackup.AwsFile awsFile = (FileBackup.AwsFile) fileBackuped.references
                            .get(fileBackuped.references.lastKey());
                    if (!awsFile.mtime.equals(fileToTreat.mtime) || !awsFile.length.equals(fileToTreat.length)) {
                        backup(file, fileToTreat, fileBackuped);
                    }
                }
            } catch (ClassNotFoundException | IOException | RocksDBException e) {
                logger.error("", e);
            }

            dbUtils.deleteFileToTreat(file);
        }
        logger.info("End backup new/modifies files");
    }
}
