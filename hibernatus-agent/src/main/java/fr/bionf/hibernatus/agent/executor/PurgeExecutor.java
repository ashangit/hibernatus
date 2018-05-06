package fr.bionf.hibernatus.agent.executor;

import fr.bionf.hibernatus.agent.db.DbUtils;
import fr.bionf.hibernatus.agent.db.FileBackup;
import fr.bionf.hibernatus.agent.db.SerializationUtil;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PurgeExecutor implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(PurgeExecutor.class);

    private final DbUtils dbUtils;
    private final long retention;

    public PurgeExecutor(DbUtils dbUtils, int retention) {
        this.dbUtils = dbUtils;
        this.retention = TimeUnit.DAYS.toMillis(retention);
    }

    @Override
    public void run() {
        long olderEpoch = System.currentTimeMillis() - retention;
        logger.info("Start delete old backup");
        RocksIterator iterator = dbUtils.iteratorFileBackup();
        // Iterate on file to backup
        for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
            byte[] file = iterator.key();
            try {
                FileBackup fileBackup = (FileBackup) SerializationUtil.deserialize(iterator.value());
                for (Map.Entry<Long, FileBackup.AwsFile> entry : fileBackup.references.entrySet()) {
                    FileBackup.AwsFile awsFile = entry.getValue();
                    if (awsFile.btime < olderEpoch) {
                        logger.info("Delete backup {} for file {}", awsFile.btime, new String(file));
                        // TODO delete aws file

                        fileBackup.references.remove(entry.getKey());

                        if (fileBackup.references.size() == 0) {
                            dbUtils.deleteFileBackup(file);
                        } else {
                            dbUtils.writeFileBackup(file, SerializationUtil.serialize(fileBackup));
                        }
                    }
                }

            } catch (IOException | ClassNotFoundException | RocksDBException e) {
                logger.error("", e);
            }
        }
        logger.info("End delete old backup");
    }
}
