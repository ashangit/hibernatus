package fr.bionf.hibernatus.agent.db;

import fr.bionf.hibernatus.agent.glacier.AmazonGlacierArchiveOperations;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public class FileBackup implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(FileBackup.class);
    public TreeMap<Long, AwsFile> references = new TreeMap<>();
    private final byte[] file;

    public FileBackup(byte[] file) {
        this.file = file;
    }

    public void backupReference(AmazonGlacierArchiveOperations amazonGlacierArchiveOperations,
                                DbUtils dbUtils, FileToTreat fileToTreat, long retention)
            throws IOException, RocksDBException {
        long now = System.currentTimeMillis();
        String archiveId = amazonGlacierArchiveOperations.upload(new String(file));
        references.put(now, new AwsFile(fileToTreat.mtime, fileToTreat.length, now,
                now + retention, archiveId));
        dbUtils.writeFileBackup(file, SerializationUtil.serialize(this));
    }

    void deleteReference(AmazonGlacierArchiveOperations amazonGlacierArchiveOperations,
                         DbUtils dbUtils, long awsFileKey) throws IOException, RocksDBException {
        amazonGlacierArchiveOperations.delete(references.get(awsFileKey).awsObject);
        references.remove(awsFileKey);

        if (references.isEmpty()) {
            dbUtils.deleteFileBackup(file);
        } else {
            dbUtils.writeFileBackup(file, SerializationUtil.serialize(this));
        }

    }

    public void purge(AmazonGlacierArchiveOperations amazonGlacierArchiveOperations,
                      DbUtils dbUtils, long now, long retention) throws IOException, RocksDBException {
        String fileS = new String(this.file);
        File f = new File(fileS);

        TreeMap<Long, AwsFile> iterateReferences = (TreeMap<Long, AwsFile>) references.clone();

        for (Map.Entry<Long, FileBackup.AwsFile> entry : iterateReferences.entrySet()) {
            FileBackup.AwsFile awsFile = entry.getValue();
            if (f.isFile() && f.lastModified() == awsFile.lastModified) {
                logger.info("Update delete time for {}", fileS);
                awsFile.deleteEpoch = now + retention;
            } else if (awsFile.deleteEpoch < now) {
                logger.info("Delete backup {} for file {}", awsFile.deleteEpoch, fileS);
                this.deleteReference(amazonGlacierArchiveOperations, dbUtils, entry.getKey());
            }
        }
    }

    public class AwsFile implements Serializable {
        public Long length;
        public Long lastModified;
        Long lastBackuped;
        Long deleteEpoch;
        String awsObject;

        AwsFile(long lastModified, long length, long lastBackuped, long deleteEpoch, String awsObject) {
            this.lastModified = lastModified;
            this.length = length;
            this.lastBackuped = lastBackuped;
            this.deleteEpoch = deleteEpoch;
            this.awsObject = awsObject;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof AwsFile)) return false;
            AwsFile awsFile = (AwsFile) o;
            return Objects.equals(length, awsFile.length) &&
                    Objects.equals(lastModified, awsFile.lastModified) &&
                    Objects.equals(lastBackuped, awsFile.lastBackuped) &&
                    Objects.equals(deleteEpoch, awsFile.deleteEpoch) &&
                    Objects.equals(awsObject, awsFile.awsObject);
        }

        @Override
        public int hashCode() {

            return Objects.hash(length, lastModified, lastBackuped, deleteEpoch, awsObject);
        }
    }
}
