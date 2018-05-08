package fr.bionf.hibernatus.agent.db;

import fr.bionf.hibernatus.agent.glacier.AmazonGlacierArchiveOperations;
import org.rocksdb.RocksDBException;

import java.io.*;
import java.util.*;

public class FileBackup implements Serializable {
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
        dbUtils.writeFileBackup(file, SerializationUtil.serialize(this));
        references.put(now, new AwsFile(fileToTreat.mtime, fileToTreat.length, now,
                now + retention, archiveId));
    }

    public void deleteReference(AmazonGlacierArchiveOperations amazonGlacierArchiveOperations,
                                DbUtils dbUtils, long awsFileKey) throws IOException, RocksDBException {
        amazonGlacierArchiveOperations.delete(references.get(awsFileKey).awsObject);
        references.remove(awsFileKey);

        if (references.isEmpty()) {
            dbUtils.deleteFileBackup(file);
        } else {
            dbUtils.writeFileBackup(file, SerializationUtil.serialize(this));
        }

    }

    // TODO add purge references here
    public void purge() {

    }

    public class AwsFile implements Serializable {
        public Long length;
        public Long modificationTimestamp;
        Long backupTimestamp;
        public Long deleteTimestamp;
        String awsObject;

        AwsFile(long modificationTimestamp, long length, long backupTimestamp, long deleteTimestamp, String awsObject) {
            this.modificationTimestamp = modificationTimestamp;
            this.length = length;
            this.backupTimestamp = backupTimestamp;
            this.deleteTimestamp = deleteTimestamp;
            this.awsObject = awsObject;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof AwsFile)) return false;
            AwsFile awsFile = (AwsFile) o;
            return Objects.equals(length, awsFile.length) &&
                    Objects.equals(modificationTimestamp, awsFile.modificationTimestamp) &&
                    Objects.equals(backupTimestamp, awsFile.backupTimestamp) &&
                    Objects.equals(deleteTimestamp, awsFile.deleteTimestamp) &&
                    Objects.equals(awsObject, awsFile.awsObject);
        }

        @Override
        public int hashCode() {

            return Objects.hash(length, modificationTimestamp, backupTimestamp, deleteTimestamp, awsObject);
        }
    }
}
