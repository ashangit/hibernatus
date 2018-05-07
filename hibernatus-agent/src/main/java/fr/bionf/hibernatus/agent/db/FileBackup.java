package fr.bionf.hibernatus.agent.db;

import java.io.*;
import java.util.*;

public class FileBackup implements Serializable {
    public TreeMap<Long, AwsFile> references = new TreeMap<>();

    public void addReference(long mtime, long length, long btime, long deleteTimestamp, String awsObject) {
        references.put(btime, new AwsFile(mtime, length, btime, deleteTimestamp, awsObject));
    }

    public class AwsFile implements Serializable {
        public Long length;
        public Long modificationTimestamp;
        public Long backupTimestamp;
        public Long deleteTimestamp;
        public String awsObject;

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
