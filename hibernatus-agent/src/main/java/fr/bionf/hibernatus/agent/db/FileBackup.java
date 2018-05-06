package fr.bionf.hibernatus.agent.db;

import java.io.*;
import java.util.*;

public class FileBackup implements Serializable {
    public TreeMap<Long, AwsFile> references = new TreeMap<>();

    public void addReference(long mtime, long length, long btime, String awsObject) {
        references.put(btime, new AwsFile(mtime, length, btime, awsObject));
    }

    public class AwsFile implements Serializable {
        public Long mtime;
        public Long length;
        public Long btime;
        public String awsObject;

        AwsFile(long mtime, long length, long btime, String awsObject) {
            this.mtime = mtime;
            this.length = length;
            this.btime = btime;
            this.awsObject = awsObject;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof AwsFile)) return false;
            AwsFile awsFile = (AwsFile) o;
            return Objects.equals(mtime, awsFile.mtime) &&
                    Objects.equals(length, awsFile.length) &&
                    Objects.equals(btime, awsFile.btime) &&
                    Objects.equals(awsObject, awsFile.awsObject);
        }

        @Override
        public int hashCode() {
            return Objects.hash(mtime, length, btime, awsObject);
        }

    }
}
