package fr.bionf.hibernatus.agent.db;

import java.io.*;
import java.util.Objects;

public class FileToTreat implements Serializable {
    public Long mtime;
    public Long length;
    //boolean isTreated = false;

    public FileToTreat(long mtime, long length) {
        this.mtime = mtime;
        this.length = length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FileToTreat)) return false;
        FileToTreat that = (FileToTreat) o;
        return Objects.equals(mtime, that.mtime) &&
                Objects.equals(length, that.length);
    }

    @Override
    public int hashCode() {

        return Objects.hash(mtime, length);
    }
}
