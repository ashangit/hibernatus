package fr.bionf.hibernatus.agent.db;

import java.io.*;

public class FileToTreat implements Serializable {
    public Long mtime;
    public Long length;
    //boolean isTreated = false;

    public FileToTreat(long mtime, long length) {
        this.mtime = mtime;
        this.length = length;
    }
}
