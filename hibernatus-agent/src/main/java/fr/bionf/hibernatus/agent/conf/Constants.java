package fr.bionf.hibernatus.agent.conf;

import java.io.File;

public class Constants {
    /* Hidden constructor */
    protected Constants() {
    }

    public static final String SUB_DB_PATH = File.separator + "database";
    public static final String SUB_BACKUP_PATH = File.separator + "backup";

    public static final String BACKUP_PREFIX_NAME = "hibernatus_meta_backup-";

    public static final long INTERVAL_BACKUP = 1;
    public static final long INTERVAL_BACKUP_META = 1;

    public static final String AWS_PROFILE_NAME = "hibernatus";
}
