package fr.bionf.hibernatus.agent.db;

import fr.bionf.hibernatus.agent.glacier.AmazonGlacierArchiveOperations;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FileBackupTest {
    private FileBackup fileBackup;

    private AmazonGlacierArchiveOperations amazonGlacierArchiveOperations;
    private DbUtils dbUtils;

    @Before
    public void setUp() {
        fileBackup = new FileBackup();

        amazonGlacierArchiveOperations = mock(AmazonGlacierArchiveOperations.class);
        dbUtils = mock(DbUtils.class);
    }

    @Test
    public void should_backup_new_file() throws IOException, RocksDBException {
        byte[] file = "/tmp/testfile".getBytes();
        FileToTreat fileToTreat = new FileToTreat(1L, 1L);
        long retention = TimeUnit.DAYS.toMillis(1L);
        String vaultName = "exempleVault";
        String archiveId = "archive_id";

        when(amazonGlacierArchiveOperations.upload(vaultName, new String(file))).thenReturn("archive_id");

        fileBackup.backupReference(amazonGlacierArchiveOperations, dbUtils, file,
                fileToTreat, retention, vaultName);

        assertEquals(archiveId, fileBackup.references.firstEntry().getValue().awsObject);

        verify(amazonGlacierArchiveOperations).upload(vaultName, new String(file));
        verify(dbUtils).writeFileBackup(any(byte[].class), any(byte[].class));
    }
}