package fr.bionf.hibernatus.agent.db;

import fr.bionf.hibernatus.agent.glacier.AmazonGlacierArchiveOperations;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class FileBackupTest {
    private byte[] file = "/tmp/testfile".getBytes();
    private String archiveId = "archive_to_delete";
    private FileBackup fileBackup;

    private AmazonGlacierArchiveOperations amazonGlacierArchiveOperations;
    private DbUtils dbUtils;

    @Before
    public void setUp() throws IOException, RocksDBException, InterruptedException {
        fileBackup = new FileBackup(file);

        amazonGlacierArchiveOperations = mock(AmazonGlacierArchiveOperations.class);
        when(amazonGlacierArchiveOperations.upload(new String(file))).thenReturn(archiveId);
        dbUtils = mock(DbUtils.class);

        FileToTreat fileToTreat = new FileToTreat(1L, 1L);
        long retention = TimeUnit.DAYS.toMillis(1L);
        fileBackup.backupReference(amazonGlacierArchiveOperations, dbUtils, fileToTreat, retention);
        Thread.sleep(10L);
        fileBackup.backupReference(amazonGlacierArchiveOperations, dbUtils, fileToTreat, retention);
    }

    @Test
    public void should_backup_new_file() throws IOException, RocksDBException {
        verify(dbUtils, times(2)).writeFileBackup(any(byte[].class), any(byte[].class));

        FileToTreat fileToTreat = new FileToTreat(1L, 1L);
        long retention = TimeUnit.DAYS.toMillis(1L);
        String archiveId = "archive_id";

        byte[] file = "/tmp/testfile.tmp".getBytes();
        when(amazonGlacierArchiveOperations.upload(new String(file))).thenReturn("archive_id");

        FileBackup fileBackup = new FileBackup(file);
        fileBackup.backupReference(amazonGlacierArchiveOperations, dbUtils, fileToTreat, retention);

        assertEquals(archiveId, fileBackup.references.firstEntry().getValue().awsObject);

        verify(amazonGlacierArchiveOperations).upload(new String(file));
        verify(dbUtils, times(3)).writeFileBackup(any(byte[].class), any(byte[].class));
    }

    @Test
    public void should_remove_file_in_archive() throws IOException, RocksDBException {
        verify(dbUtils, times(2)).writeFileBackup(any(byte[].class), any(byte[].class));
        fileBackup.deleteReference(amazonGlacierArchiveOperations, dbUtils, fileBackup.references.firstEntry().getKey());
        verify(amazonGlacierArchiveOperations).delete(archiveId);
        verify(dbUtils, times(3)).writeFileBackup(any(byte[].class), any(byte[].class));

        fileBackup.deleteReference(amazonGlacierArchiveOperations, dbUtils, fileBackup.references.firstEntry().getKey());
        verify(dbUtils).deleteFileBackup(file);
    }
}