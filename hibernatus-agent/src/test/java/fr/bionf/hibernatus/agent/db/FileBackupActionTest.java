package fr.bionf.hibernatus.agent.db;

import fr.bionf.hibernatus.agent.glacier.AmazonGlacierArchiveOperations;
import fr.bionf.hibernatus.agent.junit.rules.WithDbUtils;
import fr.bionf.hibernatus.agent.proto.Db;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class FileBackupActionTest {
    @Rule
    public WithDbUtils withDbUtils = new WithDbUtils();

    @Rule
    public TemporaryFolder rootDbFolder = new TemporaryFolder();
    private String archiveId = "archive_to_delete";
    private Db.FileToTreat fileToTreat;
    private long retention = TimeUnit.DAYS.toMillis(-1L);
    private FileBackupAction fileBackupAction;

    private AmazonGlacierArchiveOperations amazonGlacierArchiveOperations;
    private DbUtils dbUtils;

    @Before
    public void setUp() throws IOException, RocksDBException, InterruptedException {
        File f = rootDbFolder.newFile("testfile");
        fileBackupAction = new FileBackupAction(f.getAbsolutePath());

        amazonGlacierArchiveOperations = mock(AmazonGlacierArchiveOperations.class);
        when(amazonGlacierArchiveOperations.upload(fileBackupAction.getFileBackupBuilder().getFilename())).thenReturn(archiveId);
        dbUtils = withDbUtils.getDbUtils();

        fileToTreat = Db.FileToTreat.newBuilder().setFilename(f.getAbsolutePath())
                .setLength(1L).setMtime(1L).build();
        fileBackupAction.backupReference(amazonGlacierArchiveOperations, dbUtils, fileToTreat, retention);
        Thread.sleep(10L);
        fileToTreat = Db.FileToTreat.newBuilder().setFilename(f.getAbsolutePath())
                .setLength(f.length()).setMtime(f.lastModified()).build();
        fileBackupAction.backupReference(amazonGlacierArchiveOperations, dbUtils, fileToTreat, retention);
    }

    @Test
    public void should_backup_new_file() throws IOException, RocksDBException {
        verify(dbUtils, times(2)).writeFileBackup(any(byte[].class), any(byte[].class));

        String archiveId = "archive_id";


        FileBackupAction fileBackupAction = new FileBackupAction("/tmp/testfile.tmp");
        when(amazonGlacierArchiveOperations.upload(fileBackupAction.getFileBackupBuilder().getFilename())).thenReturn("archive_id");
        fileBackupAction.backupReference(amazonGlacierArchiveOperations, dbUtils, fileToTreat, retention);

        assertEquals(archiveId, fileBackupAction.getReferences().firstEntry().getValue().getAwsObject());

        verify(amazonGlacierArchiveOperations).upload(fileBackupAction.getFileBackupBuilder().getFilename());
        verify(dbUtils, times(3)).writeFileBackup(any(byte[].class), any(byte[].class));

        byte[] fileBackupValGet = dbUtils.getFileBackup(fileBackupAction.getFileName());
        FileBackupAction fileBackupedGet = new FileBackupAction(Db.FileBackup.parseFrom(fileBackupValGet));
        assertEquals(fileBackupAction.getReferences().firstEntry().getValue(), fileBackupedGet.getReferences().firstEntry().getValue());
    }

    @Test
    public void should_remove_file_in_archive() throws IOException, RocksDBException {
        verify(dbUtils, times(2)).writeFileBackup(any(byte[].class), any(byte[].class));
        fileBackupAction.deleteReference(amazonGlacierArchiveOperations, dbUtils,
                fileBackupAction.getReferences().firstEntry().getKey());
        verify(amazonGlacierArchiveOperations).delete(archiveId);
        verify(dbUtils, times(3)).writeFileBackup(any(byte[].class), any(byte[].class));

        byte[] fileBackupValGet = dbUtils.getFileBackup(fileBackupAction.getFileName());
        Db.FileBackup fileBackupedGet = Db.FileBackup.parseFrom(fileBackupValGet);
        assertEquals(1, fileBackupedGet.getReferencesMap().size());

        fileBackupAction.deleteReference(amazonGlacierArchiveOperations, dbUtils,
                fileBackupAction.getReferences().firstEntry().getKey());
        verify(dbUtils).deleteFileBackup(fileBackupAction.getFileName());
    }

    @Test
    public void should_purge_old_backup_of_a_file() throws RocksDBException {
        assertEquals(2, fileBackupAction.getFileBackupBuilder().getReferencesMap().size());
        fileBackupAction.purge(amazonGlacierArchiveOperations, dbUtils, System.currentTimeMillis(), retention);
        assertEquals(1, fileBackupAction.getFileBackupBuilder().getReferencesMap().size());
    }
}