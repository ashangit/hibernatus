package fr.bionf.hibernatus.agent.db;

import fr.bionf.hibernatus.agent.Constants;
import fr.bionf.hibernatus.agent.glacier.AmazonGlacierArchiveOperations;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.Mockito.mock;

public class DbUtilsTest {
    @Rule
    public TemporaryFolder rootDbFolder = new TemporaryFolder();
    private File dbFolder;
    private File backupFolder;

    private long retention = TimeUnit.DAYS.toMillis(1L);
    private String vaultName = "exempleVault";

    private DbUtils dbUtils;
    private AmazonGlacierArchiveOperations amazonGlacierArchiveOperations;

    private void writeData(long from, long to) throws IOException, RocksDBException {
        for (long i = from; i <= to; i++) {
            byte[] fileId = String.valueOf(i).getBytes();
            FileToTreat fileToTreat = new FileToTreat(i, i);
            dbUtils.writeFileToTreat(fileId, SerializationUtil.serialize(fileToTreat));
            FileBackup fileBackuped = new FileBackup();
            fileBackuped.backupReference(amazonGlacierArchiveOperations, dbUtils, fileId,
                    fileToTreat, retention, vaultName);
            dbUtils.writeFileBackup(fileId, SerializationUtil.serialize(fileBackuped));
        }
    }

    @Before
    public void setUp() throws IOException, RocksDBException {
        amazonGlacierArchiveOperations = mock(AmazonGlacierArchiveOperations.class);

        dbFolder = new File(rootDbFolder.getRoot().getAbsolutePath()
                + Constants.SUB_DB_PATH);
        backupFolder = new File(rootDbFolder.getRoot().getAbsolutePath()
                + Constants.SUB_BACKUP_PATH);

        dbUtils = new DbUtils(rootDbFolder.getRoot().getAbsolutePath());
        dbUtils.open();

        // Write few data
        writeData(0L, 10L);
    }

    @Test
    public void dbAndBackupFolderHaveBeenCreated() {
        assertTrue(Files.isDirectory(rootDbFolder.getRoot().toPath()));
        assertTrue(Files.isDirectory(dbFolder.toPath()));
        assertTrue(Files.isDirectory(backupFolder.toPath()));
    }

    @Test
    public void shouldCreateAFileToTreatAndIterateAndDelete() throws IOException, RocksDBException, ClassNotFoundException {
        RocksIterator iterator = dbUtils.iteratorFileToTreat();
        int nbKey = 0;
        for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
            nbKey++;
        }
        assertEquals(11, nbKey);

        byte[] fileId = "11".getBytes();
        FileToTreat fileToTreat = new FileToTreat(1L, 1L);

        dbUtils.writeFileToTreat(fileId, SerializationUtil.serialize(new FileToTreat(1L, 1L)));

        FileToTreat fileToTreatGet = (FileToTreat) SerializationUtil.deserialize(dbUtils.getFileToTreat(fileId));
        assertEquals(fileToTreat, fileToTreatGet);

        iterator = dbUtils.iteratorFileToTreat();
        nbKey = 0;
        for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
            nbKey++;
        }
        assertEquals(12, nbKey);

        //delete
        dbUtils.deleteFileToTreat(fileId);
        assertNull(dbUtils.getFileToTreat(fileId));
    }

    @Test
    public void shouldCreateAFileBackupAndIterateAndDelete() throws IOException, RocksDBException, ClassNotFoundException {
        RocksIterator iterator = dbUtils.iteratorFileBackup();
        int nbKey = 0;
        for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
            nbKey++;
        }
        assertEquals(11, nbKey);

        byte[] fileId = "11".getBytes();
        FileBackup fileBackuped = new FileBackup();
        FileToTreat fileToTreat = new FileToTreat(11L, 11L);
        fileBackuped.backupReference(amazonGlacierArchiveOperations, dbUtils, fileId,
                fileToTreat, retention, vaultName);
        byte[] fileBackupVal = SerializationUtil.serialize(fileBackuped);
        dbUtils.writeFileBackup(fileId, fileBackupVal);

        byte[] fileBackupValGet = dbUtils.getFileBackup(fileId);
        FileBackup fileBackupedGet = (FileBackup) SerializationUtil.deserialize(fileBackupValGet);
        assertEquals(fileBackuped.references.firstEntry().getValue(),
                fileBackupedGet.references.firstEntry().getValue());

        iterator = dbUtils.iteratorFileBackup();
        nbKey = 0;
        for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
            nbKey++;
        }
        assertEquals(12, nbKey);

        //delete
        dbUtils.deleteFileBackup(fileId);
        assertNull(dbUtils.getFileBackup(fileId));
    }

    @Test
    public void db_is_backuped() throws RocksDBException {
        assertFalse(new File(backupFolder, "meta/1").isFile());
        dbUtils.backup();
        assertTrue(new File(backupFolder, "meta/1").isFile());
        dbUtils.backup();
        dbUtils.backup();
        assertFalse(new File(backupFolder, "meta/1").isFile());
        assertTrue(new File(backupFolder, "meta/2").isFile());
    }

    @Test
    public void restore_db() throws RocksDBException, IOException {
        dbUtils.backup();
        writeData(11L, 20L);
        assertNotNull(dbUtils.getFileBackup("12".getBytes()));
        RocksIterator iterator = dbUtils.iteratorFileBackup();
        int nbKey = 0;
        for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
            nbKey++;
        }
        assertEquals(21, nbKey);

        File dbRestore = new File(rootDbFolder.getRoot().getAbsolutePath() + "/rest/database");
        Files.createDirectories(dbRestore.toPath());
        dbUtils.restore(dbRestore.toString());
        DbUtils dbUtilsRestore = new DbUtils(rootDbFolder.getRoot().getAbsolutePath() + "/rest");
        dbUtilsRestore.open();
        assertNull(dbUtilsRestore.getFileBackup("12".getBytes()));
        iterator = dbUtilsRestore.iteratorFileBackup();
        nbKey = 0;
        for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
            nbKey++;
        }
        assertEquals(11, nbKey);

    }

}
