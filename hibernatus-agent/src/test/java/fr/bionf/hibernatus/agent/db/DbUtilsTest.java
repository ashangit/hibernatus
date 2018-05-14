package fr.bionf.hibernatus.agent.db;

import fr.bionf.hibernatus.agent.conf.Constants;
import fr.bionf.hibernatus.agent.glacier.AmazonGlacierArchiveOperations;
import fr.bionf.hibernatus.agent.junit.rules.WithDbUtils;
import fr.bionf.hibernatus.agent.proto.Db;
import fr.bionf.hibernatus.agent.utils.TarFolder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

import static fr.bionf.hibernatus.agent.conf.Constants.BACKUP_PREFIX_NAME;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DbUtilsTest {
    @Rule
    public WithDbUtils withDbUtils = new WithDbUtils();
    public TemporaryFolder rootDbFolder;
    private File dbFolder;
    private File backupFolder;

    private long retention = TimeUnit.DAYS.toMillis(1L);

    private DbUtils dbUtils;
    private AmazonGlacierArchiveOperations amazonGlacierArchiveOperations;

    @Before
    public void setUp() throws FileNotFoundException {
        rootDbFolder = withDbUtils.getRootDbFolder();
        amazonGlacierArchiveOperations = mock(AmazonGlacierArchiveOperations.class);
        when(amazonGlacierArchiveOperations.upload(any(String.class))).thenReturn("unittest");

        dbFolder = new File(rootDbFolder.getRoot().getAbsolutePath()
                + Constants.SUB_DB_PATH);
        backupFolder = new File(rootDbFolder.getRoot().getAbsolutePath()
                + Constants.SUB_BACKUP_PATH);

        dbUtils = withDbUtils.getDbUtils();
    }

    @Test
    public void should_create_db_and_backup_folder() {
        assertTrue(Files.isDirectory(rootDbFolder.getRoot().toPath()));
        assertTrue(Files.isDirectory(dbFolder.toPath()));
        assertTrue(Files.isDirectory(backupFolder.toPath()));
    }

    @Test
    public void should_restore_db_from_tarbz_backup() throws IOException, RocksDBException {
        System.out.println(dbUtils);
        System.out.println(withDbUtils.getDbUtils());
        dbUtils.backup();

        TemporaryFolder rootRestoreFolder = new TemporaryFolder();
        rootRestoreFolder.create();
        DbUtils dbRestoreUtils = new DbUtils(rootRestoreFolder.getRoot().getAbsolutePath());

        new TarFolder(backupFolder.getAbsolutePath(), rootRestoreFolder.getRoot().getAbsolutePath()
                + File.separator + BACKUP_PREFIX_NAME + InetAddress.getLocalHost().getHostName() + ".tbz2").compress();

        dbRestoreUtils.open();
        assertNull(dbRestoreUtils.getFileToTreat("15".getBytes()));
        assertNotNull(dbRestoreUtils.getFileToTreat("2".getBytes()));

    }

    @Test
    public void should_create_fileToTreat_iterate_and_delete() throws IOException, RocksDBException {
        RocksIterator iterator = dbUtils.iteratorFileToTreat();
        int nbKey = 0;
        for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
            nbKey++;
        }
        assertEquals(11, nbKey);

        byte[] fileId = "11".getBytes();
        Db.FileToTreat fileToTreat = Db.FileToTreat.newBuilder().setFilename("11")
                .setLength(1L).setMtime(1L).build();

        dbUtils.writeFileToTreat(fileId, fileToTreat.toByteArray());

        Db.FileToTreat fileToTreatGet = Db.FileToTreat.parseFrom(dbUtils.getFileToTreat(fileId));
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
    public void should_create_fileBackup_iterate_and_delete() throws IOException, RocksDBException {
        RocksIterator iterator = dbUtils.iteratorFileBackup();
        int nbKey = 0;
        for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
            nbKey++;
        }
        assertEquals(11, nbKey);

        byte[] fileId = "11".getBytes();
        FileBackupAction fileBackuped = new FileBackupAction("11");
        Db.FileToTreat fileToTreat = Db.FileToTreat.newBuilder().setFilename("11")
                .setLength(11L).setMtime(11L).build();
        fileBackuped.backupReference(amazonGlacierArchiveOperations, dbUtils, fileToTreat, retention);
        byte[] fileBackupVal = fileBackuped.getFileBackupBuilder().toByteArray();
        dbUtils.writeFileBackup(fileId, fileBackupVal);

        byte[] fileBackupValGet = dbUtils.getFileBackup(fileId);
        Db.FileBackup fileBackupedGet = Db.FileBackup.parseFrom(fileBackupValGet);
        assertEquals(fileBackuped.getFileBackupBuilder().getReferencesMap(),
                fileBackupedGet.getReferencesMap());

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
    }

    @Test
    public void restore_db() throws RocksDBException, IOException {
        dbUtils.backup();
        withDbUtils.writeData(11L, 20L);
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
