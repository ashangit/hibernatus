package fr.bionf.hibernatus.agent.junit.rules;

import fr.bionf.hibernatus.agent.db.DbUtils;
import fr.bionf.hibernatus.agent.db.FileBackupAction;
import fr.bionf.hibernatus.agent.glacier.AmazonGlacierArchiveOperations;
import fr.bionf.hibernatus.agent.proto.Db;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

public class WithDbUtils extends ExternalResource {

    private TemporaryFolder rootDbFolder = new TemporaryFolder();
    private DbUtils dbUtils;
    private DbUtils spyDbUtils;
    private AmazonGlacierArchiveOperations amazonGlacierArchiveOperations;
    private long retention = TimeUnit.DAYS.toMillis(1L);

    @Override
    protected void before() throws IOException, RocksDBException {
        rootDbFolder.create();
        amazonGlacierArchiveOperations = mock(AmazonGlacierArchiveOperations.class);
        when(amazonGlacierArchiveOperations.upload(any(String.class))).thenReturn("unittest");

        dbUtils = new DbUtils(rootDbFolder.getRoot().getAbsolutePath());
        dbUtils.open();

        spyDbUtils = spy(dbUtils);

        // Write few data
        writeData(0L, 10L);
    }

    public void writeData(long from, long to) throws IOException, RocksDBException {
        for (long i = from; i <= to; i++) {
            byte[] fileId = String.valueOf(i).getBytes();
            Db.FileToTreat fileToTreat = Db.FileToTreat.newBuilder().setFilename(String.valueOf(i))
                    .setLength(i).setMtime(i).build();
            dbUtils.writeFileToTreat(fileId, fileToTreat.toByteArray());
            FileBackupAction fileBackuped = new FileBackupAction(String.valueOf(i));
            fileBackuped.backupReference(amazonGlacierArchiveOperations, dbUtils, fileToTreat, retention);
            dbUtils.writeFileBackup(fileId, fileBackuped.getFileBackupBuilder().toByteArray());
        }
    }

    public DbUtils getDbUtils() {
        return spyDbUtils;
    }

    public TemporaryFolder getRootDbFolder() {
        return rootDbFolder;
    }
}
