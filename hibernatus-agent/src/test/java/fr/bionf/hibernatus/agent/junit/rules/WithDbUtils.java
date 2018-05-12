package fr.bionf.hibernatus.agent.junit.rules;

import fr.bionf.hibernatus.agent.db.DbUtils;
import fr.bionf.hibernatus.agent.db.FileBackup;
import fr.bionf.hibernatus.agent.db.FileToTreat;
import fr.bionf.hibernatus.agent.db.SerializationUtil;
import fr.bionf.hibernatus.agent.glacier.AmazonGlacierArchiveOperations;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class WithDbUtils extends ExternalResource {

    public TemporaryFolder rootDbFolder = new TemporaryFolder();
    private DbUtils dbUtils;
    private DbUtils spyDbUtils;
    private AmazonGlacierArchiveOperations amazonGlacierArchiveOperations;
    private long retention = TimeUnit.DAYS.toMillis(1L);

    @Override
    protected void before() throws IOException, RocksDBException {
        rootDbFolder.create();
        amazonGlacierArchiveOperations = mock(AmazonGlacierArchiveOperations.class);

        dbUtils = new DbUtils(rootDbFolder.getRoot().getAbsolutePath());
        dbUtils.open();

        spyDbUtils = spy(dbUtils);

        // Write few data
        writeData(0L, 10L);
    }

    public void writeData(long from, long to) throws IOException, RocksDBException {
        for (long i = from; i <= to; i++) {
            byte[] fileId = String.valueOf(i).getBytes();
            FileToTreat fileToTreat = new FileToTreat(i, i);
            dbUtils.writeFileToTreat(fileId, SerializationUtil.serialize(fileToTreat));
            FileBackup fileBackuped = new FileBackup(fileId);
            fileBackuped.backupReference(amazonGlacierArchiveOperations, dbUtils, fileToTreat, retention);
            dbUtils.writeFileBackup(fileId, SerializationUtil.serialize(fileBackuped));
        }
    }

    public DbUtils getDbUtils() {
        return spyDbUtils;
    }

    public TemporaryFolder getRootDbFolder(){
        return rootDbFolder;
    }
}
