package fr.bionf.hibernatus.agent.executor;

import fr.bionf.hibernatus.agent.db.DbUtils;
import fr.bionf.hibernatus.agent.utils.TarFolder;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.RocksDBException;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class BackupMetaExecutorTest {
    private DbUtils dbUtils;
    private TarFolder tarFolder;

    @Before
    public void setUp() {
        dbUtils = mock(DbUtils.class);
        tarFolder= mock(TarFolder.class);
    }

    @Test
    public void should_call_backup_db_and_tar() throws IOException, RocksDBException {
        BackupMetaExecutor backupMetaExecutor = new BackupMetaExecutor(dbUtils);
        backupMetaExecutor.tarFolder = tarFolder;
        backupMetaExecutor.run();
        verify(dbUtils).backup();
        verify(tarFolder).compress();
    }
}
