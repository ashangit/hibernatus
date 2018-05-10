package fr.bionf.hibernatus.agent.db;

import fr.bionf.hibernatus.agent.conf.Constants;
import fr.bionf.hibernatus.agent.executor.ListFileExecutor;
import fr.bionf.hibernatus.agent.utils.TarFolder;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static fr.bionf.hibernatus.agent.conf.Constants.BACKUP_PREFIX_NAME;

public class DbUtils {
    private static final Logger logger = LoggerFactory.getLogger(ListFileExecutor.class);
    private final String dbPath;
    private final String backupPath;
    private final String backupTarBz;

    private final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
    private DBOptions dbOptions;
    private RocksDB db;

    private ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()
            .optimizeUniversalStyleCompaction()
            .setCompressionType(CompressionType.LZ4_COMPRESSION);
    private final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
            new ColumnFamilyDescriptor("file-to-treat".getBytes(), cfOpts),
            new ColumnFamilyDescriptor("file-backup".getBytes(), cfOpts)
    );

    public DbUtils(String metaPath) throws IOException {
        backupTarBz = metaPath + File.separator + BACKUP_PREFIX_NAME + InetAddress.getLocalHost().getHostName() + ".tbz2";

        // a static method that loads the RocksDB C++ library.
        RocksDB.loadLibrary();

        this.dbPath = metaPath + Constants.SUB_DB_PATH;
        this.backupPath = metaPath + Constants.SUB_BACKUP_PATH;

        createDBPaths();
    }

    private void createDBPaths() {
        if (!new File(dbPath).isDirectory()) {
            new File(dbPath).mkdirs();
        }

        if (!new File(backupPath).isDirectory()) {
            new File(backupPath).mkdirs();
        }
    }

    private void restoreBackupIfExist(String backupTarBz) throws IOException, RocksDBException {
        if (new File(backupTarBz).isFile()) {
            TarFolder tarFolder = new TarFolder(backupPath, backupTarBz);
            tarFolder.uncompress();
            this.restore();
        }
    }

    public void open() throws RocksDBException, IOException {
        logger.info("Open metadata in {}", dbPath);
        if (Objects.requireNonNull(new File(dbPath).listFiles()).length == 0) {
            restoreBackupIfExist(backupTarBz);
        }

        dbOptions = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true);

        db = RocksDB.open(dbOptions, dbPath, cfDescriptors, columnFamilyHandleList);
    }


    public void close() {
        for (ColumnFamilyHandle columnFamilyHandle :
                columnFamilyHandleList) {
            columnFamilyHandle.close();
        }
        if (db != null) {
            db.close();
        }
        if (dbOptions != null) {
            dbOptions.close();
        }
        if (cfOpts != null) {
            cfOpts.close();
        }
    }

    public RocksIterator iteratorFileBackup() {
        return this.iterator(columnFamilyHandleList.get(2));
    }

    public RocksIterator iteratorFileToTreat() {
        return this.iterator(columnFamilyHandleList.get(1));
    }

    private RocksIterator iterator(ColumnFamilyHandle columnFamilyHandle) {
        return db.newIterator(columnFamilyHandle);
    }

    public byte[] getFileBackup(byte[] key) throws RocksDBException {
        return this.get(columnFamilyHandleList.get(2), key);
    }

    byte[] getFileToTreat(byte[] key) throws RocksDBException {
        return this.get(columnFamilyHandleList.get(1), key);
    }

    private byte[] get(ColumnFamilyHandle columnFamilyHandle, byte[] key) throws RocksDBException {
        return db.get(columnFamilyHandle, key);
    }

    void writeFileBackup(byte[] key, byte[] value) throws RocksDBException {
        this.write(columnFamilyHandleList.get(2), key, value);
    }

    public void writeFileToTreat(byte[] key, byte[] value) throws RocksDBException {
        this.write(columnFamilyHandleList.get(1), key, value);
    }

    private void write(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value) throws RocksDBException {
        db.put(columnFamilyHandle, key, value);
    }

    void deleteFileBackup(byte[] key) {
        this.delete(columnFamilyHandleList.get(2), key);
    }

    public void deleteFileToTreat(byte[] key) {
        this.delete(columnFamilyHandleList.get(1), key);
    }

    private void delete(ColumnFamilyHandle columnFamilyHandle, byte[] key) {
        try {
            db.delete(columnFamilyHandle, key);
        } catch (RocksDBException e) {
            logger.error("", e);
        }
    }

    public String getBackupPath() {
        return backupPath;
    }

    private BackupEngine initBackupEngine() throws RocksDBException {
        BackupableDBOptions backupableDBOptions = new BackupableDBOptions(backupPath)
                .setShareTableFiles(true)
                .setSync(true);

        return BackupEngine.open(Env.getDefault(), backupableDBOptions);
    }

    public void backup() throws RocksDBException {
        BackupEngine backupEngine = initBackupEngine();

        backupEngine.createNewBackup(db);
        backupEngine.purgeOldBackups(1);

        backupEngine.close();
    }

    private void restore() throws RocksDBException {
        restore(this.dbPath);
    }

    void restore(String path) throws RocksDBException {
        BackupEngine backupEngine = initBackupEngine();

        RestoreOptions restoreOptions = new RestoreOptions(false);
        backupEngine.restoreDbFromLatestBackup(path, path, restoreOptions);

        backupEngine.close();
    }
}
