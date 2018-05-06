package fr.bionf.hibernatus.agent.db;

import fr.bionf.hibernatus.agent.Constants;
import fr.bionf.hibernatus.agent.executor.ListFileExecutor;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;

public class DbUtils {
    private static final Logger logger = LoggerFactory.getLogger(ListFileExecutor.class);
    private final String dbPath;
    private final String backupPath;

    private final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
    private DBOptions dbOptions;
    private RocksDB db;
    private BackupableDBOptions backupableDBOptions;
    private BackupEngine backupEngine;

    private ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()
            .optimizeUniversalStyleCompaction()
            .setCompressionType(CompressionType.LZ4_COMPRESSION);
    private final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
            new ColumnFamilyDescriptor("file-to-treat".getBytes(), cfOpts),
            new ColumnFamilyDescriptor("file-backup".getBytes(), cfOpts)
    );

    public DbUtils(String dbPath) throws RocksDBException, IOException {
        // a static method that loads the RocksDB C++ library.
        RocksDB.loadLibrary();

        this.dbPath = dbPath + Constants.SUB_DB_PATH;
        this.backupPath = dbPath + Constants.SUB_BACKUP_PATH;

        createDBPaths();

        backupableDBOptions = new BackupableDBOptions(backupPath)
                .setShareTableFiles(true)
                .setSync(true);

        backupEngine = BackupEngine.open(Env.getDefault(), backupableDBOptions);
    }

    private void createDBPaths() throws IOException {
        if (!Files.isDirectory(new File(dbPath).toPath())) {
            Files.createDirectories(new File(dbPath).toPath());
        }

        if (!Files.isDirectory(new File(backupPath).toPath())) {
            Files.createDirectories(new File(backupPath).toPath());
        }
    }

    public void open() throws RocksDBException, IOException {
        dbOptions = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true);

        db = RocksDB.open(dbOptions, dbPath, cfDescriptors, columnFamilyHandleList);
    }


    public void close() {
        if (columnFamilyHandleList != null) {
            for (ColumnFamilyHandle columnFamilyHandle :
                    columnFamilyHandleList) {
                columnFamilyHandle.close();
            }
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

    private byte[] get(ColumnFamilyHandle columnFamilyHandle, byte[] key) throws RocksDBException {
        return db.get(columnFamilyHandle, key);
    }

    public void writeFileBackup(byte[] key, byte[] value) throws RocksDBException {
        this.write(columnFamilyHandleList.get(2), key, value);
    }

    public void writeFileToTreat(byte[] key, byte[] value) throws RocksDBException {
        this.write(columnFamilyHandleList.get(1), key, value);
    }

    private void write(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value) throws RocksDBException {
        db.put(columnFamilyHandle, key, value);
    }

    public void deleteFileBackup(byte[] key) {
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

    public void backup() throws RocksDBException {
        backupEngine.createNewBackup(db);
        backupEngine.purgeOldBackups(2);
    }

    public void restore() throws RocksDBException {
        restore(this.dbPath);
    }

    public void restore(String path) throws RocksDBException {
        RestoreOptions restoreOptions = new RestoreOptions(false);
        backupEngine.restoreDbFromLatestBackup(path, path, restoreOptions);
    }
}
