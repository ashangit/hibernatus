package fr.bionf.hibernatus.agent.db;

import fr.bionf.hibernatus.agent.glacier.AmazonGlacierArchiveOperations;
import fr.bionf.hibernatus.agent.proto.Db;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public class FileBackupAction implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(FileBackupAction.class);

    private final Db.FileBackup.Builder fileBackupBuilder;
    private final byte[] fileName;

    private TreeMap<Long, Db.FileBackup.AwsFile> references;

    public FileBackupAction(String filename) {
        this(Db.FileBackup.newBuilder()
                .setFilename(filename));
    }

    public FileBackupAction(Db.FileBackup fileBackup) {
        this(fileBackup.toBuilder());
    }

    private FileBackupAction(Db.FileBackup.Builder fileBackupBuilder) {
        this.fileBackupBuilder = fileBackupBuilder;
        fileName = fileBackupBuilder.getFilename().getBytes();
        references = new TreeMap<>(fileBackupBuilder.getReferencesMap());
    }

    public Db.FileBackup getFileBackupBuilder() {
        return fileBackupBuilder.build();
    }

    byte[] getFileName() {
        return fileName;
    }

    public TreeMap<Long, Db.FileBackup.AwsFile> getReferences() {
        return references;
    }

    public void backupReference(AmazonGlacierArchiveOperations amazonGlacierArchiveOperations,
                                DbUtils dbUtils, Db.FileToTreat fileToTreat, long retention)
            throws IOException, RocksDBException {
        long now = System.currentTimeMillis();
        String archiveId = amazonGlacierArchiveOperations.upload(fileBackupBuilder.getFilename());
        fileBackupBuilder.putReferences(
                now,
                Db.FileBackup.AwsFile.newBuilder()
                        .setLastModified(fileToTreat.getMtime())
                        .setLength(fileToTreat.getLength())
                        .setLastBackuped(now)
                        .setDeleteEpoch(now + retention)
                        .setAwsObject(archiveId)
                        .build()
        );
        dbUtils.writeFileBackup(fileName, this.getFileBackupBuilder().toByteArray());
        references = new TreeMap<>(fileBackupBuilder.getReferencesMap());
    }

    void deleteReference(AmazonGlacierArchiveOperations amazonGlacierArchiveOperations,
                         DbUtils dbUtils, long awsFileKey) throws RocksDBException {
        amazonGlacierArchiveOperations.delete(fileBackupBuilder.getReferencesMap().get(awsFileKey).getAwsObject());
        fileBackupBuilder.removeReferences(awsFileKey);

        if (fileBackupBuilder.getReferencesMap().isEmpty()) {
            dbUtils.deleteFileBackup(fileName);
            references = new TreeMap<>(fileBackupBuilder.getReferencesMap());
        } else {
            dbUtils.writeFileBackup(fileName, this.getFileBackupBuilder().toByteArray());
            references = new TreeMap<>(fileBackupBuilder.getReferencesMap());
        }

    }

    public void purge(AmazonGlacierArchiveOperations amazonGlacierArchiveOperations,
                      DbUtils dbUtils, long now, long retention) throws RocksDBException {
        File f = new File(fileBackupBuilder.getFilename());

        for (Map.Entry<Long, Db.FileBackup.AwsFile> entry : fileBackupBuilder.getReferencesMap().entrySet()) {
            Db.FileBackup.AwsFile awsFile = entry.getValue();
            if (f.isFile() && f.lastModified() == awsFile.getLastModified()) {
                logger.info("Update deletion time for {}", fileBackupBuilder.getFilename());
                awsFile.toBuilder().setDeleteEpoch(now + retention);
            } else if (awsFile.getDeleteEpoch() < now) {
                logger.info("Delete backup {} for file {}", awsFile.getDeleteEpoch(), fileBackupBuilder.getFilename());
                this.deleteReference(amazonGlacierArchiveOperations, dbUtils, entry.getKey());
            }
        }
    }
}
