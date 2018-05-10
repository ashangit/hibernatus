package fr.bionf.hibernatus.agent.executor;

import fr.bionf.hibernatus.agent.conf.AgentConfig;
import fr.bionf.hibernatus.agent.db.DbUtils;
import fr.bionf.hibernatus.agent.db.FileToTreat;
import fr.bionf.hibernatus.agent.db.SerializationUtil;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static fr.bionf.hibernatus.agent.conf.AgentConfig.AGENT_BACKUP_FOLDERS_KEY;

public class ListFileExecutor implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(ListFileExecutor.class);

    private static final ExecutorService cachedExecutor = Executors.newSingleThreadExecutor();
    private final DbUtils dbUtils;
    private final ArrayList<String> folders;

    public ListFileExecutor(DbUtils dbUtils) throws IOException {

        AgentConfig agentConfig = new AgentConfig();
        this.dbUtils = dbUtils;
        this.folders = agentConfig.getArray(AGENT_BACKUP_FOLDERS_KEY);
    }

    @Override
    public void run() {
        for (String folder : folders) {
            cachedExecutor.submit(new ListFile(folder));
        }
    }

    public class ListFile implements Runnable {
        private final String folder;

        ListFile(String folder) {
            this.folder = folder;
        }

        @Override
        // TODO: currently limited to Integer.MAX sub folder otherwise the submit operation will lock and no thread will
        // treat the queue as it is blocked here
        public void run() {
            logger.info("List files to backup on {}", folder);
            try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(Paths.get(folder))) {
                for (Path path : directoryStream) {
                    if (Files.isRegularFile(path)) {
                        String fileS = path.toString();
                        File file = new File(fileS);
                        dbUtils.writeFileToTreat(fileS.getBytes(),
                                SerializationUtil.serialize(new FileToTreat(
                                        file.lastModified(),
                                        file.length())));
                    } else if (Files.isDirectory(path)) {
                        cachedExecutor.submit(new ListFile(path.toString()));
                    }
                }
            } catch (IOException | RocksDBException ex) {
                logger.error("", ex);
            }
            logger.debug("End listing files to backup on {}", folder);
        }
    }
}
