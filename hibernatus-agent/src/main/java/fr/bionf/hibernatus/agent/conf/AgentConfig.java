package fr.bionf.hibernatus.agent.conf;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;

public class AgentConfig {
    private static final Logger logger = LoggerFactory.getLogger(AgentConfig.class);
    private HashMap Configuration = new HashMap();

    public static final String AGENT_METADATA_PATH_KEY = "metadata.path";
    public static final String AGENT_BACKUP_FOLDERS_KEY = "backup.folders";
    public static final String AGENT_BACKUP_INTERVAL_KEY = "backup.interval";
    public static final long AGENT_BACKUP_INTERVAL_DEFAULT = 60 * 60 * 24;
    public static final String AGENT_BACKUP_RETENTION_KEY = "backup.retention";
    public static final int AGENT_BACKUP_RETENTION_DEFAULT = 30;
    public static final String AGENT_PURGE_INTERVAL_KEY = "purge.interval";
    public static final long AGENT_PURGE_INTERVAL_DEFAULT = 60 * 60 * 24;
    public static final String AGENT_BACKUP_VAULT_NAME = "backup.vault.name";

    public AgentConfig() throws IOException {
        this("/config.yml");
    }

    AgentConfig(String configPath) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try (InputStream streamPropFilePath = AgentConfig.class.getResourceAsStream(configPath)) {
            Configuration.putAll(mapper.readValue(streamPropFilePath, HashMap.class));
        } catch (Exception e) {
            logger.error("Issue reading config file");
            throw e;
        }
        setConfigurations();
    }

    void setConfigurations() throws UnknownHostException {
        setConf(AGENT_BACKUP_INTERVAL_KEY, AGENT_BACKUP_INTERVAL_DEFAULT);
        setConf(AGENT_BACKUP_RETENTION_KEY, AGENT_BACKUP_RETENTION_DEFAULT);
        setConf(AGENT_PURGE_INTERVAL_KEY, AGENT_PURGE_INTERVAL_DEFAULT);
        setConf(AGENT_BACKUP_VAULT_NAME, this.getVaultName());
    }

    void setConf(String key, Object value) {
        if (!Configuration.containsKey(key)) {
            Configuration.put(key, value);
        }
    }

    public String getString(String key) {
        return (String) Configuration.get(key);
    }

    public long getLong(String key) {
        return (long) Configuration.get(key);
    }

    public int getInt(String key) {
        return (int) Configuration.get(key);
    }

    public ArrayList getArray(String key) {
        return (ArrayList) Configuration.get(key);
    }

    public String getVaultName() throws UnknownHostException {
        return "hibernatus-" + InetAddress.getLocalHost().getHostName();
    }

}
