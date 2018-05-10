package fr.bionf.hibernatus.agent.utils;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;

import static org.junit.Assert.assertTrue;

public class TarFolderTest {
    @Rule
    public TemporaryFolder rootFolder = new TemporaryFolder();
    private TemporaryFolder folderRestore;
    private TarFolder tarFolder;
    private TarFolder tarFolder2;
    private File compressFile;

    private void writeData(File file) throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter(file));
        bw.write("This is the temporary file content");
        bw.write(String.valueOf(System.currentTimeMillis()));
        bw.write(file.getAbsolutePath());
        bw.close();
    }

    @Before
    public void setup() throws IOException {
        TemporaryFolder folderToCompress = new TemporaryFolder(rootFolder.newFolder("bck"));
        folderToCompress.create();
        folderRestore = new TemporaryFolder(rootFolder.newFolder("restore"));
        folderRestore.create();
        compressFile = rootFolder.newFile("bck.tbz2");
        writeData(folderToCompress.newFile("test1"));
        writeData(folderToCompress.newFile("test2"));
        folderToCompress.newFolder("folder1", "folder");
        folderToCompress.newFolder("folder2", "folder2");
        tarFolder = new TarFolder(folderToCompress.getRoot().getAbsolutePath(), compressFile.getAbsolutePath());
        tarFolder2 = new TarFolder(folderRestore.getRoot().getAbsolutePath(), compressFile.getAbsolutePath());
    }

    @Test
    public void should_create_tbz2_file() throws IOException {
        tarFolder.compress();
        assertTrue(compressFile.length() > 300);
        tarFolder2.uncompress();
        FileReader fr = new FileReader(folderRestore.getRoot().getAbsolutePath() + File.separator + "test1");
        BufferedReader br = new BufferedReader(fr);
        assertTrue(br.readLine().contains("This is the temporary file"));
    }
}
