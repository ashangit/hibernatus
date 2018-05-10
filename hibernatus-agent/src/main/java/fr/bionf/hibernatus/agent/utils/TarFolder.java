package fr.bionf.hibernatus.agent.utils;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.utils.IOUtils;

import java.io.*;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TarFolder {
    private final String folder;
    private final File archive;

    public TarFolder(String folder, String archive) {
        this.folder = folder;
        this.archive = new File(archive);
    }

    public void compress()
            throws IOException {
        FileOutputStream fos = new FileOutputStream(archive);
        TarArchiveOutputStream taos = new TarArchiveOutputStream(
                new BZip2CompressorOutputStream(new BufferedOutputStream(fos)));
        taos.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_STAR);
        taos.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);

        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(Paths.get(folder))) {
            for (Path path : directoryStream) {
                addFilesToCompression(taos, path.toFile(), ".");
            }
        }

        taos.close();
        fos.close();
    }

    private void addFilesToCompression(TarArchiveOutputStream taos, File file, String dir)
            throws IOException {
        // Create an entry for the file
        taos.putArchiveEntry(new TarArchiveEntry(file, dir + File.separator + file.getName()));
        if (file.isFile()) {
            // Add the file to the archive
            BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
            IOUtils.copy(bis, taos);
            taos.closeArchiveEntry();
            bis.close();
        } else if (file.isDirectory()) {
            taos.closeArchiveEntry();

            try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(file.toPath())) {
                for (Path path : directoryStream) {
                    addFilesToCompression(taos, path.toFile(), dir + File.separator + file.getName());
                }
            }
        }
    }

    public void uncompress() throws IOException {
        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(archive));
        TarArchiveInputStream tais = new TarArchiveInputStream(new BZip2CompressorInputStream(bis));

        TarArchiveEntry tarEntry = tais.getNextTarEntry();
        while (tarEntry != null) {
            File destPath = new File(folder + File.separator + tarEntry.getName());
            if (tarEntry.isDirectory()) {
                destPath.mkdirs();
            } else {
                destPath.createNewFile();
                FileOutputStream fos = new FileOutputStream(destPath);
                IOUtils.copy(tais, fos);
                fos.close();
            }
            tarEntry = tais.getNextTarEntry();
        }
        tais.close();
        bis.close();
    }
}
