package fr.bionf.hibernatus.agent.db;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;

public class SerializationUtilTest {
    private FileToTreat fileToTreat;
    private byte[] fileToTreatBytes;

    @Before
    public void setUp() throws IOException {
        fileToTreat = new FileToTreat(1L, 1L);
        fileToTreatBytes = SerializationUtil.serialize(fileToTreat);
    }

    @Test
    public void should_deserialize_fileToTreat() throws IOException, ClassNotFoundException {
        assertEquals(fileToTreat, SerializationUtil.deserialize(fileToTreatBytes));
    }


}
