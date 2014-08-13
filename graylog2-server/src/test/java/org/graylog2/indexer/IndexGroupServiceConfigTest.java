package org.graylog2.indexer;

import org.testng.annotations.Test;

public class IndexGroupServiceConfigTest {

    @Test
    public void testIndexCreateFunction() {
        final IndexGroupConfig iF1 = new IndexGroupConfig("gl2", "messages");

//        final Index gl2_messages_0 = iF1.createIndexFromName.apply("gl2_messages_0");
//        assertNotNull(gl2_messages_0);
//        assertEquals(gl2_messages_0.getName(), "gl2_messages");
//        assertEquals(gl2_messages_0.getNumber(), 0);
//
//        try {
//            final Index gl2_messages_ = iF1.createIndexFromName.apply("gl2_messages_");
//            fail("missing index number should throw exception");
//        } catch (IllegalStateException ignored) {}
//
//        final Index gl2_messages_20 = iF1.createIndexFromName.apply("gl2_messages_20");
//        assertNotNull(gl2_messages_20);
//        assertEquals(gl2_messages_20.getName(), "gl2_messages");
//        assertEquals(gl2_messages_20.getNumber(), 20);
//
//        final IndexGroupConfig iF2 = new IndexGroupConfig("gl2", "mess_ages");
//        final Index gl2_mess_ages_20 = iF2.createIndexFromName.apply("gl2_mess_ages_20");
//        assertNotNull(gl2_mess_ages_20);
//        assertEquals(gl2_mess_ages_20.getName(), "gl2_mess_ages");
//        assertEquals(gl2_mess_ages_20.getNumber(), 20);
//
//        final IndexGroupConfig iF3 = new IndexGroupConfig("gl2", "mess_ag_es");
//        final Index gl2_mess_ag_es_20 = iF3.createIndexFromName.apply("gl2_mess_ag_es_20");
//        assertNotNull(gl2_mess_ag_es_20);
//        assertEquals(gl2_mess_ag_es_20.getName(), "gl2_mess_ag_es");
//        assertEquals(gl2_mess_ag_es_20.getNumber(), 20);
//
//        try {
//            final IndexGroupConfig famEs20 = new IndexGroupConfig("es", "messages");
//            final Index es_20 = famEs20.createIndexFromName.apply("es_20");
//            fail("malformed prefix or index name should throw exception");
//        } catch (IllegalStateException ignored) {}
    }


}