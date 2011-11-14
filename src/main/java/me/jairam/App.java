package me.jairam;

import java.util.List;
import java.io.UnsupportedEncodingException;

import java.nio.ByteBuffer;
import java.util.Random;
import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.thrift.AuthorizationException;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

public class App {

        public static void main(String[] args) throws TTransportException, UnsupportedEncodingException, InvalidRequestException, NotFoundException, UnavailableException, TimedOutException, TException, AuthenticationException, AuthorizationException {

                TTransport tr = new TFramedTransport(new TSocket("localhost", 9160));
                TProtocol proto = new TBinaryProtocol(tr);

                Cassandra.Client client = new Cassandra.Client(proto);
                tr.open();
                String keyspace = "Checkins";
                client.set_keyspace(keyspace);
                //record id
                String key_user_id = "1";
                String columnFamily = "position";
                // insert data
                long timestamp = System.currentTimeMillis();
                Random r = new Random(timestamp);
                Column latColumn = new Column(ByteBuffer.wrap("latitude".getBytes()));
                latColumn.setValue(Long.toHexString(r.nextLong()).getBytes());
                latColumn.setTimestamp(timestamp);

                Column longColumn = new Column(ByteBuffer.wrap("longitude".getBytes()));
                longColumn.setValue(Long.toHexString(r.nextLong()).getBytes());
                longColumn.setTimestamp(timestamp);

                ColumnParent columnParent = new ColumnParent(columnFamily);
                client.insert(ByteBuffer.wrap(key_user_id.getBytes()), columnParent,latColumn,ConsistencyLevel.ALL) ;
                client.insert(ByteBuffer.wrap(key_user_id.getBytes()), columnParent,longColumn,ConsistencyLevel.ALL);

                //Gets column by key
                SlicePredicate predicate = new SlicePredicate();
                predicate.setSlice_range(new SliceRange(ByteBuffer.wrap(new byte[0]), ByteBuffer.wrap(new byte[0]), false, 100));
                List<ColumnOrSuperColumn> columnsByKey = client.get_slice(ByteBuffer.wrap(key_user_id.getBytes()), columnParent, predicate, ConsistencyLevel.ALL);
                System.out.println(columnsByKey);

                
                //Get all keys
                KeyRange keyRange = new KeyRange(100);
                keyRange.setStart_key(new byte[0]);
                keyRange.setEnd_key(new byte[0]);
                List<KeySlice> keySlices = client.get_range_slices(columnParent, predicate, keyRange, ConsistencyLevel.ONE);
                System.out.println(keySlices.size());
                System.out.println(keySlices);
                for (KeySlice ks : keySlices) {
                        System.out.println(new String(ks.getKey()));
                }
                tr.close();
        }

}
