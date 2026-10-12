package site.ycsb.db;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

/**
 * Real, unmodified-upstream-YCSB {@link DB} binding for this project's
 * storage layer. Talks a small fixed-width binary protocol to
 * {@code tools/ycsb_bridge.py}, which forwards every operation to a real
 * {@code PoolNode} (or a bare {@code StorageEngine} for the local backend)
 * -- see that file's module docstring for exactly why a bridge and not a
 * from-scratch reimplementation of {@code roce/protocol.py}'s wire format
 * in Java: this binding is thin glue, not a second implementation of the
 * thing under test.
 *
 * <p>One socket per DB instance (YCSB creates one DB instance per client
 * thread and calls {@link #init()} once), reused for every operation on
 * that thread for the life of the run -- matching how a real client of
 * this project's storage layer would hold one connection open, and
 * avoiding a connect-per-operation cost that would benchmark TCP handshake
 * overhead instead of the storage path.</p>
 *
 * <p>Properties (set via {@code -p}):</p>
 * <ul>
 *   <li>{@code rocepool.host} (default 127.0.0.1)</li>
 *   <li>{@code rocepool.port} (default 19900)</li>
 * </ul>
 */
public class RocepoolClient extends DB {
  private static final int OP_READ = 1;
  private static final int OP_INSERT = 2;
  private static final int OP_UPDATE = 3;
  private static final int OP_DELETE = 4;
  private static final int OP_SCAN = 5;

  private static final int ST_OK = 0;
  private static final int ST_NOT_FOUND = 1;
  private static final int ST_ERROR = 2;

  private Socket socket;
  private DataInputStream in;
  private DataOutputStream out;

  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    String host = props.getProperty("rocepool.host", "127.0.0.1");
    int port = Integer.parseInt(props.getProperty("rocepool.port", "19900"));
    try {
      socket = new Socket(host, port);
      socket.setTcpNoDelay(true);
      in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 1 << 16));
      out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 1 << 16));
    } catch (IOException e) {
      throw new DBException("connecting to rocepool bridge at " + host + ":" + port, e);
    }
  }

  @Override
  public void cleanup() throws DBException {
    try {
      if (socket != null) {
        socket.close();
      }
    } catch (IOException e) {
      throw new DBException(e);
    }
  }

  private static void writeStr(DataOutputStream o, String s) throws IOException {
    byte[] b = s.getBytes(StandardCharsets.UTF_8);
    o.writeShort(b.length);
    o.write(b);
  }

  private static String readStr(DataInputStream i, int len) throws IOException {
    byte[] b = new byte[len];
    i.readFully(b);
    return new String(b, StandardCharsets.UTF_8);
  }

  private void writeFieldNames(Set<String> fields) throws IOException {
    if (fields == null) {
      out.writeByte(1); // all_fields
      out.writeShort(0);
    } else {
      out.writeByte(0);
      out.writeShort(fields.size());
      for (String f : fields) {
        writeStr(out, f);
      }
    }
  }

  private void writeValues(Map<String, ByteIterator> values) throws IOException {
    out.writeShort(values.size());
    for (Map.Entry<String, ByteIterator> e : values.entrySet()) {
      writeStr(out, e.getKey());
      byte[] v = e.getValue().toArray();
      out.writeInt(v.length);
      out.write(v);
    }
  }

  private Map<String, ByteIterator> readFields() throws IOException {
    int count = in.readUnsignedShort();
    Map<String, ByteIterator> result = new HashMap<>();
    for (int i = 0; i < count; i++) {
      int nlen = in.readUnsignedShort();
      String name = readStr(in, nlen);
      int vlen = in.readInt();
      byte[] value = new byte[vlen];
      in.readFully(value);
      result.put(name, new ByteArrayByteIterator(value));
    }
    return result;
  }

  /** Reads the 1-byte status, and for ST_ERROR also the message -- shared
   *  by every operation's response handling below. Returns the status;
   *  throws with the bridge's own error text on ST_ERROR so it shows up
   *  in YCSB's own error accounting rather than being swallowed. */
  private int readStatus() throws IOException {
    int status = in.readUnsignedByte();
    if (status == ST_ERROR) {
      int mlen = in.readUnsignedShort();
      String msg = readStr(in, mlen);
      throw new IOException("rocepool bridge error: " + msg);
    }
    return status;
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      out.writeByte(OP_READ);
      writeStr(out, table);
      writeStr(out, key);
      writeFieldNames(fields);
      out.flush();
      int status = readStatus();
      if (status == ST_NOT_FOUND) {
        return Status.NOT_FOUND;
      }
      result.putAll(readFields());
      return Status.OK;
    } catch (IOException e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                      Vector<HashMap<String, ByteIterator>> result) {
    try {
      out.writeByte(OP_SCAN);
      writeStr(out, table);
      writeStr(out, startkey);
      out.writeInt(recordcount);
      writeFieldNames(fields);
      out.flush();
      int status = readStatus();
      if (status == ST_NOT_FOUND) {
        return Status.NOT_FOUND;
      }
      int n = in.readInt();
      for (int i = 0; i < n; i++) {
        result.add(new HashMap<>(readFields()));
      }
      return Status.OK;
    } catch (IOException e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try {
      out.writeByte(OP_UPDATE);
      writeStr(out, table);
      writeStr(out, key);
      writeValues(values);
      out.flush();
      int status = readStatus();
      return status == ST_NOT_FOUND ? Status.NOT_FOUND : Status.OK;
    } catch (IOException e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      out.writeByte(OP_INSERT);
      writeStr(out, table);
      writeStr(out, key);
      writeValues(values);
      out.flush();
      readStatus();
      return Status.OK;
    } catch (IOException e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    try {
      out.writeByte(OP_DELETE);
      writeStr(out, table);
      writeStr(out, key);
      out.flush();
      int status = readStatus();
      return status == ST_NOT_FOUND ? Status.NOT_FOUND : Status.OK;
    } catch (IOException e) {
      return Status.ERROR;
    }
  }
}
