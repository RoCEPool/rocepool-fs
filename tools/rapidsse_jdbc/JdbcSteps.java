// Executes SQL steps over JDBC for tools/rapidsse_tpch.py, the way JMeter's
// JDBC sampler does in RapidsSE's own test plans: one connection
// (autocommit), Statement.executeQuery for "Select Statement" steps,
// Statement.execute for "Update Statement" steps, the result kept as text.
//
// Protocol on stdin/stdout, one step at a time:
//   in : "S <nbytes>\n" or "U <nbytes>\n", then <nbytes> of UTF-8 SQL
//        ("A <nbytes>": a select whose whole result is wanted)
//   out: one line: ok \t millis \t rows \t first-column-of-first-row \t error \t digest
//        (digest: SHA-256 of the whole result, every value as text, so two
//        runs' answers can be compared exactly)
//        (for "A": the whole result instead of the first value, columns
//        separated by \u001f, rows by \u001e)
// A line "Q\n" ends the session.
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;

public class JdbcSteps {
    static String clean(String s) {
        return s == null ? "" : s.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ');
    }

    public static void main(String[] args) throws Exception {
        String url = args[0];
        DataInputStream in = new DataInputStream(new BufferedInputStream(System.in));
        PrintStream out = new PrintStream(new FileOutputStream(FileDescriptor.out), true, "UTF-8");
        Connection conn = DriverManager.getConnection(url, args.length > 1 ? args[1] : "",
                                                      args.length > 2 ? args[2] : "");
        conn.setAutoCommit(true);
        out.println("CONNECTED\t" + clean(conn.getMetaData().getDriverName()) + " "
                    + clean(conn.getMetaData().getDriverVersion()));
        while (true) {
            String head = readLine(in);
            if (head == null || head.equals("Q")) break;
            boolean all = head.charAt(0) == 'A';
            boolean select = all || head.charAt(0) == 'S';
            int n = Integer.parseInt(head.substring(2).trim());
            byte[] buf = new byte[n];
            in.readFully(buf);
            String sql = new String(buf, StandardCharsets.UTF_8);
            long t0 = System.nanoTime();
            long rows = 0;
            String first = "";
            String err = "";
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-256");
            boolean ok = true;
            try (Statement st = conn.createStatement()) {
                if (select) {
                    try (ResultSet rs = st.executeQuery(sql)) {
                        int nc = rs.getMetaData().getColumnCount();
                        StringBuilder sb = new StringBuilder();
                        while (rs.next()) {
                            if (rows == 0) first = String.valueOf(rs.getObject(1));
                            for (int c = 1; c <= nc; c++) {
                                md.update(String.valueOf(rs.getObject(c)).getBytes(StandardCharsets.UTF_8));
                                md.update((byte) (c == nc ? 0x1e : 0x1f));
                            }
                            if (all) {
                                if (rows > 0) sb.append('\u001e');
                                for (int c = 1; c <= nc; c++) {
                                    if (c > 1) sb.append('\u001f');
                                    sb.append(clean(String.valueOf(rs.getObject(c))));
                                }
                            }
                            rows++;
                        }
                        if (all) first = sb.toString();
                    }
                } else {
                    st.execute(sql);
                }
            } catch (SQLException e) {
                ok = false;
                err = e.getClass().getSimpleName() + ": " + e.getMessage();
            }
            long ms = (System.nanoTime() - t0) / 1000000;
            String digest = select && ok ? java.util.HexFormat.of().formatHex(md.digest()) : "";
            out.println((ok ? "1" : "0") + "\t" + ms + "\t" + rows + "\t" + clean(first) + "\t" + clean(err)
                        + "\t" + digest);
        }
        conn.close();
    }

    static String readLine(DataInputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        int c;
        while ((c = in.read()) != -1 && c != '\n') sb.append((char) c);
        return c == -1 && sb.length() == 0 ? null : sb.toString();
    }
}
