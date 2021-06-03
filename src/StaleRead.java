import com.mysql.cj.jdbc.exceptions.CommunicationsException;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class StaleRead {
    private static String host, db;
    private static String user, password;
    private static boolean staleRead;
    private static int writeConcurrency, readConcurrency;

    public static void main(String[] args) throws Exception{
        args = new String[]{"127.1:4000", "test", "root", "", "true", "1", "1"};
        parseArgs(args);
        String url = String.format("jdbc:mysql://%s/%s?cachePrepStmts=true&useServerPrepStmts=true&useLocalSessionState=true", host, db);

        Class.forName("com.mysql.cj.jdbc.Driver");

        startThreads(url);
    }

    private static void startThreads(String url) {
        for (int i = 0; i < writeConcurrency; i++) {
            Runnable writeWorker = null;
//            switch (i % 4) {
//                case 0:
//                    writeWorker = new DeleteWorker(url, user, password);
//                    break;
//                default:
                    writeWorker = new InsertWorker(url, user, password);
//                    break;
//            }
            new Thread(writeWorker).start();
        }

        for (int i = 0; i < readConcurrency; i++) {
            Runnable readWorker = null;
//            switch (i % 3) {
//                case 0:
                    readWorker = new LookupWorker(url, user, password, staleRead);
//                    break;
//                case 1:
//                    readWorker = new PrefixLookupWorker(url, user, password, staleRead);
//                    break;
//                default:
//                    readWorker = new RangeLookupWorker(url, user, password, staleRead);
//                    break;
//            }
            new Thread(readWorker).start();
        }
    }

    private static void parseArgs(String[] args) throws Exception {
        int i = 0;
        if (args.length > i) {
            host = args[i];
        }
        i++;

        if (args.length > i) {
            db = args[i];
        }
        i++;

        if (args.length > i) {
            user = args[i];
        }
        i++;

        if (args.length > i) {
            password = args[i];
        }
        i++;

        if (args.length > i) {
            if (args[i].equalsIgnoreCase("true")) {
                staleRead = true;
            } else if (args[i].equalsIgnoreCase("false")) {
                staleRead = false;
            } else {
                throw new Exception(String.format("stale read must be true or false, but got '%s'", args[i]));
            }
        }
        i++;

        if (args.length > i) {
            writeConcurrency = Integer.parseInt(args[i]);
        }
        i++;

        if (args.length > i) {
            readConcurrency = Integer.parseInt(args[i]);
        }
    }
}

class Worker implements Runnable {

    private String url, user, password;
    protected boolean staleRead;

    public Worker(String url, String user, String password, boolean staleRead) {
        this.url = url;
        this.user = user;
        this.password = password;
        this.staleRead = staleRead;
    }

    protected void execute(Connection conn) throws SQLException {
    }

    protected void startTxn(Connection conn) throws SQLException {
        Statement stmt = conn.createStatement();
        if (staleRead) {
            stmt.execute("START TRANSACTION READ ONLY WITH TIMESTAMP BOUND EXACT STALENESS '00:00:03'");
        } else {
            stmt.execute("START TRANSACTION");
        }
        stmt.close();
    }

    protected void commitTxn(Connection conn) throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute("COMMIT");
        stmt.close();
    }

    protected void prepare(Connection conn) throws SQLException {
    }

    @Override
    public void run() {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, user, password);
            prepare(conn);
            while (true) {
                try {
                    execute(conn);
                } catch (CommunicationsException ce) {
                    System.err.println(ce.getMessage());
                    break;
                } catch (SQLException se) {
                    System.err.println(se.getMessage());
                }
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}

class ParamsGenerator {
    private static final int KEY_LENGTH = 100;
    private static final int KEY_PREFIX_LENGTH = 3;
    private static final int TS_RANGE = 100;
    private static final int VALUE_LENGTH = 4000;

    private static byte[] value = new byte[VALUE_LENGTH];

    public static byte[] generateKey() {
        byte[] bytes = new byte[KEY_LENGTH];
        Random rand = new Random();
        for (int i = 0; i < KEY_LENGTH; i++) {
            if (i < KEY_PREFIX_LENGTH) {
                bytes[i] = (byte) ('0' + rand.nextInt(10));
            } else{
                bytes[i] = '0';
            }
        }
        return bytes;
    }

    public static byte[] generatePrefix() {
        byte[] bytes = new byte[KEY_PREFIX_LENGTH];
        Random rand = new Random();
        for (int i = 0; i < KEY_PREFIX_LENGTH; i++) {
            bytes[i] = (byte) ('0' + rand.nextInt(10));
        }
        return bytes;
    }

    public static List<byte[]> generateRange() {
        byte[] start = generateKey();
        byte[] end = start.clone();
        end[KEY_PREFIX_LENGTH - 2] += 1;
        List<byte[]> list = new ArrayList<>();
        list.add(start);
        list.add(end);
        return list;
    }

    public static long generateLong() {
        Random rand = new Random();
        return rand.nextInt(TS_RANGE);
    }

    public static InputStream generateValue() {
        return new ByteArrayInputStream(value);
    }
}

class InsertWorker extends Worker {

    public InsertWorker(String url, String user, String password) {
        super(url, user, password, false);
    }

    @Override
    protected void prepare(Connection conn) throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute("SET @@TIDB_READ_STALENESS=3");
        stmt.close();
    }

    @Override
    protected void execute(Connection conn) throws SQLException {
        String sql = "insert into MUSSEL_V2_BENCHMARK_TEST"
                + " (primary_key, secondary_key, timestamp, value) values (?, ?, ?, ?)"
                + " on duplicate key update value=VALUES(value)";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setBytes(1, ParamsGenerator.generateKey());
        ps.setBytes(2, ParamsGenerator.generateKey());
        ps.setLong(3, ParamsGenerator.generateLong());
        ps.setBlob(4, ParamsGenerator.generateValue());
        ps.executeUpdate();
        ps.close();
    }
}

class DeleteWorker extends Worker {

    public DeleteWorker(String url, String user, String password) {
        super(url, user, password, false);
    }

    @Override
    protected void execute(Connection conn) throws SQLException {
        String sql = "delete from MUSSEL_V2_BENCHMARK_TEST"
                + " where primary_key=? and secondary_key=?";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setBytes(1, ParamsGenerator.generateKey());
        ps.setBytes(2, ParamsGenerator.generateKey());
        ps.executeUpdate();
        ps.close();
    }
}

class LookupWorker extends Worker {

    public LookupWorker(String url, String user, String password, boolean staleRead) {
        super(url, user, password, staleRead);
    }

    @Override
    protected void prepare(Connection conn) throws SQLException {
        if (!staleRead) {
            return;
        }
        Statement stmt = conn.createStatement();
        stmt.execute("SET @@TIDB_READ_STALENESS=3");
        stmt.close();
    }

    @Override
    protected void execute(Connection conn) throws SQLException {
//        startTxn(conn);
        String sql = "select * from MUSSEL_V2_BENCHMARK_TEST"
                + " where primary_key=? and secondary_key=?" +
                " order by timestamp desc limit 1";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setBytes(1, ParamsGenerator.generateKey());
        ps.setBytes(2, ParamsGenerator.generateKey());
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
        }
        rs.close();
        ps.close();
//        commitTxn(conn);
    }
}

class PrefixLookupWorker extends Worker {

    public PrefixLookupWorker(String url, String user, String password, boolean staleRead) {
        super(url, user, password, staleRead);
    }

    @Override
    protected void execute(Connection conn) throws SQLException {
        startTxn(conn);
        String sql = "select * from MUSSEL_V2_BENCHMARK_TEST"
                + " where primary_key=? and secondary_key like concat(?, '%')";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setBytes(1, ParamsGenerator.generateKey());
        ps.setBytes(2, ParamsGenerator.generatePrefix());
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
        }
        rs.close();
        ps.close();
        commitTxn(conn);
    }
}

class RangeLookupWorker extends Worker {

    public RangeLookupWorker(String url, String user, String password, boolean staleRead) {
        super(url, user, password, staleRead);
    }

    @Override
    protected void execute(Connection conn) throws SQLException {
        startTxn(conn);
        String sql = "select * from MUSSEL_V2_BENCHMARK_TEST"
                + " where primary_key=? and secondary_key>=? and secondary_key<=?";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setBytes(1, ParamsGenerator.generateKey());
        List<byte[]> range = ParamsGenerator.generateRange();
        ps.setBytes(2, range.get(0));
        ps.setBytes(3, range.get(1));
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
        }
        rs.close();
        ps.close();
        commitTxn(conn);
    }
}