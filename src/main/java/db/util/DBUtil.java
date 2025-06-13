package db.util;

import java.sql.*;

public class DBUtil {
    public static Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName("oracle.jdbc.OracleDriver");

            String dbUrl = "jdbc:oracle:thin:@//192.168.217.202:1521/KOPODA";
            //String dbUrl = "jdbc:oracle:thin:@dinkdb_medium?TNS_ADMIN=C:/Users/DA/Downloads/Wallet_DinkDB";
            String dbUser = "da2518";
            String dbPasswd = "da18";

            conn = DriverManager.getConnection(dbUrl, dbUser, dbPasswd);
        } catch (ClassNotFoundException e) {
            System.out.println("Oracle JDBC 드라이버를 찾을 수 없습니다.");
            e.printStackTrace();
        } catch (SQLException e) {
            System.out.println("DB 연결 실패: " + e.getMessage());
            e.printStackTrace();
        }
        return conn;
    }

    public static void close(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void close(PreparedStatement pstmt) {
        if (pstmt != null) {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void close(Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void close(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void closeAll(Connection conn, Statement stmt, ResultSet rs) {
        close(rs);
        close(stmt);
        close(conn);
    }

    public static void closeAll(Connection conn, Statement stmt) {
        close(stmt);
        close(conn);
    }

    public static void closeAll(Connection conn, PreparedStatement pstmt, ResultSet rs) {
        close(rs);
        close(pstmt);
        close(conn);
    }

    public static void closeAll(Connection conn, PreparedStatement pstmt) {
        close(pstmt);
        close(conn);
    }
}
