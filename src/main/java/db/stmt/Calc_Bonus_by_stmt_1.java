package db.stmt;

import db.util.DBUtil;

import java.sql.*;

public class Calc_Bonus_by_stmt_1 {

    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;

        try {
            long startTime = System.currentTimeMillis();

            conn = DBUtil.getConnection();
            conn.setAutoCommit(false);

            stmt = conn.createStatement();

            // 기존 쿠폰 테이블 초기화
            stmt.executeUpdate("TRUNCATE TABLE BONUS_COUPON");

            // 고객 정보 조회 (2018년 1월 1일 이후 가입자)
            String selectSql = "SELECT ID, EMAIL, CREDIT_LIMIT, GENDER, ADDRESS1 " +
                    "FROM CUSTOMER " +
                    "WHERE ENROLL_DT >= TO_DATE('2018-01-01', 'YYYY-MM-DD')";
            rs = stmt.executeQuery(selectSql);

            int count = 0;

            while (rs.next()) {
                String customerId = rs.getString("ID");
                String email = rs.getString("EMAIL");
                int credit = rs.getInt("CREDIT_LIMIT");
                String gender = rs.getString("GENDER");
                String address = rs.getString("ADDRESS1");

                String couponCd = getCouponCode(credit, gender, address);

                // 매번 Statement 생성하여 insert
                Statement insertStmt = conn.createStatement();
                String insertSql = String.format(
                        "INSERT INTO BONUS_COUPON (YYYYMM, CUSTOMER_ID, EMAIL, COUPON_CD, CREDIT_POINT, SEND_DT) " +
                                "VALUES ('202506', '%s', '%s', '%s', %d, SYSDATE)",
                        customerId, email, couponCd, credit
                );
                insertStmt.executeUpdate(insertSql);
                insertStmt.close();

                conn.commit();
                count++;
            }

            long endTime = System.currentTimeMillis();
            System.out.println("✅ 처리 완료: " + count + "건");
            System.out.println("⏱️ 총 소요 시간: " + (endTime - startTime) / 1000.0 + "초");

        } catch (SQLException e) {
            System.out.println("SQL 오류: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.out.println("기타 오류: " + e.getMessage());
            e.printStackTrace();
        } finally {
            DBUtil.closeAll(conn, stmt, rs);
        }
    }

    // 쿠폰 코드 계산
    private static String getCouponCode(int credit, String gender, String address) {
        if (credit < 1000) return "AA";
        if (credit <= 2999) return "BB";
        if (credit <= 3999) {
            if ("F".equalsIgnoreCase(gender) &&
                    address != null &&
                    address.contains("송파구") &&
                    address.contains("풍납1동")) {
                return "C2";
            }
            return "CC";
        }
        return "DD";
    }
}
