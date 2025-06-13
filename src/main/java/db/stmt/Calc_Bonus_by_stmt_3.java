package db.stmt;

import db.util.DBUtil;

import java.sql.*;

public class Calc_Bonus_by_stmt_3 {

    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        Statement insertStmt = null;

        try {
            long startTime = System.currentTimeMillis();

            conn = DBUtil.getConnection();
            conn.setAutoCommit(false);

            stmt = conn.createStatement();
            insertStmt = conn.createStatement();

            // 기존 쿠폰 테이블 초기화
            stmt.executeUpdate("TRUNCATE TABLE BONUS_COUPON");

            // 조건 필터링된 고객 데이터만 조회
            String selectSql = "SELECT ID, EMAIL, CREDIT_LIMIT, GENDER, ADDRESS1 " +
                    "FROM CUSTOMER " +
                    "WHERE ENROLL_DT >= TO_DATE('2018-01-01', 'YYYY-MM-DD') " +
                    "AND CREDIT_LIMIT IS NOT NULL " +
                    "AND EMAIL IS NOT NULL";
            rs = stmt.executeQuery(selectSql);

            int count = 0;

            while (rs.next()) {
                String customerId = rs.getString("ID");
                String email = rs.getString("EMAIL");
                int credit = rs.getInt("CREDIT_LIMIT");
                String gender = rs.getString("GENDER");
                String address = rs.getString("ADDRESS1");

                String couponCd = getCouponCode(credit, gender, address);

                String insertSql = String.format(
                        "INSERT INTO BONUS_COUPON (YYYYMM, CUSTOMER_ID, EMAIL, COUPON_CD, CREDIT_POINT, SEND_DT) " +
                                "VALUES ('202506', '%s', '%s', '%s', %d, SYSDATE)",
                        customerId, email, couponCd, credit
                );
                insertStmt.executeUpdate(insertSql);
                conn.commit();
                count++;

                if (count % 10000 == 0) {
                    System.out.println("📌 현재까지 처리된 건수: " + count);
                }
            }

            long endTime = System.currentTimeMillis();
            System.out.println("✅ 최종 처리 완료: " + count + "건");
            System.out.println("⏱️ 총 소요 시간: " + (endTime - startTime) / 1000.0 + "초");

        } catch (SQLException e) {
            System.out.println("❌ SQL 오류: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.out.println("❌ 기타 오류: " + e.getMessage());
            e.printStackTrace();
        } finally {
            DBUtil.close(rs);
            DBUtil.close(stmt);
            DBUtil.close(insertStmt);
            DBUtil.close(conn);
        }
    }

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
