package db.callstmt;

import db.util.DBUtil;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Calc_Bonus_by_callstmt_3 {

    private static final String YYYYMM = "202506";

    public static void main(String[] args) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long startTime = System.currentTimeMillis();
        System.out.println("시작 시간: " + format.format(new Date(startTime)));

        String sql =
                "INSERT INTO BONUS_COUPON (\n" +
                        "    YYYYMM, CUSTOMER_ID, EMAIL, COUPON_CD, CREDIT_POINT, SEND_DT, RECEIVE_DT, USE_DT\n" +
                        ") \n" +
                        "SELECT\n" +
                        "    '" + YYYYMM + "',\n" +
                        "    ID,\n" +
                        "    EMAIL,\n" +
                        "    CASE\n" +
                        "        WHEN CREDIT_LIMIT < 1000 THEN 'AA'\n" +
                        "        WHEN CREDIT_LIMIT < 3000 THEN 'BB'\n" +
                        "        WHEN CREDIT_LIMIT < 4000 AND GENDER = 'F'\n" +
                        "             AND INSTR(ADDRESS1, '송파구') > 0\n" +
                        "             AND INSTR(ADDRESS2, '풍납 1동') > 0 THEN 'C2'\n" +
                        "        WHEN CREDIT_LIMIT < 4000 THEN 'CC'\n" +
                        "        ELSE 'DD'\n" +
                        "    END AS COUPON_CD,\n" +
                        "    CREDIT_LIMIT,\n" +
                        "    SYSDATE,\n" +
                        "    NULL,\n" +
                        "    NULL\n" +
                        "FROM CUSTOMER\n" +
                        "WHERE ENROLL_DT >= TO_DATE('2013-01-01', 'YYYY-MM-DD')";

        try (Connection conn = DBUtil.getConnection();
             Statement clear = conn.createStatement();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            clear.execute("TRUNCATE TABLE BONUS_COUPON");
            System.out.println("🧹 BONUS_COUPON 테이블 초기화 완료");

            int inserted = pstmt.executeUpdate();
            System.out.println("✅ 최종 처리 완료: 총 " + inserted + "건");

        } catch (SQLException e) {
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();
        System.out.printf("⏱ 총 처리 시간: %.3f초\n", (endTime - startTime) / 1000.0);
        System.out.println("종료 시간: " + format.format(new Date(endTime)));
    }
}
