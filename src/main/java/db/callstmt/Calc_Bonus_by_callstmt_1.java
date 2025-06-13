package db.callstmt;

import db.util.DBUtil;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Calc_Bonus_by_callstmt_1 {

    private static final String YYYYMM = "202506";
    private static final int COMMIT_UNIT = 10000;

    public static void main(String[] args) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long startTime = System.currentTimeMillis();
        System.out.println("시작 시간: " + format.format(new Date(startTime)));

        int totalCount = 0;

        String plsql =
                "DECLARE\n" +
                        "    CURSOR c_customer IS\n" +
                        "        SELECT ID, EMAIL, CREDIT_LIMIT, GENDER, ADDRESS1, ADDRESS2\n" +
                        "        FROM CUSTOMER\n" +
                        "        WHERE ENROLL_DT >= TO_DATE('2018-01-01', 'YYYY-MM-DD');\n" +
                        "    v_count INTEGER := 0;\n" +
                        "    v_coupon_cd VARCHAR2(10);\n" +
                        "BEGIN\n" +
                        "    FOR rec IN c_customer LOOP\n" +
                        "        IF rec.CREDIT_LIMIT < 1000 THEN\n" +
                        "            v_coupon_cd := 'AA';\n" +
                        "        ELSIF rec.CREDIT_LIMIT < 3000 THEN\n" +
                        "            v_coupon_cd := 'BB';\n" +
                        "        ELSIF rec.CREDIT_LIMIT < 4000 THEN\n" +
                        "            IF rec.GENDER = 'F' AND\n" +
                        "               INSTR(rec.ADDRESS1, '송파구') > 0 AND\n" +
                        "               INSTR(rec.ADDRESS2, '풍납 1동') > 0 THEN\n" +
                        "                v_coupon_cd := 'C2';\n" +
                        "            ELSE\n" +
                        "                v_coupon_cd := 'CC';\n" +
                        "            END IF;\n" +
                        "        ELSE\n" +
                        "            v_coupon_cd := 'DD';\n" +
                        "        END IF;\n" +
                        "        INSERT INTO BONUS_COUPON (\n" +
                        "            YYYYMM, CUSTOMER_ID, EMAIL, COUPON_CD, CREDIT_POINT, SEND_DT, RECEIVE_DT, USE_DT\n" +
                        "        ) VALUES (\n" +
                        "            '" + YYYYMM + "', rec.ID, rec.EMAIL, v_coupon_cd, rec.CREDIT_LIMIT, SYSDATE, NULL, NULL\n" +
                        "        );\n" +
                        "        v_count := v_count + 1;\n" +
                        "        IF MOD(v_count, " + COMMIT_UNIT + ") = 0 THEN\n" +
                        "            COMMIT;\n" +
                        "            DBMS_OUTPUT.PUT_LINE('📌 처리 건수: ' || v_count);\n" +
                        "        END IF;\n" +
                        "    END LOOP;\n" +
                        "    COMMIT;\n" +
                        "    DBMS_OUTPUT.PUT_LINE('✅ 최종 처리 완료: ' || v_count || '건');\n" +
                        "END;";


        try (Connection conn = DBUtil.getConnection();
             Statement clear = conn.createStatement();
             CallableStatement callStmt = conn.prepareCall(plsql)) {

            clear.execute("TRUNCATE TABLE BONUS_COUPON");
            System.out.println("🧹 BONUS_COUPON 테이블 초기화 완료");

            // DBMS_OUTPUT 활성화 (옵션)
            callStmt.execute();
            System.out.println("✅ PL/SQL 실행 완료");

        } catch (SQLException e) {
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();
        System.out.printf("⏱ 총 처리 시간: %.3f초\n", (endTime - startTime) / 1000.0);
        System.out.println("종료 시간: " + format.format(new Date(endTime)));
    }
}
