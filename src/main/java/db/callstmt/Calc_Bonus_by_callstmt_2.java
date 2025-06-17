package db.callstmt;

import db.util.DBUtil;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Calc_Bonus_by_callstmt_2 {

    private static final String YYYYMM = "202506";
    private static final int COMMIT_UNIT = 10000;

    public static void main(String[] args) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long startTime = System.currentTimeMillis();
        System.out.println("시작 시간: " + format.format(new Date(startTime)));

        String plsql =
                "DECLARE\n" +
                        "    CURSOR c_customer IS\n" +
                        "        SELECT ID, EMAIL, CREDIT_LIMIT, GENDER, ADDRESS1, ADDRESS2\n" +
                        "        FROM CUSTOMER\n" +
                        "        WHERE ENROLL_DT >= TO_DATE('2018-01-01', 'YYYY-MM-DD');\n" +
                        "    TYPE t_customer IS TABLE OF c_customer%ROWTYPE INDEX BY PLS_INTEGER;\n" +
                        "    TYPE t_coupon IS TABLE OF VARCHAR2(10) INDEX BY PLS_INTEGER;\n" +
                        "    v_customers t_customer;\n" +
                        "    v_coupon_codes t_coupon;\n" +
                        "    v_count INTEGER := 0;\n" +
                        "    v_limit CONSTANT PLS_INTEGER := " + COMMIT_UNIT + ";\n" +
                        "BEGIN\n" +
                        "    OPEN c_customer;\n" +
                        "    LOOP\n" +
                        "        FETCH c_customer BULK COLLECT INTO v_customers LIMIT v_limit;\n" +
                        "        EXIT WHEN v_customers.COUNT = 0;\n" +
                        "        FOR i IN 1 .. v_customers.COUNT LOOP\n" +
                        "            IF v_customers(i).CREDIT_LIMIT < 1000 THEN\n" +
                        "                v_coupon_codes(i) := 'AA';\n" +
                        "            ELSIF v_customers(i).CREDIT_LIMIT < 3000 THEN\n" +
                        "                v_coupon_codes(i) := 'BB';\n" +
                        "            ELSIF v_customers(i).CREDIT_LIMIT < 4000 THEN\n" +
                        "                IF v_customers(i).GENDER = 'F'\n" +
                        "                   AND INSTR(v_customers(i).ADDRESS1, '송파구') > 0\n" +
                        "                   AND INSTR(v_customers(i).ADDRESS2, '풍납 1동') > 0 THEN\n" +
                        "                    v_coupon_codes(i) := 'C2';\n" +
                        "                ELSE\n" +
                        "                    v_coupon_codes(i) := 'CC';\n" +
                        "                END IF;\n" +
                        "            ELSE\n" +
                        "                v_coupon_codes(i) := 'DD';\n" +
                        "            END IF;\n" +
                        "        END LOOP;\n" +
                        "        FORALL i IN INDICES OF v_customers\n" +
                        "            INSERT INTO BONUS_COUPON (\n" +
                        "                YYYYMM, CUSTOMER_ID, EMAIL, COUPON_CD, CREDIT_POINT, SEND_DT, RECEIVE_DT, USE_DT\n" +
                        "            ) VALUES (\n" +
                        "                '" + YYYYMM + "',\n" +
                        "                v_customers(i).ID,\n" +
                        "                v_customers(i).EMAIL,\n" +
                        "                v_coupon_codes(i),\n" +
                        "                v_customers(i).CREDIT_LIMIT,\n" +
                        "                SYSDATE, NULL, NULL\n" +
                        "            );\n" +
                        "        v_count := v_count + v_customers.COUNT;\n" +
                        "        COMMIT;\n" +
                        "    END LOOP;\n" +
                        "    CLOSE c_customer;\n" +
                        "    ? := v_count;\n" +  // OUT 파라미터로 반환
                        "END;";

        try (Connection conn = DBUtil.getConnection();
             Statement clear = conn.createStatement();
             CallableStatement callStmt = conn.prepareCall(plsql)) {

            clear.execute("TRUNCATE TABLE BONUS_COUPON");
            System.out.println("🧹 BONUS_COUPON 테이블 초기화 완료");

            callStmt.registerOutParameter(1, Types.INTEGER);  // OUT 파라미터 등록
            callStmt.execute();

            int totalInserted = callStmt.getInt(1);  // 결과 가져오기
            System.out.println("✅ 최종 처리 완료: 총 " + totalInserted + "건");

        } catch (SQLException e) {
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();
        System.out.printf("⏱ 총 처리 시간: %.3f초\n", (endTime - startTime) / 1000.0);
        System.out.println("종료 시간: " + format.format(new Date(endTime)));
    }
}
