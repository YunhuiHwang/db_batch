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

        String plsql = """
                DECLARE
                    CURSOR c_customer IS
                        SELECT ID, EMAIL, CREDIT_LIMIT, GENDER, ADDRESS1, ADDRESS2
                        FROM CUSTOMER
                        WHERE ENROLL_DT >= TO_DATE('2018-01-01', 'YYYY-MM-DD');
                
                    v_count INTEGER := 0;
                    v_coupon_cd VARCHAR2(10);
                BEGIN
                    FOR rec IN c_customer LOOP
                        IF rec.CREDIT_LIMIT < 1000 THEN
                            v_coupon_cd := 'AA';
                        ELSIF rec.CREDIT_LIMIT < 3000 THEN
                            v_coupon_cd := 'BB';
                        ELSIF rec.CREDIT_LIMIT < 4000 THEN
                            IF rec.GENDER = 'F' AND
                               INSTR(rec.ADDRESS1, '송파구') > 0 AND
                               INSTR(rec.ADDRESS2, '풍납 1동') > 0 THEN
                                v_coupon_cd := 'C2';
                            ELSE
                                v_coupon_cd := 'CC';
                            END IF;
                        ELSE
                            v_coupon_cd := 'DD';
                        END IF;
                
                        INSERT INTO BONUS_COUPON (
                            YYYYMM, CUSTOMER_ID, EMAIL, COUPON_CD, CREDIT_POINT, SEND_DT, RECEIVE_DT, USE_DT
                        ) VALUES (
                            '""" + YYYYMM + """', rec.ID, rec.EMAIL, v_coupon_cd, rec.CREDIT_LIMIT, SYSDATE, NULL, NULL
                );
                
                v_count := v_count + 1;
                
                IF MOD(v_count, """ + COMMIT_UNIT + """) = 0 THEN
                            COMMIT;
                            DBMS_OUTPUT.PUT_LINE('📌 처리 건수: ' || v_count);
                        END IF;
                    END LOOP;
                    COMMIT;
                    DBMS_OUTPUT.PUT_LINE('✅ 최종 처리 완료: ' || v_count || '건');
                END;
                """;

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
