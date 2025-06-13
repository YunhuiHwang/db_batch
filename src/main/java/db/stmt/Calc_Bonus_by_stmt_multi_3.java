package db.stmt;

import db.util.DBUtil;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.concurrent.*;

public class Calc_Bonus_by_stmt_multi_3 {

    private static final String YYYMM = "202506";
    private static final int THREAD_COUNT = 8;

    public static void main(String[] args) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long startTime = System.currentTimeMillis();
        System.out.println("시작 시간: " + format.format(new Date(startTime)));

        List<Map<String, Object>> customerList = new ArrayList<>();

        // 1. 2018년 1월 1일 이후 가입 고객 조회
        try (Connection conn = DBUtil.getConnection();
             Statement stmt = conn.createStatement()) {

            stmt.setFetchSize(10);
            String query = "SELECT * FROM CUSTOMER WHERE ENROLL_DT >= TO_DATE('2018-01-01','YYYY-MM-DD')";
            ResultSet rs = stmt.executeQuery(query);

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                row.put("ID", rs.getString("ID"));
                row.put("EMAIL", rs.getString("EMAIL"));
                row.put("GENDER", rs.getString("GENDER"));
                row.put("ADDRESS1", rs.getString("ADDRESS1"));
                row.put("ADDRESS2", rs.getString("ADDRESS2"));
                row.put("CREDIT_LIMIT", rs.getInt("CREDIT_LIMIT"));
                customerList.add(row);
            }
            rs.close();

            stmt.executeUpdate("TRUNCATE TABLE BONUS_COUPON");
            System.out.println("🧹 BONUS_COUPON 테이블 초기화 완료");

        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        // 2. 병렬 처리 분할
        int partitionSize = (customerList.size() + THREAD_COUNT - 1) / THREAD_COUNT;
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        List<Future<Integer>> futures = new ArrayList<>();

        for (int i = 0; i < THREAD_COUNT; i++) {
            int fromIndex = i * partitionSize;
            int toIndex = Math.min(fromIndex + partitionSize, customerList.size());
            List<Map<String, Object>> subList = customerList.subList(fromIndex, toIndex);
            futures.add(executor.submit(() -> processCustomers(subList)));
        }

        // 3. 결과 수집
        int totalInsert = 0;
        try {
            for (Future<Integer> future : futures) {
                totalInsert += future.get();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            executor.shutdown();
        }

        // 4. 처리 시간 출력
        long endTime = System.currentTimeMillis();
        System.out.println("✅ 최종 처리 완료: 총 " + totalInsert + "건");
        System.out.printf("⏱ 총 처리 시간: %.3f초\n", (endTime - startTime) / 1000.0);
        System.out.println("종료 시간: " + format.format(new Date(endTime)));
    }

    private static int processCustomers(List<Map<String, Object>> customers) {
        int insertCount = 0;

        try (Connection conn = DBUtil.getConnection();
             Statement stmt = conn.createStatement()) {

            conn.setAutoCommit(false);

            for (Map<String, Object> row : customers) {
                String id = (String) row.get("ID");
                String email = (String) row.get("EMAIL");
                String gender = (String) row.get("GENDER");
                String address1 = (String) row.get("ADDRESS1");
                String address2 = (String) row.get("ADDRESS2");
                int credit = (int) row.get("CREDIT_LIMIT");

                String couponCode;
                if (credit < 1000) couponCode = "AA";
                else if (credit < 3000) couponCode = "BB";
                else if (credit < 4000) couponCode = "CC";
                else couponCode = "DD";

                if ("CC".equals(couponCode) &&
                        address1 != null && address1.contains("송파구") &&
                        address2 != null && address2.contains("풍납 1동") &&
                        "F".equalsIgnoreCase(gender)) {
                    couponCode = "C2";
                }

                String insertSQL = String.format(
                        "INSERT INTO BONUS_COUPON (YYYYMM, CUSTOMER_ID, EMAIL, COUPON_CD, CREDIT_POINT, SEND_DT, RECEIVE_DT, USE_DT) " +
                                "VALUES ('%s', '%s', '%s', '%s', %d, SYSDATE, NULL, NULL)",
                        YYYMM, id, email, couponCode, credit
                );

                try {
                    stmt.executeUpdate(insertSQL);
                    conn.commit();
                    insertCount++;

                    if (insertCount % 10000 == 0) {
                        System.out.println("📌 [" + Thread.currentThread().getName() + "] 현재까지 처리된 건수: " + insertCount);
                    }

                } catch (SQLException e) {
                    conn.rollback();
                    System.err.println("[Insert 실패] 고객 ID: " + id);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return insertCount;
    }
}
