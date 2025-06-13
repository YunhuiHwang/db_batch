package db.pstmt;

import db.util.DBUtil;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.concurrent.*;

public class Calc_Bonus_by_pstmt_1 {

    private static final int THREAD_COUNT = 8;
    private static final int COMMIT_UNIT = 10000;
    private static final int FETCH_SIZE = 1000;
    private static final String YYYMM = "202506";

    public static void main(String[] args) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long startTime = System.currentTimeMillis();
        System.out.println("시작 시간: " + format.format(new Date(startTime)));

        List<Map<String, Object>> customerList = new ArrayList<>();

        // Step 1: 고객 데이터 로딩 + BONUS_COUPON 테이블 초기화
        try (Connection conn = DBUtil.getConnection();
             Statement stmt = conn.createStatement()) {

            stmt.setFetchSize(FETCH_SIZE);
            ResultSet rs = stmt.executeQuery("SELECT * FROM CUSTOMER WHERE ENROLL_DT >= TO_DATE('2018-01-01','YYYY-MM-DD')");

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

        // Step 2: 병렬 처리 시작
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        int partitionSize = (customerList.size() + THREAD_COUNT - 1) / THREAD_COUNT;
        List<Future<Integer>> futures = new ArrayList<>();

        for (int i = 0; i < THREAD_COUNT; i++) {
            int fromIndex = i * partitionSize;
            int toIndex = Math.min(fromIndex + partitionSize, customerList.size());
            List<Map<String, Object>> subList = customerList.subList(fromIndex, toIndex);
            futures.add(executor.submit(() -> insertWithPreparedStatement(subList)));
        }

        executor.shutdown();

        // Step 3: 총 건수 집계
        int totalInsert = 0;
        try {
            for (Future<Integer> future : futures) {
                totalInsert += future.get();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Step 4: 처리 시간 출력
        long endTime = System.currentTimeMillis();
        System.out.println("✅ 총 발급된 쿠폰 수: " + totalInsert);
        System.out.printf("⏱ 총 처리 시간: %.3f 초\n", (endTime - startTime) / 1000.0);
        System.out.println("종료 시간: " + format.format(new Date(endTime)));
    }

    private static int insertWithPreparedStatement(List<Map<String, Object>> customers) {
        int insertCount = 0;

        try (Connection conn = DBUtil.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(
                     "INSERT INTO BONUS_COUPON (YYYYMM, CUSTOMER_ID, EMAIL, COUPON_CD, CREDIT_POINT, SEND_DT, RECEIVE_DT, USE_DT) " +
                             "VALUES (?, ?, ?, ?, ?, SYSDATE, NULL, NULL)")
        ) {
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

                if ("CC".equals(couponCode)
                        && "F".equalsIgnoreCase(gender)
                        && address1 != null && address1.contains("송파구")
                        && address2 != null && address2.contains("풍납 1동")) {
                    couponCode = "C2";
                }

                try {
                    pstmt.setString(1, YYYMM);
                    pstmt.setString(2, id);
                    pstmt.setString(3, email);
                    pstmt.setString(4, couponCode);
                    pstmt.setInt(5, credit);

                    pstmt.executeUpdate();
                    insertCount++;

                    if (insertCount % COMMIT_UNIT == 0) {
                        conn.commit();
                        System.out.printf("📌 [%s] %,d건 커밋 완료\n", Thread.currentThread().getName(), insertCount);
                    }

                } catch (SQLException e) {
                    conn.rollback();
                    System.err.printf("[Insert 실패] 고객 ID: %s (%s)\n", id, e.getMessage());
                }
            }

            // 마지막 커밋 처리
            if (insertCount % COMMIT_UNIT != 0) {
                conn.commit();
                System.out.printf("📌 [%s] 마지막 %,d건 커밋 완료\n", Thread.currentThread().getName(), insertCount % COMMIT_UNIT);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return insertCount;
    }
}
