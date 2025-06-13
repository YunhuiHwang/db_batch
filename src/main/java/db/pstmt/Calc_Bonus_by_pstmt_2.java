package db.pstmt;

import db.util.DBUtil;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.concurrent.*;

public class Calc_Bonus_by_pstmt_2 {

    private static final String YYYYMM = "202506";
    private static final int THREAD_COUNT = 8;
    private static final int BATCH_UNIT = 1000;
    private static final int FETCH_SIZE = 1000;

    public static void main(String[] args) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long startTime = System.currentTimeMillis();
        System.out.println("시작 시간: " + format.format(new Date(startTime)));

        List<Map<String, Object>> customerList = new ArrayList<>();

        // Step 1: 고객 정보 로딩 + 테이블 초기화
        try (Connection conn = DBUtil.getConnection();
             Statement stmt = conn.createStatement()) {

            stmt.setFetchSize(FETCH_SIZE);
            ResultSet rs = stmt.executeQuery(
                    "SELECT * FROM CUSTOMER WHERE ENROLL_DT >= TO_DATE('2018-01-01','YYYY-MM-DD')"
            );
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

        // Step 2: 병렬 처리
        int partitionSize = (customerList.size() + THREAD_COUNT - 1) / THREAD_COUNT;
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        List<Future<Integer>> futures = new ArrayList<>();

        for (int i = 0; i < THREAD_COUNT; i++) {
            int fromIndex = i * partitionSize;
            int toIndex = Math.min(fromIndex + partitionSize, customerList.size());
            List<Map<String, Object>> subList = customerList.subList(fromIndex, toIndex);
            futures.add(executor.submit(() -> processCustomers(subList)));
        }

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

        long endTime = System.currentTimeMillis();
        System.out.println("✅ 총 발급된 쿠폰 수: " + totalInsert);
        System.out.printf("⏱ 총 처리 시간: %.3f초\n", (endTime - startTime) / 1000.0);
        System.out.println("종료 시간: " + format.format(new Date(endTime)));
    }

    private static int processCustomers(List<Map<String, Object>> customers) {
        int insertCount = 0;
        int batchCount = 0;

        try (Connection conn = DBUtil.getConnection()) {
            conn.setAutoCommit(false);

            String insertSQL = "INSERT INTO BONUS_COUPON " +
                    "(YYYYMM, CUSTOMER_ID, EMAIL, COUPON_CD, CREDIT_POINT, SEND_DT, RECEIVE_DT, USE_DT) " +
                    "VALUES (?, ?, ?, ?, ?, SYSDATE, NULL, NULL)";

            try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
                for (Map<String, Object> row : customers) {
                    String id = (String) row.get("ID");
                    String email = (String) row.get("EMAIL");
                    String gender = (String) row.get("GENDER");
                    String address1 = (String) row.get("ADDRESS1");
                    String address2 = (String) row.get("ADDRESS2");
                    int credit = (int) row.get("CREDIT_LIMIT");

                    String couponCode = switchCouponCode(credit, gender, address1, address2);

                    pstmt.setString(1, YYYYMM);
                    pstmt.setString(2, id);
                    pstmt.setString(3, email);
                    pstmt.setString(4, couponCode);
                    pstmt.setInt(5, credit);
                    pstmt.addBatch();

                    batchCount++;
                    insertCount++;

                    if (batchCount == BATCH_UNIT) {
                        pstmt.executeBatch();
                        conn.commit();
                        System.out.printf("📌 [%s] %,d건 커밋 완료\n",
                                Thread.currentThread().getName(), insertCount);
                        batchCount = 0;
                    }
                }

                if (batchCount > 0) {
                    pstmt.executeBatch();
                    conn.commit();
                    System.out.printf("📌 [%s] 나머지 %,d건 커밋 완료\n",
                            Thread.currentThread().getName(), batchCount);
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return insertCount;
    }

    private static String switchCouponCode(int credit, String gender, String addr1, String addr2) {
        String code;
        if (credit < 1000) code = "AA";
        else if (credit < 3000) code = "BB";
        else if (credit < 4000) code = "CC";
        else code = "DD";

        if ("CC".equals(code) &&
                "F".equalsIgnoreCase(gender) &&
                addr1 != null && addr1.contains("송파구") &&
                addr2 != null && addr2.contains("풍납 1동")) {
            code = "C2";
        }

        return code;
    }
}
