package com.yxq.flinkcdc.mysql;

import java.sql.*;

/**
 * @author yxq
 * @date 2022-10-18
 */
public class MySqlDBUtils {

    private static PreparedStatement ps = null;
    private static Connection conn = null;

    private static final String DRIVER_NAME = "com.mysql.jdbc.Driver";
    private static final String URL_TEMPLATE = "jdbc:mysql://%s/jeecg?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC&autoReconnect=true&failOverReadOnly=false";
    private static final String hostPort = "localhost:3306";
    private static final String username = "root";
    private static final String password = "root";

    public static void executeSql(String sql) throws Exception {
        if (conn == null) {
            init();
        }
        ps.executeUpdate(sql);
    }

    public static void init() {
        if (conn == null) {
            try {
                conn = getConnection(hostPort, username, password);
                conn.setAutoCommit(true);
                ps = conn.prepareStatement("select 1+1;");
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }

    public static Connection getConnection(String host, String username, String password) {
        Connection conn = null;
        try {
            Class.forName(DRIVER_NAME);
            conn = DriverManager.getConnection(String.format(URL_TEMPLATE, host), username, password);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static void release(Connection conn, Statement st, ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            rs = null;
        }
        if (st != null) {
            try {
                st.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
