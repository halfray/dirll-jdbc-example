package com.halfray.bigbata;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DrillConnection {

	public static void main(String[] args) throws SQLException {

		try {
			// 加载MySql的驱动类
			Class.forName("org.apache.drill.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("找不到驱动程序类 ，加载驱动失败！");
			e.printStackTrace();
		}

		String url = "jdbc:drill:zk=" + "10.0.30.101," + "10.0.30.102,"
				+ "10.0.30.103," + "10.0.30.104," + "10.0.30.105,"
				+ "10.0.30.106," + "10.0.30.107," + "10.0.30.108,"
				+ "10.0.30.109," + "10.0.30.110;" + "schema=hive";
		String username = "admin";
		String password = "admin";
		Connection con = DriverManager.getConnection(url, username, password);
		// 查询结果
		Statement stmt = con.createStatement();
		String sql = "select * from hive.`remaworks_2_0_sichuan`.`tb_d_m_dns_log` order by rr_x112 desc limit 1000";
		ResultSet rs = stmt.executeQuery(sql);
		System.out.println("内容列表:");
		try {
			if (rs != null) {
				while (rs.next()) {
					System.out.println(rs.getString("ws_0005"));
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			stmt.close();
			con.close();
		}
	}
}
