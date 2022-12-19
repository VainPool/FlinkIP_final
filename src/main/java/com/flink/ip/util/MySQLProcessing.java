package com.flink.ip.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class MySQLProcessing {
    private String mysqlUrl;
    private String mysqlDriverName;
    private String mysqlUserName;
    private String mysqlPassword;

    public MySQLProcessing(String mysqlUrl, String mysqlDriverName, String mysqlUserName, String mysqlPassword) {
        this.mysqlUrl = mysqlUrl;
        this.mysqlDriverName = mysqlDriverName;
        this.mysqlUserName = mysqlUserName;
        this.mysqlPassword = mysqlPassword;
    }

    public void executeDDL() throws ClassNotFoundException, SQLException {
        Connection connection;
        String url = mysqlUrl;
        String username = mysqlUserName;
        String password = mysqlPassword;

        Class.forName(mysqlDriverName);
        connection = DriverManager.getConnection(url,username,password);

        connection.setAutoCommit(false);
        String ddl1 ="CREATE TABLE IF NOT EXISTS `customer` (\n" +
                "  `C_CUSTKEY` bigint NOT NULL,\n" +
                "  `C_NAME` varchar(25) DEFAULT NULL,\n" +
                "  `C_ADDRESS` varchar(40) DEFAULT NULL,\n" +
                "  `C_NATIONKEY` bigint DEFAULT NULL,\n" +
                "  `C_PHONE` text,\n" +
                "  `C_ACCTBAL` double DEFAULT NULL,\n" +
                "  `C_MKTSEGMENT` text,\n" +
                "  `C_COMMENT` varchar(117) DEFAULT NULL,\n" +
                "  `perc` int DEFAULT NULL,\n" +
                "  PRIMARY KEY (`C_CUSTKEY`)\n" +
                ")";

        String ddl2 = "CREATE TABLE IF NOT EXISTS `lineitem` (\n" +
                "  `L_ORDERKEY` bigint DEFAULT NULL,\n" +
                "  `L_PARTKEY` bigint DEFAULT NULL,\n" +
                "  `L_SUPPKEY` bigint DEFAULT NULL,\n" +
                "  `L_LINENUMBER` int DEFAULT NULL,\n" +
                "  `L_QUANTITY` int DEFAULT NULL,\n" +
                "  `L_EXTENDEDPRICE` double DEFAULT NULL,\n" +
                "  `L_DISCOUNT` double DEFAULT NULL,\n" +
                "  `L_TAX` double DEFAULT NULL,\n" +
                "  `L_RETURNFLAG` text,\n" +
                "  `L_LINESTATUS` text,\n" +
                "  `L_SHIPDATE` date DEFAULT NULL,\n" +
                "  `L_COMMITDATE` date DEFAULT NULL,\n" +
                "  `L_RECEIPTDATE` date DEFAULT NULL,\n" +
                "  `L_SHIPINSTRUCT` text,\n" +
                "  `L_SHIPMODE` text,\n" +
                "  `L_COMMENT` varchar(44) DEFAULT NULL,\n" +
                "  `perc` int DEFAULT NULL\n" +
                ")";
        String ddl3 = "CREATE TABLE IF NOT EXISTS `nation` (\n" +
                "  `N_NATIONKEY` bigint NOT NULL,\n" +
                "  `N_NAME` text,\n" +
                "  `N_REGIONKEY` bigint DEFAULT NULL,\n" +
                "  `N_COMMENT` varchar(152) DEFAULT NULL,\n" +
                "  `perc` int DEFAULT NULL,\n" +
                "  PRIMARY KEY (`N_NATIONKEY`)\n" +
                ")";

        String ddl4 = "CREATE TABLE IF NOT EXISTS `orders` (\n" +
                "  `O_ORDERKEY` bigint NOT NULL,\n" +
                "  `O_CUSTKEY` bigint DEFAULT NULL,\n" +
                "  `O_ORDERSTATUS` text,\n" +
                "  `O_TOTALPRICE` double DEFAULT NULL,\n" +
                "  `O_ORDERDATE` date DEFAULT NULL,\n" +
                "  `O_ORDERPRIORITY` text,\n" +
                "  `O_CLERK` text,\n" +
                "  `O_SHIPPRIORITY` int DEFAULT NULL,\n" +
                "  `O_COMMENT` varchar(79) DEFAULT NULL,\n" +
                "  `perc` int DEFAULT NULL,\n" +
                "  PRIMARY KEY (`O_ORDERKEY`)\n" +
                ")";

        String ddl5 = "CREATE TABLE IF NOT EXISTS `region` (\n" +
                "  `R_REGIONKEY` bigint NOT NULL,\n" +
                "  `R_NAME` text,\n" +
                "  `R_COMMENT` varchar(152) DEFAULT NULL,\n" +
                "  `perc` int DEFAULT NULL,\n" +
                "  PRIMARY KEY (`R_REGIONKEY`)\n" +
                ")";
        String ddl6 = "CREATE TABLE IF NOT EXISTS `supplier` (\n" +
                "  `S_SUPPKEY` bigint NOT NULL,\n" +
                "  `S_NAME` text,\n" +
                "  `S_ADDRESS` varchar(40) DEFAULT NULL,\n" +
                "  `S_NATIONKEY` bigint DEFAULT NULL,\n" +
                "  `S_PHONE` text,\n" +
                "  `S_ACCTBAL` double DEFAULT NULL,\n" +
                "  `S_COMMENT` varchar(101) DEFAULT NULL,\n" +
                "  `perc` int DEFAULT NULL,\n" +
                "  PRIMARY KEY (`S_SUPPKEY`)\n" +
                ")";
        String [] sqls = new String[6];
        sqls[0] = ddl1;
        sqls[1] = ddl2;
        sqls[2] = ddl3;
        sqls[3] = ddl4;
        sqls[4] = ddl5;
        sqls[5] = ddl6;

        Statement statement = connection.createStatement();
        for (int i = 0; i < sqls.length; i++){
            statement.addBatch(sqls[i]);
        }
        statement.executeBatch();
        connection.commit();
        statement.close();
        connection.close();
    }

    public void executeDelete() throws ClassNotFoundException, SQLException {
        Connection connection;
        String url = mysqlUrl;
        String username = mysqlUserName;
        String password = mysqlPassword;

        Class.forName(mysqlDriverName);
        connection = DriverManager.getConnection(url,username,password);

        connection.setAutoCommit(false);

        String [] tables = new String[6];
        tables[0] = "customer";
        tables[1] = "nation";
        tables[2] = "lineitem";
        tables[3] = "orders";
        tables[4] = "region";
        tables[5] = "supplier";
        String sql = "delete from ";
        Statement statement = connection.createStatement();
        for (String i : tables){
            statement.addBatch(sql+i);
        }
        statement.executeBatch();
        connection.commit();
        statement.close();
        connection.close();
    }
}
