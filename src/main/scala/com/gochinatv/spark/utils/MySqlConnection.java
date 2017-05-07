package com.gochinatv.spark.utils;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by zhuhuihui on 17/5/7.
 */
public class MySqlConnection {

    private MySqlConnection(){}

    private static int maxPool = 10;

    private static LinkedList<Connection> connectionPools = null;

    static{
        init();
    }

    private static Connection createConnection(){
        Connection conn = null;
        String url="jdbc:mysql://localhost:3306/accelarator";
        String user="upenv";
        String pwd="upenv";
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try {
            conn= DriverManager.getConnection(url,user,pwd);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }


    private static void init(){
        connectionPools = new LinkedList<Connection>();
        for(int i=0;i<maxPool;i++) {
            Connection conn = createConnection();
            connectionPools.push(conn);
        }
    }


    public static void batchInsert(List<String> batchData){
        Connection conn = getConnection();
        PreparedStatement statement = null;
        try {
            statement = conn.prepareStatement("insert into visited(agree_id,ts,count,value) values (?,?,?,?)");
            for(int i=0;i<batchData.size();i++){
                String data = batchData.get(i);
                    String[] col = data.split(",");
                    statement.setString(1,col[0]+"");
                    statement.setString(2,col[1]+"");
                    statement.setInt(3,Integer.parseInt(col[2]+""));
                    statement.setInt(4,Integer.parseInt(col[3]+""));
                    statement.addBatch();
                }
            statement.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            try {
                if(statement!=null && !statement.isClosed())
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        close(conn);
    }

    public static Connection getConnection(){
        Connection conn = null;
        if(null==connectionPools){
            init();
        }
        if(connectionPools.size()>0){
            conn = connectionPools.poll();
        }else{
            try {
                Thread.sleep(1000);
                conn = getConnection();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return conn;
    }


    public static void close(Connection conn){
        connectionPools.push(conn);
    }


    public static void main(String[] args){
        List<String> list = new ArrayList<>();
        list.add("1234,2017-04-09,100,300");
        MySqlConnection.batchInsert(list);
    }

}
