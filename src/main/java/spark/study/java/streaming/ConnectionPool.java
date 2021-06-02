package spark.study.java.streaming;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;

/**
 * author:liangsir
 * qq:714628767
 * created 2019/01/21.
 * 简易版的连接池
 */
public class ConnectionPool {
    /*//造一个全局的静态的连接池队列
    private static LinkedList<Connection> connectionQueue = null;

    //注册驱动---必须是静态的
    static{
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
    //获取连接，多线程访问并发控制
    public synchronized static Connection getConnection(){
        try{
           if (connectionQueue == null){
               connectionQueue = new LinkedList<Connection>();
               for (int i = 0; i < 3; i++) {
                   Connection conn = DriverManager.getConnection("jdbc:mysql://192.168.33.11:3306/testdb","","");
                   connectionQueue.push(conn);
               }
           }
        }catch(Exception e){
            e.printStackTrace();
        }
       return connectionQueue.poll();
    }

    //归还线程
    public static void returnConnection(Connection conn){
        connectionQueue.push(conn);
    }
*/




    // 静态的Connection队列
    private static LinkedList<Connection> connectionQueue;

    /**
     * 加载驱动
     */
    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取连接，多线程访问并发控制
     * @return
     */
    public synchronized static Connection getConnection() {
        try {
            if(connectionQueue == null) {
                connectionQueue = new LinkedList<Connection>();
                for(int i = 0; i < 10; i++) {
                    Connection conn = DriverManager.getConnection(
                            "jdbc:mysql://spark1:3306/testdb",
                            "",
                            "");
                    connectionQueue.push(conn);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connectionQueue.poll();
    }

    /**
     * 还回去一个连接
     */
    public static void returnConnection(Connection conn) {
        connectionQueue.push(conn);
    }

}
