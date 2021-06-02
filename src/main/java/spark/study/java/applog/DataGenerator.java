package spark.study.java.applog;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class DataGenerator {
	
	public static void main(String[] args) throws Exception {
		Random random = new Random();
		
		// 生成100个deviceID
		List<String> deviceIDs = new ArrayList<String>();
		for(int i = 0; i < 100; i++) {
			deviceIDs.add(getRandomUUID());
		}

		//线程安全，没有StringBuilder高效，但必String高效的字符缓存流
		StringBuffer buffer = new StringBuffer("");  
		
		for(int i = 0; i < 1000; i++) {
			// 生成随机时间戳
			Calendar cal = Calendar.getInstance();
			//日历设置当前的时间
			cal.setTime(new Date());
			//这个是对里面的分钟随机减掉1到600分之间的一个数
			cal.add(Calendar.MINUTE, -random.nextInt(600)); 
			//long timestamp = cal.getTime().getTime();
			Date date = cal.getTime();//从日历中获取日期
			long timestamp = date.getTime();//从日期中获取毫秒值

			// 生成随机deviceID
			String deviceID = deviceIDs.get(random.nextInt(100));  
			
			// 生成随机的上行流量
			long upTraffic = random.nextInt(100000);
			// 生成随机的下行流量
			long downTraffic = random.nextInt(100000);
			
			buffer.append(timestamp).append("\t")  
					.append(deviceID).append("\t")  
					.append(upTraffic).append("\t")
					.append(downTraffic).append("\n");  
		}

		//打印流
		PrintWriter pw = null;  
		try {
			//FileOutputSteam(文件字节输出流)----->OutputStreamWriter（字符转化流）------->PrintWriter（字符打印流）
			pw = new PrintWriter(new OutputStreamWriter(
					new FileOutputStream("./src/main/java/spark/study/java/applog/access.log")));
			pw.write(buffer.toString());  
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			pw.close();
		}
	}

	//获取随机字符串
	private static String getRandomUUID() {
		return UUID.randomUUID().toString().replace("-", "");
	}
	
}
