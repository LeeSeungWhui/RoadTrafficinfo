package com.ybs.pullapidata.roadtrafficinfo;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.ybs.pullapidata.roadtrafficinfo.ApiConnection;
import com.ybs.pullapidata.roadtrafficinfo.DbConnection;

public class RoadTrafficInfo 
{
	static public ApiConnection apiconnection;
	public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException 
	{
		// 기본 설
		String BaseDate = "20180420";
		long totalSec = System.currentTimeMillis() / 1000;
		long currentSec = totalSec % 60;
		long totalMin = totalSec / 60;
		long currentMin = totalMin % 60;
		long totalHour = totalMin / 60;
		long currentHour = totalHour % 24;
		String BaseTime = String.valueOf(currentHour) + String.valueOf(currentMin) + String.valueOf(currentSec); 
		
		List<String> column = new ArrayList<String>();
		column.add("roadsectionid");
		column.add("avgspeed");
		column.add("roadnametext");
		column.add("startnodeid");
		column.add("endnodeid");		
		column.add("traveltime");
		column.add("generatedate");
		
		// DB 연결
		String host = "192.168.0.53";
		String name = "HVI_DB";
		String user = "root";
		String pass = "dlatl#001";
		DbConnection dbconnection = new DbConnection(host, name, user, pass);
	    dbconnection.Connect();
	    
	   // 쿼리 수행
	    String sql = "select max(cast(x_pos as int)) as MAX_X,min(cast(x_pos as int)) as MIN_X, max(cast(y_pos as int)) as MAX_Y,min(cast(y_pos as int)) as MIN_Y from STD_NODE";
	    dbconnection.runQuery(sql);
	    dbconnection.getResult().next();
	    // 결과 저장
	   	int max_x = dbconnection.getResult().getInt("MAX_X") + 1;
	   	int max_y = dbconnection.getResult().getInt("MAX_Y") + 1;
	   	int min_x = dbconnection.getResult().getInt("MIN_X");
	   	int min_y = dbconnection.getResult().getInt("MIN_Y");
	    
	   	System.out.println(min_x + " " + max_x + " " + min_y + " " + max_y);
	   	
	    // api data 받아서 csv파일 생성
	    String FileName = "ROAD_TRAFFIC_INFO_" + BaseDate + BaseTime + ".csv";
	    BufferedWriter bufWriter = new BufferedWriter(new FileWriter(FileName));
	    CreateCSV(bufWriter);
	    List<String> response;
	    apiconnection = new ApiConnection();
	    for(double x = min_x;  x < max_x; x+=0.5)
	    {
	    	for(double y = min_y; y < max_y; y+=0.5)
	    	{
//	    		int x = 126, y = 35;
	    		apiconnection.setUrl("http://openapi.its.go.kr/api/NTrafficInfo");
				apiconnection.setServiceKey("key", "1521509851552");
				apiconnection.urlAppender("ReqType", "2");
				apiconnection.urlAppender("MinX", String.valueOf(x));
				apiconnection.urlAppender("MaxX", String.valueOf(x+0.5));
				apiconnection.urlAppender("MinY", String.valueOf(y));
				apiconnection.urlAppender("MaxY", String.valueOf(y+0.5));
				apiconnection.pullData();
				response = apiconnection.getResult("response");
				System.out.println(apiconnection.urlBuilder);
				if(response.get(0).equalsIgnoreCase("NULL") != true)
				{
					//System.out.println(response.get(0));
					try
					{
						List<List<String>> datalist = new ArrayList<List<String>>();
						for(String s:column)
						{
							datalist.add(apiconnection.getResult(s));
						}
						WriteCSV(bufWriter, datalist);
					}
					catch(Exception e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
						y -= 0.5;
						continue;
					}
				}
	    	}
	    }
	    bufWriter.close();
	    
	    // DB에 입력
	    sql = "LOAD DATA LOCAL INFILE '" + FileName + "' INTO TABLE ROAD_TRAFFIC_INFO FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES";
	    dbconnection.LoadLocalData(sql);
	}
	
	public static void CreateCSV(BufferedWriter bufWriter)
	{
		try
		{
			bufWriter.write("\"ROADSECTIONID\",\" AVGSPEED\",\" STARTNODEID\",\" ROADNAME\",\" TRAVELTIME\",\" ENDNODEID\",\" GERATEDATE\"");
			bufWriter.newLine();
		} catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void WriteCSV(BufferedWriter bufWriter, List<List<String>> datalist) throws IOException
	{
//		System.out.println(datalist.get(0).size() + " " + datalist.get(1).size()+ " " + datalist.get(2).size()+ " " + datalist.get(3).size()+ " " + datalist.get(4).size()+ " " + datalist.get(5).size()+ " " + datalist.get(6).size());
		String buffer = "";
		for(int i = 0; i < datalist.get(0).size(); i++)
		{
			int j = 0;
			for(; j < datalist.size() - 1; j++)
			{
				if(datalist.get(j).get(i).contains("</"))
				{
					buffer += "\"" + datalist.get(j).get(i).substring(0,datalist.get(j).get(i).indexOf('<') ) + "\",";
				}
				else
				{
					buffer += "\"" + datalist.get(j).get(i) + "\",";
				}
			}
			if(datalist.get(j).get(i).contains("</"))
			{
				buffer += "\"" + datalist.get(j).get(i).substring(0,datalist.get(j).get(i).indexOf('<') );
			}
			else
			{
				buffer += "\"" + datalist.get(j).get(i);
			}
			buffer += "\"\n";
		}
		bufWriter.write(buffer);
	}
}
