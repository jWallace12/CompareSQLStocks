// Name: Jonah Wallace
// WWU - Winter 2017
// CSCI 330 - Assignment #3

// Analyze and compare stock data and record the results in a new database

import java.util.Properties; 
import java.util.Scanner; 
import java.io.FileInputStream; 
import java.sql.*;
import java.lang.Object.*;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;


public class Assignment3 {
   
   // connection
      static Connection conn = null;
      
      // main method
      public static void main(String[] args) throws Exception {
            // take in parameter files, initialize connections
            String paramsFile1 = "readerparams.txt";                             
            String paramsFile2 = "writerparams.txt";
            Connection johnsonData = null;
            Connection myData = null;
            try {
               // connect to databases
               johnsonData = connectToDatabase(paramsFile1);
               myData = connectToDatabase(paramsFile2);
               
               // create my table
               deleteTable(myData);
               createTable(myData);
               
               // determine the number of industry in the database
               int industryCount = numIndustries(johnsonData);
               
               String[] industrySet = getIndustries(industryCount, johnsonData);
               for (int i = 1; i < industryCount; i++) {
                  System.out.println("Processing: " + industrySet[i]);
                  processIndustry(johnsonData, myData, industrySet[i]);
               }

               // close out connections
               johnsonData.close();
               myData.close();
               
               // catch errors
               } catch (SQLException ex) {
               System.out.printf("SQLException: %s%nSQLState: %s%nVendorError: %s%n", 
               ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
            }               
     } 
     
     // connect to given database
     public static Connection connectToDatabase(String file) throws Exception {
         Properties connectprops = new Properties();
         connectprops.load(new FileInputStream(file));
         try { 
            Class.forName("com.mysql.jdbc.Driver");
            String dburl = connectprops.getProperty("dburl"); 
            String username = connectprops.getProperty("user"); 
            System.out.printf("Database connection %s %s established.%n", dburl, username);       
            return DriverManager.getConnection(dburl, connectprops);
         } catch (SQLException ex) {
            System.out.printf("SQLException: %s%nSQLState: %s%nVendorError: %s%n", 
            ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
            return null;
         }
      }
     
     // drop table if it exists
     public static void deleteTable(Connection conn) throws SQLException { 
         PreparedStatement pstmt = conn.prepareStatement("drop table if exists Performance; ");
         pstmt.executeUpdate();
     }
     
     // create my table
     public static void createTable(Connection conn) throws SQLException {
         PreparedStatement pstmt = conn.prepareStatement("create table Performance (Industry char(30), Ticker char(6), StartDate char(10), EndDate char(10), TickerReturn char(12), IndustryReturn char(12));");
         pstmt.executeUpdate();
     }
     
     // insert data into my table
     public static void insertData(Connection conn, String industry, String ticker, String startDate, String endDate, String tickerReturn, String industryReturn) throws SQLException {
         PreparedStatement pstmt = conn.prepareStatement("insert into Performance(Industry, Ticker, StartDate, EndDate, TickerReturn, IndustryReturn)  values(?, ?, ?, ?, ?, ?);");
         pstmt.setString(1, industry);
         pstmt.setString(2, ticker);
         pstmt.setString(3, startDate);
         pstmt.setString(4, endDate);
         pstmt.setString(5, tickerReturn);
         pstmt.setString(6, industryReturn);
         pstmt.executeUpdate();
     }

     
     // return the number of industries in the database
     public static int numIndustries(Connection reader) throws SQLException { 
     
         PreparedStatement pstmt = reader.prepareStatement(
         "select count(distinct Industry) from Company");
         ResultSet rs = pstmt.executeQuery();
         rs.next();
         int val = rs.getInt(1);
         return val;    
      }
     
     // get all of the industries
     public static String[] getIndustries(int num, Connection reader) throws SQLException{
     
         String industries[] = new String[num];     
         PreparedStatement pstmt = reader.prepareStatement(
         "select distinct Industry from Company");
         ResultSet rs = pstmt.executeQuery();
         for (int i = 0; i < num; i++) {
            rs.next();
            industries[i] = rs.getString(1);
         }
         return industries;
      }
     
     // process all of the data from a given industry
     public static void processIndustry(Connection conn, Connection conn2, String industry) throws SQLException {
      
       // receive all tickers with at least 2.5 intervals of analysis
       
       // first find how tickers there are
         PreparedStatement pstmt = conn.prepareStatement(
         "select distinct Ticker from Company natural join PriceVolume where Industry = ? group by Ticker having count(TransDate) >= 150 order by Ticker");
         pstmt.setString(1, industry);
         ResultSet rs = pstmt.executeQuery();
         int numTickers = 0;
         while (rs.next()) {
           numTickers++;
         }
         // then get the tickers
         String tickers[] = new String[numTickers]; 
         PreparedStatement pstmt2 = conn.prepareStatement(
         "select distinct Ticker from Company natural join PriceVolume where Industry = ? group by Ticker having count(TransDate) >= 150 order by Ticker");
         pstmt2.setString(1, industry);
         ResultSet rs2 = pstmt2.executeQuery();
          for (int j = 0; j < tickers.length; j++) { 
             rs2.next();         
             tickers[j] = rs2.getString(1);
          }
      
         // find the maximum minimum/minimum maximum trades datas from the set of tickers
         String maxMinDate = getMaxDate(conn, tickers);
         String minMaxDate = getMinDate(conn, tickers);
      
         // find the number of trade days in the intervals, determine the number of intervals
         String totalDays = getTradingDays(conn, tickers[0], maxMinDate, minMaxDate);
         int intervals = Integer.parseInt(totalDays);
         intervals = intervals / 60;
      
         // get the first and last trade days of each interval
         String startDays[] = new String[intervals];
         String lastDays[] = new String[intervals];
         ResultSet intervalDays = getIntervals(conn, tickers[0], maxMinDate, minMaxDate);
         for (int k = 0; k < intervals; k++){
            intervalDays.next();
            startDays[k] = intervalDays.getString(1);
            for (int l = 0; l < 59; l++) {
               intervalDays.next();
            }
            lastDays[k] = intervalDays.getString(1);
         }
      
         // process calculations and store results in each interval for all tickers
         for (int m = 0; m < intervals; m++) {
             calcReturns(conn, conn2, industry, tickers, startDays[m], lastDays[m]);
         }
      }
     
    // search through all minimum trading days for each ticker and return the maximum
    public static String getMaxDate(Connection conn, String tickers[]) throws SQLException {
         String max = "1000.01.01";
         for (int i = 0; i < tickers.length; i++) {
            PreparedStatement pstmt = conn.prepareStatement(
            "select min(TransDate) from Company natural join PriceVolume where Ticker = ?");
            pstmt.setString(1, tickers[i]);
            ResultSet rs = pstmt.executeQuery();
            rs.next();
            String date = rs.getString(1);
            if (max.compareTo(date) <= 0) {
               max = date;
            }
         }
         return max;
      }
    
    // search through all maximum trading days for each ticker and return the mimimum
    public static String getMinDate(Connection conn, String tickers[]) throws SQLException {
         String min = "3000.01.01";
         for (int i = 0; i < tickers.length; i++) {
            PreparedStatement pstmt = conn.prepareStatement(
            "select max(TransDate) from Company natural join PriceVolume where Ticker = ?");
            pstmt.setString(1, tickers[i]);
            ResultSet rs = pstmt.executeQuery();
            rs.next();
            String date = rs.getString(1);
            if (min.compareTo(date) >= 0) {
               min = date;
            }
         }
         return min;
      }
    
    // get the number of trading days from the minimum to maximum trading days
    public static String getTradingDays(Connection conn, String ticker, String min, String max) throws SQLException {
         PreparedStatement pstmt = conn.prepareStatement(
         "select count(distinct TransDate) from Company natural join PriceVolume where Ticker = ? and TransDate >= ? and TransDate <= ?");
         pstmt.setString(1, ticker);
         pstmt.setString(2, min);
         pstmt.setString(3, max);
         ResultSet rs = pstmt.executeQuery();
         rs.next();
         return rs.getString(1);
    }
    
    // get all the trading days from the minimum to maximum trading days
    public static ResultSet getIntervals(Connection conn, String ticker, String min, String max) throws SQLException {
         PreparedStatement pstmt = conn.prepareStatement(
         "select distinct TransDate from Company natural join PriceVolume where Ticker = ? and TransDate >= ? and TransDate <= ?");
         pstmt.setString(1, ticker);
         pstmt.setString(2, min);
         pstmt.setString(3, max);
         return pstmt.executeQuery();
    }
    
    // perform calculations on the stock on a given interval, and stores them in the database
    public static void calcReturns(Connection conn, Connection conn2, String industry, String tickers[], String startDay, String endDay) throws SQLException {
         // store all tickers' ticker returns
         double tickerReturns[] = new double[tickers.length];
      
         // store all ticker's industry returns
         double industryReturns[] = new double[tickers.length];
      
         // find the OpenPrice of the first day and ClosingPrice of the last day
         // of the interval for all tickers
         for (int i = 0; i < tickers.length; i++) {
            PreparedStatement pstmt = conn.prepareStatement(
            "select OpenPrice from PriceVolume where Ticker = ? and TransDate = ?");
            pstmt.setString(1, tickers[i]);
            pstmt.setString(2, startDay);
            ResultSet rs = pstmt.executeQuery();
            rs.next();
            String openingPrice = rs.getString(1);
         
            PreparedStatement pstmt2 = conn.prepareStatement(
            "select ClosePrice from PriceVolume where Ticker = ? and TransDate = ?");
            pstmt2.setString(1, tickers[i]);
            pstmt2.setString(2, endDay);
            rs = pstmt2.executeQuery();
            rs.next();
            String closingPrice = rs.getString(1);
            double doubOpen = Double.parseDouble(openingPrice);
            double doubClose = Double.parseDouble(closingPrice);
            double tickerReturn = (doubClose/doubOpen)-1;
            tickerReturns[i] = tickerReturn; 
         }
      
         // calculate the industry return of each ticker by comparing all ticker returns
         // from all tickers beside the one being compared
         double num = tickers.length - 1;
         double ratio = 1/(num);
         for (int j = 0; j < tickers.length; j++) {
            double sum = 0.0;
            for (int k = 0; k < tickers.length; k++) {
               if (j != k) {
                  sum += tickerReturns[k];
               }
            }
            industryReturns[j] = ratio * sum;
         }
      
         // store all the data into my database
         for (int m = 0; m < tickers.length; m++) {
            insertData(conn2, industry, tickers[m], startDay, endDay, String.format("%10.7f", tickerReturns[m]), String.format("%10.7f", industryReturns[m]));
         } 
      }
}