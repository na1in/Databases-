import java.io.FileInputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.*;

/**
 * Runs queries against a back-end database
 */
public class Query
{
  private String configFilename;
  private Properties configProps = new Properties();

  private String jSQLDriver;
  private String jSQLUrl;
  private String jSQLUser;
  private String jSQLPassword;

  // DB Connection
  private Connection conn;

  // Logged In User
  private String username; // customer username is unique

  //Objects required 
  private TreeMap<Integer,ArrayList<Flight>> iternaries=new TreeMap<Integer,ArrayList<Flight>>();
  private TreeMap<Integer, ArrayList<Flight>> searchFlights = new TreeMap<Integer,ArrayList<Flight>>();

  // Canned queries

  private static final String CHECK_FLIGHT_CAPACITY = "SELECT capacity FROM Flights WHERE fid = ?";
  private PreparedStatement checkFlightCapacityStatement;

  private static final String CHECK_USERNAME_PASSWORD= "SELECT * FROM USERS where username = ? and password = ?";
  private PreparedStatement checkUsernamePasswordStatement;

  private static final String CREATE_CUSTOMER="INSERT INTO USERS values (?,?,?);";
  private PreparedStatement createCustomerStatement;

  private static final String DIRECT_FLIGHTS="SELECT * FROM Flights where origin_city = ? and dest_city= ? and day_of_month=? and canceled!=1 order by actual_time,fid;";
  private PreparedStatement directFlightsStatement;

  private static final String INDIRECT_FLIGHTS= "SELECT F1.fid as fid1, F2.fid as fid2, F1.actual_time as time1, F2.actual_time as time2, F1.day_of_month as day_of_month, F1.carrier_id as cid1, F2.carrier_id as cid2, F1.flight_num as fnum1, F2.flight_num as fnum2, F1.origin_city as origin_city1, F2.origin_city as origin_city2, F1.dest_city as dest_city1, F2.dest_city as dest_city2, F1.capacity as capacity1, F2.capacity as capacity2, F1.price as price1, F2.price as price2 from Flights as F1, Flights as F2 where F1.origin_city = ? and F1.dest_city = F2.origin_city and F2.dest_city = ? and F1.day_of_month = F2.day_of_month and F1.day_of_month = ? and F1.canceled != 1 and F2.canceled != 1 order by (F1.actual_time + F2.actual_time)";
  private PreparedStatement indirectFlightsStatement;

  private static final String CLEAR_USERS = "DELETE FROM users;";
  private PreparedStatement clearUsersStatement;

  private static final String CLEAR_RESERVATIONS = "DELETE FROM reservations;";
  private PreparedStatement clearReservationStatement;

  private static final String CLEAR_CAPACITY="DELETE FROM capacity;";
  private PreparedStatement clearCapacityStatement;

  private static final String CHECK_DAY="Select day from reservations where username = ?;";
  private PreparedStatement checkDayStatement;

  private static final String MAX_RESERVATIONID="Select top 1 rid from reservations order by rid desc ;";
  private PreparedStatement maxReservationIdStatement;

  private static final String FLIGHT_BOOKING="INSERT INTO RESERVATIONS values (?,?,?,?,?,?,?,?);";
  private PreparedStatement flightBookingStatement;

  private static final String CREATE_CAPACITY="INSERT INTO Capacity Select Flights.fid,Flights.capacity from Flights where Flights.fid = ? and NOT EXISTS (Select * from capacity where Capacity.fid=Flights.fid);";
  private PreparedStatement createCapacityStatement;

  private static final String UPDATE_CAPACITY="UPDATE Capacity set capacity=(capacity-1) where fid=? ;";
  private PreparedStatement updateCapacityStatement;

  private static final String CHECK_CAPACITY="SELECT * from capacity where fid =?;";
  private PreparedStatement checkCapacityStatement;

  private static final String CHECK_RESERVATIONS="SELECT * from reservations where username = ? order by rid;";
  private PreparedStatement checkReservationsStatement;

  private static final String BOOKED_FLIGHTS="select * from Flights where fid=?;";
  private PreparedStatement bookedFlightsStatement;

  private static final String CHECK_MONEY="Select balance from users where username = ?;";
  private PreparedStatement checkMoneyStatement;

  private static final String UPDATE_MONEY="Update Users set balance = ? where username = ?;";
  private PreparedStatement updateMoneyStatement;

  private static final String UPDATE_PAYMENT="Update reservations set paid = ? where rid = ?;";
  private PreparedStatement updatePaymentStatement;

  private static final String REFUND_MONEY="Update users set balance=(balance +?) where username = ?;";
  private PreparedStatement refundMoneyStatement;

  private static final String INCREASE_CAPACITY="Update capacity set capacity = (capacity+1) where fid =?;";
  private PreparedStatement increaseCapacityStatement;

  private static final String CANCEL_RESERVATION="DELETE from reservations where rid =?;";
  private PreparedStatement cancelReservationStatement;




  // transactions
  private static final String BEGIN_TRANSACTION_SQL = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION;";
  private PreparedStatement beginTransactionStatement;

  private static final String COMMIT_SQL = "COMMIT TRANSACTION";
  private PreparedStatement commitTransactionStatement;

  private static final String ROLLBACK_SQL = "ROLLBACK TRANSACTION";
  private PreparedStatement rollbackTransactionStatement;

  class Flight
  {
    public int fid;
    public int dayOfMonth;
    public String carrierId;
    public String flightNum;
    public String originCity;
    public String destCity;
    public int time;
    public int capacity;
    public int price;

    @Override
    public String toString()
    {
      return "ID: " + fid + " Day: " + dayOfMonth + " Carrier: " + carrierId +
              " Number: " + flightNum + " Origin: " + originCity + " Dest: " + destCity + " Duration: " + time +
              " Capacity: " + capacity + " Price: " + price;
    }
  }

  public Query(String configFilename)
  {
    this.configFilename = configFilename;
  }

  /* Connection code to SQL Azure.  */
  public void openConnection() throws Exception
  {
    configProps.load(new FileInputStream(configFilename));

    jSQLDriver = configProps.getProperty("flightservice.jdbc_driver");
    jSQLUrl = configProps.getProperty("flightservice.url");
    jSQLUser = configProps.getProperty("flightservice.sqlazure_username");
    jSQLPassword = configProps.getProperty("flightservice.sqlazure_password");

    /* load jdbc drivers */
    Class.forName(jSQLDriver).newInstance();

    /* open connections to the flights database */
    conn = DriverManager.getConnection(jSQLUrl, // database
            jSQLUser, // user
            jSQLPassword); // password

    conn.setAutoCommit(true); //by default automatically commit after each statement

    /* You will also want to appropriately set the transaction's isolation level through:
       conn.setTransactionIsolation(...)
       See Connection class' JavaDoc for details.
    */
  }

  public void closeConnection() throws Exception
  {
    conn.close();
  }

  /**
   * Clear the data in any custom tables created. Do not drop any tables and do not
   * clear the flights table. You should clear any tables you use to store reservations
   * and reset the next reservation ID to be 1.
   */
  public void clearTables ()
  {
    try{
      //clearReservationStatement.clearParameters();
      clearReservationStatement.executeUpdate();

      //clearUsersStatement.clearParameters();
      clearUsersStatement.executeUpdate();

      clearCapacityStatement.executeUpdate();

    }catch(SQLException e){
     //e.printStackTrace();
    }

  }

  /**
   * prepare all the SQL statements in this method.
   * "preparing" a statement is almost like compiling it.
   * Note that the parameters (with ?) are still not filled in
   */
  public void prepareStatements() throws Exception
  {
    beginTransactionStatement = conn.prepareStatement(BEGIN_TRANSACTION_SQL);
    commitTransactionStatement = conn.prepareStatement(COMMIT_SQL);
    rollbackTransactionStatement = conn.prepareStatement(ROLLBACK_SQL);

    checkFlightCapacityStatement = conn.prepareStatement(CHECK_FLIGHT_CAPACITY);
    checkUsernamePasswordStatement=conn.prepareStatement(CHECK_USERNAME_PASSWORD);
    createCustomerStatement=conn.prepareStatement(CREATE_CUSTOMER);
    directFlightsStatement=conn.prepareStatement(DIRECT_FLIGHTS);
    indirectFlightsStatement=conn.prepareStatement(INDIRECT_FLIGHTS);
    clearUsersStatement=conn.prepareStatement(CLEAR_RESERVATIONS);
    clearReservationStatement=conn.prepareStatement(CLEAR_USERS);
    checkDayStatement = conn.prepareStatement(CHECK_DAY);
    maxReservationIdStatement=conn.prepareStatement(MAX_RESERVATIONID);
    flightBookingStatement=conn.prepareStatement(FLIGHT_BOOKING);
    createCapacityStatement=conn.prepareStatement(CREATE_CAPACITY);
    updateCapacityStatement=conn.prepareStatement(UPDATE_CAPACITY);
    checkCapacityStatement=conn.prepareStatement(CHECK_CAPACITY);
    checkReservationsStatement=conn.prepareStatement(CHECK_RESERVATIONS);
    bookedFlightsStatement=conn.prepareStatement(BOOKED_FLIGHTS);
    checkMoneyStatement=conn.prepareStatement(CHECK_MONEY);
    updateMoneyStatement=conn.prepareStatement(UPDATE_MONEY);
    updatePaymentStatement=conn.prepareStatement(UPDATE_PAYMENT);
    clearCapacityStatement=conn.prepareStatement(CLEAR_CAPACITY);
    increaseCapacityStatement=conn.prepareStatement(INCREASE_CAPACITY);
    refundMoneyStatement=conn.prepareStatement(REFUND_MONEY);
    cancelReservationStatement=conn.prepareStatement(CANCEL_RESERVATION);
 

    /* add here more prepare statements for all the other queries you need */
    /* . . . . . . */
  }

  /**
   * Takes a user's username and password and attempts to log the user in.
   *
   * @param username
   * @param password
   *
   * @return If someone has already logged in, then return "User already logged in\n"
   * For all other errors, return "Login failed\n".
   *
   * Otherwise, return "Logged in as [username]\n".
   */
  public String transaction_login(String username, String password)
  {
    
    try{
      if(this.username==null){
      checkUsernamePasswordStatement.clearParameters();
      checkUsernamePasswordStatement.setString(1,username);
      checkUsernamePasswordStatement.setString(2,password);
      ResultSet rs=checkUsernamePasswordStatement.executeQuery();
            if(rs.next()){
              this.username=username;
              return "Logged in as "+ username + "\n";
            }
       }else{
        return "User already logged in"+"\n";
       }
    }
    catch (SQLException e) { 
      e.printStackTrace(); 
    return "Login failed"+"\n";
  }
  return "Login failed"+"\n";
}
  
  /**
   * Implement the create user function.
   *
   * @param username new user's username. User names are unique the system.
   * @param password new user's password.
   * @param initAmount initial amount to deposit into the user's account, should be >= 0 (failure otherwise).
   *
   * @return either "Created user {@code username}\n" or "Failed to create user\n" if failed.
   */
  public String transaction_createCustomer (String username, String password, int initAmount)
  {
    username=username.toLowerCase();
    password=password.toLowerCase();
   if(initAmount >=0 && username.length()<=20 && password.length()<=20){
    try{
      beginTransaction();
      createCustomerStatement.clearParameters();
      createCustomerStatement.setString(1,username);
      createCustomerStatement.setString(2,password);
      createCustomerStatement.setInt(3,initAmount);
      createCustomerStatement.executeUpdate();
      commitTransaction();
      return "Created user " + username + "\n";
    }catch(SQLException e){
      return "Failed to create user"+"\n";
    }
   }else{
    return "Failed to create user"+"\n" ;
   }

  }

  /**
   * Implement the search function.
   *
   * Searches for flights from the given origin city to the given destination
   * city, on the given day of the month. If {@code directFlight} is true, it only
   * searches for direct flights, otherwise is searches for direct flights
   * and flights with two "hops." Only searches for up to the number of
   * itineraries given by {@code numberOfItineraries}.
   *
   * The results are sorted based on total flight time.
   *
   * @param originCity
   * @param destinationCity
   * @param directFlight if true, then only search for direct flights, otherwise include indirect flights as well
   * @param dayOfMonth
   * @param numberOfItineraries number of itineraries to return
   *
   * @return If no itineraries were found, return "No flights match your selection\n".
   * If an error occurs, then return "Failed to search\n".
   *
   * Otherwise, the sorted itineraries printed in the following format:
   *
   * Itinerary [itinerary number]: [number of flights] flight(s), [total flight time] minutes\n
   * [first flight in itinerary]\n
   * ...
   * [last flight in itinerary]\n
   *
   * Each flight should be printed using the same format as in the {@code Flight} class. Itinerary numbers
   * in each search should always start from 0 and increase by 1.
   *
   * @see Flight#toString()
   */
  public String transaction_search(String originCity, String destinationCity, boolean directFlight, int dayOfMonth,
                                   int numberOfItineraries)
  {

    try{

    directFlightsStatement.clearParameters();
    directFlightsStatement.setString(1,originCity);
    directFlightsStatement.setString(2,destinationCity);
    directFlightsStatement.setInt(3,dayOfMonth);
    ResultSet rs= directFlightsStatement.executeQuery();
    int num=numberOfItineraries;

    while(rs.next() && numberOfItineraries>0){
        numberOfItineraries--;
        int fid=rs.getInt("fid");
        Flight direct = flightText(fid);

        if(iternaries.containsKey(direct.time)){
          iternaries.get(direct.time).add(direct);
        }else{
          ArrayList<Flight>flights= new ArrayList<Flight>();
          flights.add(direct);
          iternaries.put(direct.time,flights);
        }
      }
      if(num==numberOfItineraries){
        return "No flights match your selection"+"\n";
      }

       if(!directFlight && numberOfItineraries>0){
    
      indirectFlightsStatement.clearParameters();
      indirectFlightsStatement.setString(1,originCity);
      indirectFlightsStatement.setString(2,destinationCity);
      indirectFlightsStatement.setInt(3,dayOfMonth);
      ResultSet urs=indirectFlightsStatement.executeQuery();

      while(urs.next() && numberOfItineraries >0){
        numberOfItineraries--;
        Flight f1 = new Flight();
        Flight f2 = new Flight();

        f1.fid = urs.getInt("fid1");
        f1.dayOfMonth=urs.getInt("day_of_month");
        f1.flightNum=urs.getString("fnum1");
        f1.carrierId=urs.getString("cid1");
        f1.time=urs.getInt("time1");
        f1.originCity=urs.getString("origin_city1");
        f1.destCity=urs.getString("dest_city1");
        f1.capacity=urs.getInt("capacity1");
        f1.price=urs.getInt("price1");

        f2.fid=urs.getInt("fid2");
        f2.dayOfMonth=f1.dayOfMonth;
        f2.flightNum=urs.getString("fnum2");
        f2.carrierId=urs.getString("cid2");
        f2.time=urs.getInt("time2");
        f2.originCity=urs.getString("origin_city2");
        f2.destCity=urs.getString("dest_city2");
        f2.capacity=urs.getInt("capacity2");
        f2.price=urs.getInt("price2");

        int totalTime=urs.getInt("time1")+urs.getInt("time2");

        if(iternaries.containsKey(totalTime)){
          iternaries.get(totalTime).add(f1);
          iternaries.get(totalTime).add(f2);
        }else{
          ArrayList<Flight>flights= new ArrayList<Flight>();
          flights.add(f1);
          flights.add(f2);
          iternaries.put(totalTime,flights);
        }
      }
      if(num==numberOfItineraries){
        return "No flights match your selection"+"\n";
      }

    }
        
      
    }catch (SQLException e) { e.printStackTrace();
    return "Failed to Search";
     }

  return flightAssorter(iternaries,destinationCity);
  } 



  private String flightAssorter(TreeMap<Integer,ArrayList<Flight>> groupFlights,String destinationCity){
           StringBuffer flightResult = new StringBuffer();
       int number = 0;
 
    Iterator<Map.Entry<Integer,ArrayList<Flight>>> it=groupFlights.entrySet().iterator();
    while(it.hasNext()){
    Map.Entry<Integer,ArrayList<Flight>> pair=it.next();
    ArrayList<Flight> schedules = pair.getValue();
    ArrayList<Flight> fids_group = new ArrayList<Flight>();
    for (Flight flight: schedules) {
      fids_group.add(flight);
      if(flight.destCity.equals(destinationCity)){
        int num_flights = fids_group.size();
        flightResult.append("Itinerary " + number + ": " + num_flights + " flight(s), " + pair.getKey() + " minutes"+"\n");
        for (Flight f1: fids_group) {
        flightResult.append(f1.toString() + "\n");
        }
        searchFlights.put(number, new ArrayList<Flight>(fids_group));
        number++;
        fids_group.clear();
      } else {
        flightResult.append("");
      }
    }
  }
  return flightResult.toString();


  }
  

  /**
   * Same as {@code transaction_search} except that it only performs single hop search and
   * do it in an unsafe manner.
   *
   * @param originCity
   * @param destinationCity
   * @param directFlight
   * @param dayOfMonth
   * @param numberOfItineraries
   *
   * @return The search results. Note that this implementation *does not conform* to the format required by
   * {@code transaction_search}.
   */
  private String transaction_search_unsafe(String originCity, String destinationCity, boolean directFlight,
                                          int dayOfMonth, int numberOfItineraries)
  {
    StringBuffer sb = new StringBuffer();

    try
    {
      // one hop itineraries
      String unsafeSearchSQL =
              "SELECT TOP (" + numberOfItineraries + ") day_of_month,carrier_id,flight_num,origin_city,dest_city,actual_time,capacity,price "
                      + "FROM Flights "
                      + "WHERE origin_city = \'" + originCity + "\' AND dest_city = \'" + destinationCity + "\' AND day_of_month =  " + dayOfMonth + " "
                      + "ORDER BY actual_time ASC";
             

      Statement searchStatement = conn.createStatement();
      ResultSet oneHopResults = searchStatement.executeQuery(unsafeSearchSQL);

      while (oneHopResults.next())
      {
        int result_dayOfMonth = oneHopResults.getInt("day_of_month");
        String result_carrierId = oneHopResults.getString("carrier_id");
        String result_flightNum = oneHopResults.getString("flight_num");
        String result_originCity = oneHopResults.getString("origin_city");
        String result_destCity = oneHopResults.getString("dest_city");
        int result_time = oneHopResults.getInt("actual_time");
        int result_capacity = oneHopResults.getInt("capacity");
        int result_price = oneHopResults.getInt("price");

        sb.append("Day: " + result_dayOfMonth + " Carrier: " + result_carrierId + " Number: " + result_flightNum + " Origin: " + result_originCity + " Destination: " + result_destCity + " Duration: " + result_time + " Capacity: " + result_capacity + " Price: " + result_price + "\n");
      }
      oneHopResults.close();
    } catch (SQLException e) { e.printStackTrace(); }

    return sb.toString();
  }

  /**
   * Implements the book itinerary function.
   *
   * @param itineraryId ID of the itinerary to book. This must be one that is returned by search in the current session.
   *
   * @return If the user is not logged in, then return "Cannot book reservations, not logged in\n".
   * If try to book an itinerary with invalid ID, then return "No such itinerary {@code itineraryId}\n".
   * If the user already has a reservation on the same day as the one that they are trying to book now, then return
   * "You cannot book two flights in the same day\n".
   * For all other errors, return "Booking failed\n".
   *
   * And if booking succeeded, return "Booked flight(s), reservation ID: [reservationId]\n" where
   * reservationId is a unique number in the reservation system that starts from 1 and increments by 1 each time a
   * successful reservation is made by any user in the system.
   */
  public String transaction_book(int itineraryId)
  {
    if(this.username == null){
      return "Cannot book reservations, not logged in"+"\n";
    }
    if(!searchFlights.containsKey(itineraryId)){
      return "no such iternary" + itineraryId;
    }

    try {
      beginTransaction();
      checkDayStatement.clearParameters();
      checkDayStatement.setString(1,this.username);
      ResultSet rs=checkDayStatement.executeQuery();
      Flight flight= searchFlights.get(itineraryId).get(0);
      int bookingday=flight.dayOfMonth;
      if(rs.next()){
        int day=rs.getInt("day");
        if(bookingday == day){
          //rollbackTransaction();
          return "You cannot book two flights on the same day";
        }
      }

      maxReservationIdStatement.clearParameters();
      ResultSet max = maxReservationIdStatement.executeQuery();
      int reserveId=1;
      if(max.next()){
        int maximum=max.getInt("rid");
        reserveId=maximum + 1;
      }

      ArrayList<Flight> bookingFlights = searchFlights.get(itineraryId);
        if(bookingFlights.size()==1){
          Flight direct = bookingFlights.get(0);

          createCapacityStatement.clearParameters();
          createCapacityStatement.setInt(1,direct.fid);
          createCapacityStatement.executeUpdate();

          checkCapacityStatement.clearParameters();
          checkCapacityStatement.setInt(1,direct.fid);
          ResultSet cap=checkCapacityStatement.executeQuery();

            if(cap.next()){
            int flightCapacity =cap.getInt("capacity");
            if(flightCapacity<1){
              rollbackTransaction();
              return "Booking failed"+"\n";
            }else{
            flightBookingStatement.clearParameters();
            flightBookingStatement.setInt(1,reserveId);
            flightBookingStatement.setString(2,this.username);
            flightBookingStatement.setInt(3,direct.dayOfMonth);
            flightBookingStatement.setInt(4,direct.fid);
            flightBookingStatement.setInt(5,0);
            flightBookingStatement.setInt(6,direct.price);
            flightBookingStatement.setInt(7,0);
            flightBookingStatement.setInt(8,0);
            flightBookingStatement.executeUpdate();
            updateCapacityStatement.clearParameters();
            updateCapacityStatement.setInt(1,direct.fid);
            updateCapacityStatement.executeUpdate();
            commitTransaction();
             return "Booked flight(s), reservation ID: "+reserveId +"\n";

            }

          }
          rollbackTransaction();
          return "booking failed"+"\n";
          }else{
            Flight indirect1=bookingFlights.get(0);
            Flight indirect2=bookingFlights.get(1);

            createCapacityStatement.clearParameters();
            createCapacityStatement.setInt(1,indirect1.fid);
            createCapacityStatement.executeUpdate();

            createCapacityStatement.clearParameters();
            createCapacityStatement.setInt(1,indirect2.fid);
            createCapacityStatement.executeUpdate();

            checkCapacityStatement.clearParameters();
            checkCapacityStatement.setInt(1,indirect1.fid);
            ResultSet cap1=checkCapacityStatement.executeQuery();
            int capacity1=0;
            if(cap1.next()){
              capacity1 = cap1.getInt("capacity");
            }

            checkCapacityStatement.clearParameters();
            checkCapacityStatement.setInt(1,indirect2.fid);
            ResultSet cap2=checkCapacityStatement.executeQuery();
            int capacity2=0;
            if(cap2.next()){
              capacity2 = cap2.getInt("capacity");
            }

                      if(capacity1 <1 || capacity2 <1){
                        rollbackTransaction();
                        return "booking failed"+"\n";
                      }else{
                        flightBookingStatement.clearParameters();
                        flightBookingStatement.setInt(1,reserveId);
                        flightBookingStatement.setString(2,this.username);
                        flightBookingStatement.setInt(3,indirect1.dayOfMonth);
                        flightBookingStatement.setInt(4,indirect1.fid);
                        flightBookingStatement.setInt(5,indirect2.fid);
                        flightBookingStatement.setInt(6,indirect1.price);
                        flightBookingStatement.setInt(7,indirect2.price);
                        flightBookingStatement.setInt(8,0);
                        flightBookingStatement.executeUpdate();

                        updateCapacityStatement.clearParameters();
                        updateCapacityStatement.setInt(1,indirect1.fid);
                        updateCapacityStatement.executeUpdate();

                        updateCapacityStatement.clearParameters();
                        updateCapacityStatement.setInt(1,indirect2.fid);
                        updateCapacityStatement.executeUpdate();
                        commitTransaction();
                       return "Booked flight(s),reservartion ID: "+reserveId +"\n";
                      }

          }

      } catch (SQLException e) {
        return "Booking failed" +"\n";
      }

    }
  /**
   * Implements the reservations function.
   *
   * @return If no user has logged in, then return "Cannot view reservations, not logged in\n"
   * If the user has no reservations, then return "No reservations found\n"
   * For all other errors, return "Failed to retrieve reservations\n"
   *
   * Otherwise return the reservations in the following format:
   *
   * Reservation [reservation ID] paid: [true or false]:\n"
   * [flight 1 under the reservation]
   * [flight 2 under the reservation]
   * Reservation [reservation ID] paid: [true or false]:\n"
   * [flight 1 under the reservation]
   * [flight 2 under the reservation]
   * ...
   *
   * Each flight should be printed using the same format as in the {@code Flight} class.
   *
   * @see Flight#toString()
   */
  public String transaction_reservations()
  {
    if(this.username==null){
      return "Cannot veiw reservations, not logged in"+"\n";
    }
    StringBuffer sb = new StringBuffer();
    try{
    checkReservationsStatement.clearParameters();
    checkReservationsStatement.setString(1,this.username);
    ResultSet rs=checkReservationsStatement.executeQuery();
    
    int reservedFlights=0;
    while(rs.next()){ 
      reservedFlights++;
      int rid=rs.getInt("rid");
      int flightid1=rs.getInt("fid1");
      int flightid2=rs.getInt("fid2");
      int paid = rs.getInt("paid");
      String payment = "";
      if(paid==0){
        payment ="false";
      }
      if(paid==1){
        payment ="true";
      }
      sb.append("Reservation "+ rid + " paid: " + payment + ":\n");
      if(flightid2 == 0){
        sb.append(flightText(flightid1).toString()+"\n");
      }else{
        sb.append(flightText(flightid1).toString()+"\n");
        sb.append(flightText(flightid2).toString()+"\n");
      }
    }
    if(reservedFlights==0){
      return "No reservations found "+"\n";
    }else{
    return sb.toString();
    }
  }catch(SQLException e){
    e.printStackTrace();
  }
    return "Failed to retrieve reservations"+"\n";
  }

  /**
   * Implements the cancel operation.
   *
   * @param reservationId the reservation ID to cancel
   *
   * @return If no user has logged in, then return "Cannot cancel reservations, not logged in\n"
   * For all other errors, return "Failed to cancel reservation [reservationId]"
   *
   * If successful, return "Canceled reservation [reservationId]"
   *
   * Even though a reservation has been canceled, its ID should not be reused by the system.
   */
  public String transaction_cancel(int reservationId)
  {

    if(this.username==null){
      return "Cannot cancel reservations, not logged in"+"\n";
    }
    try{
      //beginTransaction();
    checkReservationsStatement.clearParameters();
    checkReservationsStatement.setString(1,this.username);
    ResultSet rs=checkReservationsStatement.executeQuery();
    while(rs.next()){
      int rid=rs.getInt("rid");
      int paid=rs.getInt("paid");
      int totalPrice=rs.getInt("price1")+rs.getInt("price2");
      int fid1=rs.getInt("fid1");
      int fid2=rs.getInt("fid2");

      if(rid !=reservationId){
        //rollbackTransaction();
        return "Failed to cancel reservation "+reservationId;
      }else{
          cancelReservationStatement.setInt(1,reservationId);
          cancelReservationStatement.executeUpdate();
              increaseCapacityStatement.setInt(1,fid1);
              increaseCapacityStatement.executeUpdate();          
              if(fid2!=0){
              increaseCapacityStatement.setInt(1,fid2);
              increaseCapacityStatement.executeUpdate();
              }
        if(paid==1){
          refundMoneyStatement.clearParameters();
          refundMoneyStatement.setInt(1,totalPrice);
          refundMoneyStatement.setString(2,this.username);
          refundMoneyStatement.executeUpdate();
        }
        return "Canceled reservation "+reservationId+"\n";
      }
    }
  }catch(SQLException e){
     return "Failed to cancel reservation "+reservationId+"\n";
  }
  return "Failed to cancel reservation "+reservationId+"\n";
}





  /**
   * Implements the pay function.
   *
   * @param reservationId the reservation to pay for.
   *
   * @return If no user has logged in, then return "Cannot pay, not logged in\n"
   * If the reservation is not found / not under the logged in user's name, then return
   * "Cannot find unpaid reservation [reservationId] under user: [username]\n"
   * If the user does not have enough money in their account, then return
   * "User has only [balance] in account but itinerary costs [cost]\n"
   * For all other errors, return "Failed to pay for reservation [reservationId]\n"
   *
   * If successful, return "Paid reservation: [reservationId] remaining balance: [balance]\n"
   * where [balance] is the remaining balance in the user's account.
   */
  public String transaction_pay (int reservationId)
  {
    
    if(this.username==null){
      return "Cannot pay, not logged in"+"\n";
    }
    try{
    beginTransaction();
    checkReservationsStatement.clearParameters();
    checkReservationsStatement.setString(1,this.username);
    ResultSet rs=checkReservationsStatement.executeQuery();

    checkMoneyStatement.clearParameters();
    checkMoneyStatement.setString(1,this.username);
    ResultSet mrs=checkMoneyStatement.executeQuery();
    int money=0;
    int cost=0;
    if(mrs.next()){
      money=mrs.getInt("balance");
    }

    if(!rs.next()){
      rollbackTransaction();
     return "Cannot find unpaid reservation "+reservationId+" under user: " +username+ "\n";
    }else{
    int rid= rs.getInt("rid");
    int totalPrice=rs.getInt("price1")+rs.getInt("price2");
    cost=totalPrice;
    int paid=rs.getInt("paid");

      if(paid==1 || rid != reservationId){
        rollbackTransaction();
        return "Cannot find unpaid reservation "+reservationId+" under user: " +username+ "\n";
      }
      

      if(money >= totalPrice){
        int moneyLeft = money-totalPrice;
        updateMoneyStatement.setInt(1,moneyLeft);
        updateMoneyStatement.setString(2,this.username);
        updateMoneyStatement.executeUpdate();
        updatePaymentStatement.setInt(1,1);
        updatePaymentStatement.setInt(2,rid);
        updatePaymentStatement.executeUpdate();
        commitTransaction();
        return "Paid reservation: "+reservationId +" remaining balance: "+moneyLeft+"\n";
      }else{
        return "User has only "+ money+ " in account but itinerary costs " +cost+"\n";
      }



    }
  }catch(SQLException e){
    e.printStackTrace();
  }
  return "Failed to pay for reservation"+reservationId+"\n";

    }


  /* some utility functions below */

  public void beginTransaction() throws SQLException
  {
    conn.setAutoCommit(false);
    beginTransactionStatement.executeUpdate();
  }

  public void commitTransaction() throws SQLException
  {
    commitTransactionStatement.executeUpdate();
    conn.setAutoCommit(true);
  }

  public void rollbackTransaction() throws SQLException
  {
    rollbackTransactionStatement.executeUpdate();
    conn.setAutoCommit(true);
  }

  private Flight flightText(int fid) throws SQLException{
    Flight booked= new Flight();
try{
    bookedFlightsStatement.clearParameters();
    bookedFlightsStatement.setInt(1,fid);
    ResultSet rs=bookedFlightsStatement.executeQuery();    
      while (rs.next())
      {
        booked.fid=rs.getInt("fid");
        booked.dayOfMonth = rs.getInt("day_of_month");
        booked.carrierId = rs.getString("carrier_id");
        booked.flightNum = rs.getString("flight_num");
        booked.originCity = rs.getString("origin_city");
        booked.destCity = rs.getString("dest_city");
        booked.time = rs.getInt("actual_time");
        booked.capacity = rs.getInt("capacity");
        booked.price = rs.getInt("price");
      }
      //oneHopResults.close();
  }catch(SQLException e){
    e.printStackTrace();
  }
    return booked;
  }

  /**
   * Shows an example of using PreparedStatements after setting arguments. You don't need to
   * use this method if you don't want to.
   */
  private int checkFlightCapacity(int fid) throws SQLException
  {
    checkFlightCapacityStatement.clearParameters();
    checkFlightCapacityStatement.setInt(1, fid);
    ResultSet results = checkFlightCapacityStatement.executeQuery();
    results.next();
    int capacity = results.getInt("capacity");
    results.close();

    return capacity;
  }


}
