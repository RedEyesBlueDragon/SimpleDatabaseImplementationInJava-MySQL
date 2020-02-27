package ceng.ceng351.bookdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.*;

public class BOOKDB implements IBOOKDB {

	private static String user = "2237253"; // TODO: Your userName
	private static String password = "3a892fa4"; // TODO: Your password
	private static String host = "144.122.71.65"; // host name
	private static String database = "db2237253"; // TODO: Your database name
	private static int port = 8084; // port

	public static Connection connection = null;

	public static void connect() {

		String url = "jdbc:mysql://" + host + ":" + port + "/" + database;

		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
			connection = DriverManager.getConnection(url, user, password);
		} catch (SQLException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public static void disconnect() {

		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * * Place your initialization code inside if required. * *
	 * <p>
	 * * This function will be called before all other operations. If your *
	 * implementation need initialization , necessary operations should be done *
	 * inside this function. For example, you can set your connection to the *
	 * database server inside this function.
	 */
	/**
	 * * Place your initialization code inside if required. * *
	 * <p>
	 * * This function will be called before all other operations. If your *
	 * implementation need initialization , necessary operations should be done *
	 * inside this function. For example, you can set your connection to the *
	 * database server inside this function.
	 */
	@Override
	public void initialize() {
		/*
		 * String url = "jdbc:mysql://" + this.host + ":" + this.port + "/" +
		 * this.database;
		 *
		 * try { Class.forName("com.mysql.cj.jdbc.Driver"); this.con =
		 * DriverManager.getConnection(url, this.user, this.password); } catch
		 * (SQLException | ClassNotFoundException e) { e.printStackTrace(); }
		 */
	}

	/**
	 * * Should create the necessary tables when called. * * @return the number of
	 * tables that are created successfully.
	 */
	public int createTables() {

		int numberofTablesInserted = 0;

			String Author = "create table author (" + "author_id int," + "author_name varchar(60) ," + "primary key (author_id))";
	
			String Publisher = "create table publisher (" + "publisher_id int," + "publisher_name varchar(50) ," + "primary key (publisher_id))";
	
			String Book = "create table book (" + "isbn char(13)," + "book_name varchar(120)," + "publisher_id int," + "first_publish_year char(4)," + "page_count int," + "category varchar(25)," + "rating float,"
					+ "primary key (isbn)," + "foreign key (publisher_id) references publisher(publisher_id))";
	
			String Author_of = "create table author_of (" + "isbn char(13)," + "author_id int," + "primary key (isbn,author_id)," + "foreign key (isbn) references book(isbn),"
					+ "foreign key (author_id) references author(author_id))";
	
			String Phw1 = "create table phw1 (" + "isbn char(13)," + "book_name varchar(120)," + "rating float," + "primary key (isbn)," + "foreign key (isbn) references book(isbn))";

		connect();

			try {
				Statement statement = this.connection.createStatement();
				statement.executeUpdate(Author);
				numberofTablesInserted++;
			} catch (SQLException e) {
			}
	
			try {
				Statement statement2 = this.connection.createStatement();
				statement2.executeUpdate(Publisher);
				numberofTablesInserted++;
			} catch (SQLException e) {
			}
	
			try {
				Statement statement3 = this.connection.createStatement();
				statement3.executeUpdate(Book);
				numberofTablesInserted++;
			} catch (SQLException e) {
			}
	
			try {
				Statement statement4 = this.connection.createStatement();
				statement4.executeUpdate(Author_of);
				numberofTablesInserted++;
			} catch (SQLException e) {
			}
	
			try {
				Statement statement5 = this.connection.createStatement();
				statement5.executeUpdate(Phw1);
				numberofTablesInserted++;
			} catch (SQLException e) {
				}

		return numberofTablesInserted;
	}

	public int dropTables() {
								    	
		int result;
		int numberofTablesDropped = 0;

  
        String queryDropAuthorTable = "drop table if exists author";
        String queryDropPublisherTable = "drop table if exists publisher";
        String queryDropBookTable = "drop table if exists book";
        String queryDropAuthor_ofTable = "drop table if exists author_of";
        String queryDropPhw1Table = "drop table if exists phw1";
  
  
    
	        	connect();

	        try {	
	        	Statement statement4 = this.connection.createStatement();
	        	result = statement4.executeUpdate(queryDropAuthor_ofTable);
	        	numberofTablesDropped++;}
	        	catch (SQLException e) {
			    	e.printStackTrace();
			    }
	        try {
	        	Statement statement5 = this.connection.createStatement();
	        	result = statement5.executeUpdate(queryDropPhw1Table);
	        	numberofTablesDropped++;} 
	        	catch (SQLException e) {
			    	e.printStackTrace();
			    }

	        try {
	        	Statement statement3 = this.connection.createStatement();
	        	result = statement3.executeUpdate(queryDropBookTable);
	        	numberofTablesDropped++;} 
	        	catch (SQLException e) {
			    	e.printStackTrace();
			    }
	        try {
	        	Statement statement2 = this.connection.createStatement();
	        	result = statement2.executeUpdate(queryDropPublisherTable);
	        	numberofTablesDropped++;} 
	        	catch (SQLException e) {
			    	e.printStackTrace();
			    }
	        try {
	        	Statement statement = this.connection.createStatement();
	        	result = statement.executeUpdate(queryDropAuthorTable);
	        	numberofTablesDropped++;} 
	        	catch (SQLException e) {
			    	e.printStackTrace();
			    }


		    
		    return numberofTablesDropped;
		
			}
		
		
	
	
     public int insertAuthor(Author[] authors) {
		
		int num = 0;
		int size = authors.length;
	
		for(int i=0;i < size;i++)
		{
			try {
				String query = "insert into author (author_id,author_name) values (?,?) "; 
				PreparedStatement prestat =	this.connection.prepareStatement(query);
				prestat.setInt(1, authors[i].getAuthor_id());
				prestat.setString(2, authors[i].getAuthor_name());
						
				prestat.executeUpdate();			
				prestat.close();
				num++;
					
			}catch (SQLException e) {
				e.printStackTrace();
			}
		}   	
		return num; 
	 }
			
			
			
		
			/**
			 * Should insert an array of Book into the database.
			 *
			 * @return Number of rows inserted successfully.
			 */
			
			
	public int insertBook(Book[] books) {
	
		int result = 0;
		int num = 0;
		int size = books.length;
	
		for(int i=0;i < size;i++)
		{
				
			try {
				
				String query = "insert into book (isbn, book_name, publisher_id, first_publish_year, page_count, category, rating) values (?,?,?,?,?,?,?) "; 
				PreparedStatement prestat =	this.connection.prepareStatement(query);
					prestat.setString(1, books[i].getIsbn());
					prestat.setString(2, books[i].getBook_name());
					prestat.setInt(3, books[i].getPublisher_id());
					prestat.setString(4, books[i].getFirst_publish_year());
					prestat.setInt(5, books[i].getPage_count());
					prestat.setString(6, books[i].getCategory());
					prestat.setDouble(7, books[i].getRating());
					
				prestat.executeUpdate();			
				prestat.close();
				
				num++;
	
	
			}catch (SQLException e) {
				e.printStackTrace();
			}
		}   	
		return num; 
	}
			
			
	public int insertPublisher(Publisher[] publishers) {
		
		int num = 0;
		int size = publishers.length;
	
		for(int i=0;i < size;i++)
		{
			try {
				String query = "insert into publisher (publisher_id,publisher_name) values (?,?) "; 
				PreparedStatement prestat =	this.connection.prepareStatement(query);
				prestat.setInt(1, publishers[i].getPublisher_id());
				prestat.setString(2, publishers[i].getPublisher_name());
						
				prestat.executeUpdate();			
				prestat.close();
				num++;
					
			}catch (SQLException e) {
				e.printStackTrace();
			}
		}   	
		return num; 
		
		
	}
			
	public int insertAuthor_of(Author_of[] author_ofs) {
		int num = 0;
		int size = author_ofs.length;
	
		for(int i=0;i < size;i++)
		{
			try {
				String query = "insert into author_of (isbn,author_id) values (?,?) "; 
				PreparedStatement prestat =	this.connection.prepareStatement(query);
				prestat.setString(1, author_ofs[i].getIsbn());
				prestat.setInt(2, author_ofs[i].getAuthor_id());
						
				prestat.executeUpdate();			
				prestat.close();
				num++;
					
			}catch (SQLException e) {
				e.printStackTrace();
			}
		}   	
		return num; 
	}

    /**
     * Should return isbn, first_publish_year, page_count and publisher_name of 
     * the books which have the maximum number of pages.
     * @return QueryResult.ResultQ1[]
     */
    public QueryResult.ResultQ1[] functionQ1() {
    	int size =0;
    	ResultSet result;
    	connect();
    	
    	String query= "select b.isbn,b.first_publish_year,b.page_count,p.publisher_name " + 
    			"from book b,publisher p " + 
    			"where b.publisher_id = p.publisher_id and " + 
    			"b.page_count = (select MAX(page_count) from book) order by isbn asc;";
    	try
    	{
    	Statement stat = this.connection.createStatement();
    	result = stat.executeQuery(query);
    	
    	
	    	if (result != null) 
	    	{
	    		result.last();    
	    	  size = result.getRow(); 
	    	}
    	}
    	catch (SQLException e) {
			e.printStackTrace();
		}
    	
    	QueryResult.ResultQ1[] array = new QueryResult.ResultQ1[size];
    	
    	try
    	{
    	Statement stat = this.connection.createStatement();
    	result = stat.executeQuery(query);
    	int i = 0;
	    	while (result.next())
	    	{
	    		QueryResult.ResultQ1 qres = new QueryResult.ResultQ1(result.getString("isbn"), result.getString("first_publish_year"), result.getInt("page_count"), result.getString("publisher_name"));
	    		array[i] = qres;
	    		i++;	    		
	    	}
	    	return array;
    	}
    	
    	catch (SQLException e) {
			e.printStackTrace();
		}
    	
    	return array;
    	
		
	}
    
    /**
     * For the publishers who have published books that were co-authored by both 
     * of the given authors(author1 and author2); list publisher_id(s) and average
     * page_count(s)  of all the books these publishers have published.
     * @param author_id1
     * @param author_id2
     * @return QueryResult.ResultQ2[]
     */

    public QueryResult.ResultQ2[] functionQ2(int author_id1, int author_id2) {
    	int size =0;
    	ResultSet result;
    	connect();
    	
    	String query="select distinct b.publisher_id, F.bla " + 
    		               " 	from author_of a, author_of a2, book b, (select distinct AVG(b3.page_count) as bla " + 
    		               "				from book b2, author_of a3, author_of a4, book b3 " + 
    		               "				where " + 
    		               "                  a3.isbn = a4.isbn and " + 
    		               "				a3.author_id = ? and " + 
    		               "				a4.author_id = ? and " +
    		               "                  b2.isbn = a3.isbn and " + 
    		               "                  b2.publisher_id = b3.publisher_id " + 
    		               "                  group by b3.publisher_id) F " + 
    		               " 	where a.isbn = a2.isbn and " + 
    		               "      b.isbn = a.isbn and " + 
    		               "      a.author_id = ? and " +
    		               "      a2.author_id = ? " + 
    		               " 	order by publisher_id asc;";
    	
    	try
    	{
    		
    	PreparedStatement stat = this.connection.prepareStatement(query);
    	stat.setInt(1, author_id1);
    	stat.setInt(2, author_id2);
    	stat.setInt(3, author_id1);
    	stat.setInt(4, author_id2);
    	result = stat.executeQuery();
    	
    	
	    	if (result != null) 
	    	{
	    		result.last();    
	    	  size = result.getRow(); 
	    	}
    	}
    	catch (SQLException e) {
			e.printStackTrace();
		}
    	
    	QueryResult.ResultQ2[] array = new QueryResult.ResultQ2[size];
    	
    	try
    	{
    	PreparedStatement stat = this.connection.prepareStatement(query);
    	stat.setInt(1, author_id1);
    	stat.setInt(2, author_id2);
    	stat.setInt(3, author_id1);
    	stat.setInt(4, author_id2);
    	result = stat.executeQuery();
    	
    	int i = 0;
	    	while (result.next())
	    	{
	    		QueryResult.ResultQ2 qres = new QueryResult.ResultQ2(result.getInt(1), result.getDouble(2));
	    		array[i] = qres;
	    		i++;	    		
	    	}
	    stat.close();
    	}catch (SQLException e) {
			e.printStackTrace();
		}
    	
		return array;
	}
    
    /**
     * List book_name, category and first_publish_year of the earliest 
     * published book(s) of the author(s) whose author_name is given.
     * @param author_name
     * @return QueryResult.ResultQ3[]
     */

    public QueryResult.ResultQ3[] functionQ3(String author_name) {
    	int size =0;
    	ResultSet result;
    	connect();
    	
    	String query = "select distinct b.book_name , b.category, b.first_publish_year " + 
    			"	from book b, author a, author_of f" + 
    			"    where a.author_name = ? and " + 
    			"		a.author_id = f.author_id and" + 
    			"        f.isbn = b.isbn and " + 
    			"        b.first_publish_year = (select distinct min(b2.first_publish_year)" + 
    			"										from book b2, author a2, author_of f2" + 
    			"										where a2.author_name = ? and" + 
    			"										a2.author_id = f2.author_id and" + 
    			"										f2.isbn = b2.isbn) order by b.book_name, b.category, b.first_publish_year ;";
    	
    	
    	try
    	{
    	PreparedStatement stat = this.connection.prepareStatement(query);
    	stat.setString(1, author_name);
    	stat.setString(2, author_name);
       	result = stat.executeQuery();
    	  	
	    	if (result != null) 
	    	{
	    		result.last();    	  size = result.getRow(); 
	    	}
    	}catch (SQLException e) {
			e.printStackTrace();
			}
    	
    	QueryResult.ResultQ3[] array = new QueryResult.ResultQ3[size];
    	
    	try
    	{
    	PreparedStatement stat = this.connection.prepareStatement(query);

    	stat.setString(1, author_name);
    	stat.setString(2, author_name);
    	result = stat.executeQuery();
    	int i = 0;
	    	while (result.next())
	    	{
	    		QueryResult.ResultQ3 qres = new QueryResult.ResultQ3(result.getString(1), result.getString(2), result.getString(3));
	    		array[i] = qres;
	    		i++;	    		
	    	}
	    stat.close();
    	}catch (SQLException e) {
			e.printStackTrace();
		}
    	
		return array;
	}

    /**
     * For publishers whose name contains at least 3 words 
     * (i.e., "Koc Universitesi Yayinlari"), have published at least 3 books 
     * and average rating of their books are greater than(>) 3; 
     * list their publisher_id(s) and distinct category(ies) they have published. 
     * PS: You may assume that each word in publisher_name is seperated by a space.
     * @return QueryResult.ResultQ4[]
     */
    public QueryResult.ResultQ4[] functionQ4() {
    	int size =0;
    	ResultSet result;
    	connect();
    	
    	String query = " select distinct p.publisher_id, b.category from publisher p, book b " + 
    			"		where p.publisher_id = b.publisher_id and 3 <= (select count(distinct b2.isbn) from book b2 " + 
    			"															where b2.publisher_id = b.publisher_id group by b2.publisher_id) " + 
    			"												and 3 = (SELECT SUM(LENGTH(p2.publisher_name) - LENGTH(REPLACE(p2.publisher_name, ' ', '')) + 1) " + 
    			"																from publisher p2 where p.publisher_id = p2.publisher_id ) " + 
    			"                                                and 3 < (select avg(b3.rating) from book b3 where b3.publisher_id = b.publisher_id group by b3.publisher_id)  order by p.publisher_id, b.category;  ";
    	
    	
    	
    	try
    	{
    	PreparedStatement stat = this.connection.prepareStatement(query);
    	result = stat.executeQuery();
    	  	
	    	if (result != null) 
	    	{
	    		result.last();    	  size = result.getRow(); 
	    	}
    	}catch (SQLException e) {
			e.printStackTrace();
			}
    	
    	QueryResult.ResultQ4[] array = new QueryResult.ResultQ4[size];
    	
    	try
    	{
    	PreparedStatement stat = this.connection.prepareStatement(query);
    	result = stat.executeQuery();
    	int i = 0;
	    	while (result.next())
	    	{
	    		QueryResult.ResultQ4 qres = new QueryResult.ResultQ4(result.getInt(1),result.getString(2));
	    		array[i] = qres;
	    		i++;	    		
	    	}
	    stat.close();
    	}catch (SQLException e) {
			e.printStackTrace();
		}
    	
		return array;
    	
	}
    
    /**
     * List author_id and author_name of the authors who have worked with 
     * all the publishers that the given author_id has worked.
     * @param author_id
     * @return QueryResult.ResultQ5[]
     */
    
    

    public QueryResult.ResultQ5[] functionQ5(int author_id) {
    	int size =0;
    	ResultSet result;
    	connect();
    	
    	String query = "select distinct a1.author_id, a1.author_name " + 
    			" from author a1 " + 
    			" where not exists (select p2.publisher_id from publisher p2,author_of f2 , book b2, author a " + 
    			"    						where f2.author_id = ? and  " + 
    			"    			                 f2.isbn = b2.isbn and " + 
    			"    			                 b2.publisher_id = p2.publisher_id and not exists (select a.author_id, a.author_name " + 
    			"						             											from author a, author_of f, book b, publisher p " + 
    			"									 											where a1.author_id = a.author_id and " + 
    			"																					a.author_id = f.author_id  and " + 
    			"									     											f.isbn = b.isbn and " + 
    			"																					b.publisher_id = p.publisher_id and " + 
    			"                                                                                    p.publisher_id = p2.publisher_id))  order by a1.author_id asc";
    	
    	try
    	{
    	PreparedStatement stat = this.connection.prepareStatement(query);
    	stat.setInt(1, author_id);
    	result = stat.executeQuery();
    	  	
	    	if (result != null) 
	    	{
	    		result.last();    	  size = result.getRow(); 
	    	}
    	}catch (SQLException e) {
			e.printStackTrace();
			}
    	
    	QueryResult.ResultQ5[] array = new QueryResult.ResultQ5[size];
    	
    	try
    	{
    	PreparedStatement stat = this.connection.prepareStatement(query);
    	stat.setInt(1, author_id);
    	result = stat.executeQuery();
    	int i = 0;
	    	while (result.next())
	    	{
	    		QueryResult.ResultQ5 qres = new QueryResult.ResultQ5(result.getInt(1), result.getString(2));
	    		array[i] = qres;
	    		i++;	    		
	    	}
	    stat.close();
    	}catch (SQLException e) {
			e.printStackTrace();
		}
    	
		return array;
    	
	}
    
    
    /**
     * List author_name(s) and isbn(s) of the book(s) written by "Selective" authors. 
     * "Selective" authors are those who have worked with publishers that have 
     * published their books only.(i.e., they haven't published books of 
     * different authors)
     * @return QueryResult.ResultQ6[]
     */
    
    

    public QueryResult.ResultQ6[] functionQ6() {
    	int size =0;
    	ResultSet result;
    	connect();
    	
    	String query = "select distinct a1.author_id, b1.isbn from author a1, book b1, author_of f1	" + 
    			"		where a1.author_id = f1.author_id and" + 
    			"			  f1.isbn = b1.isbn and a1.author_id not IN  (select distinct a.author_id from author a, book b, author_of f " + 
    			"		where a.author_id = f.author_id and " + 
    			"			  f.isbn = b.isbn and b.publisher_id " + 
    			"              IN (select b2.publisher_id " + 
    			"						from author a2, book b2, author_of f2 " + 
    			"                        where a.author_id != a2.author_id and " + 
    			"						a2.author_id = f2.author_id and " + 
    			"                        f2.isbn = b2.isbn and " + 
    			"                        b2.publisher_id = b.publisher_id) ) order by a1.author_id , b1.isbn asc;"; 
    	
    	try
    	{
    	PreparedStatement stat = this.connection.prepareStatement(query);
    	result = stat.executeQuery();
    	  	
	    	if (result != null) 
	    	{
	    		result.last();    	  size = result.getRow(); 
	    	}
    	}catch (SQLException e) {
			e.printStackTrace();
			}
    	
    	QueryResult.ResultQ6[] array = new QueryResult.ResultQ6[size];
    	
    	try
    	{
    	PreparedStatement stat = this.connection.prepareStatement(query);
    	result = stat.executeQuery();
    	int i = 0;
	    	while (result.next())
	    	{
	    		QueryResult.ResultQ6 qres = new QueryResult.ResultQ6(result.getInt(1), result.getString(2));
	    		array[i] = qres;
	    		i++;	    		
	    	}
	    stat.close();
    	}catch (SQLException e) {
			e.printStackTrace();
		}
    	
		return array;
    	
    	
	}

    /**
     * List publisher_id and publisher_name of the publishers who have published 
     * at least 2 books in  'Roman' category and average rating of their books 
     * are more than (>) the given value.
     * @param rating
     * @return QueryResult.ResultQ7[]
     */
    
        
    public QueryResult.ResultQ7[] functionQ7(double rating) {
    	int size =0;
    	ResultSet result;
    	connect();
    	
    	String query = "select distinct p.publisher_id, p.publisher_name from publisher p, publisher p2, book b, book b2 " + 
    			"		where p.publisher_id = b.publisher_id and " + 
    			"				p.publisher_id = b2.publisher_id and " + 
    			"                b.category = \"Roman\" and " + 
    			"                b2.category = \"Roman\" and " + 
    			"                b.isbn != b2.isbn and " + 
    			"                ? < (select avg(b3.rating) " + 
    			"						from book b3 " + 
    			"							where b3.publisher_id = b.publisher_id " + 
    			"								group by b3.publisher_id) order by p.publisher_id asc;";
    	
    	try
    	{
    	PreparedStatement stat = this.connection.prepareStatement(query);
    	stat.setDouble(1, rating);
    	result = stat.executeQuery();
    	  	
	    	if (result != null) 
	    	{
	    		result.last();    	  size = result.getRow(); 
	    	}
    	}catch (SQLException e) {
			e.printStackTrace();
			}
    	
    	QueryResult.ResultQ7[] array = new QueryResult.ResultQ7[size];
    	
    	try
    	{
    	PreparedStatement stat = this.connection.prepareStatement(query);
    	stat.setDouble(1, rating);
    	result = stat.executeQuery();
    	int i = 0;
	    	while (result.next())
	    	{
	    		QueryResult.ResultQ7 qres = new QueryResult.ResultQ7(result.getInt(1), result.getString(2));
	    		array[i] = qres;
	    		i++;	    		
	    	}
	    stat.close();
    	}catch (SQLException e) {
			e.printStackTrace();
		}
    	
		return array;
	}
    
    /**
     * Some of the books  in the store have been published more than once: 
     * although they have same names(book\_name), they are published with different
     * isbns. For each  multiple copy of these books, find the book_name(s) with the 
     * lowest rating for each book_name  and store their isbn, book_name and 
     * rating into phw1 table using a single BULK insertion query. 
     * If there exists more than 1 with the lowest rating, then store them all.
     * After the bulk insertion operation, list isbn, book_name and rating of 
     * all rows in phw1 table.
     * @return QueryResult.ResultQ8[]
     */

    public QueryResult.ResultQ8[] functionQ8() {
    	int size =0;
    	ResultSet result;
    	connect();
    	
    	String query = "insert into phw1 (isbn, book_name, rating) select b.isbn, b.book_name, b.rating from book b, book b1 " + 
    			"					where b.book_name = b1.book_name and " + 
    			"						b.isbn != b1.isbn and b.rating = (select min(b3.rating) " + 
    			"																from book b3, book b2 " + 
    			"															where b2.book_name = b3.book_name and " + 
    			"																	b3.isbn != b2.isbn) ;";
    	
    	String query2 = "select b.isbn, b.book_name, b.rating from phw1 b order by b.isbn asc ;";
    	
    	try
    	{
    	PreparedStatement stat = this.connection.prepareStatement(query);
    	stat.executeUpdate();	
    		
    	PreparedStatement stat2 = this.connection.prepareStatement(query2);
    	result = stat2.executeQuery();
    	  	
	    	if (result != null) 
	    	{
	    		result.last();    	  size = result.getRow(); 
	    	}
    	}catch (SQLException e) {
			e.printStackTrace();
			}
    	
    	QueryResult.ResultQ8[] array = new QueryResult.ResultQ8[size];
    	
    	try
    	{
    		PreparedStatement stat2 = this.connection.prepareStatement(query2);
        	result = stat2.executeQuery();
        	
    	int i = 0;
	    	while (result.next())
	    	{
	    		QueryResult.ResultQ8 qres = new QueryResult.ResultQ8(result.getString(1), result.getString(2), result.getDouble(3));
	    		array[i] = qres;
	    		i++;	    		
	    	}
	    stat2.close();
    	}catch (SQLException e) {
			e.printStackTrace();
		}
    	
		return array;
    	
	}
    
    /**
     * For the books that contain the given keyword anywhere in their names, 
     * increase their ratings by one. 
     * Please note that, the maximum rating cannot be more than 5, 
     * therefore if the previous rating is greater than 4, do not update the 
     * rating of that book. 
     * @param keyword
     * @return sum of the ratings of all books
     */

    public double functionQ9(String keyword) {
    	double number = 0;
    	ResultSet result;
    	keyword = "%" + keyword + "%" ;
    	connect();
    	
    	String query = "Update book  set rating = rating + 1 where isbn != '' and isbn in " + 
    			         "  (select isbn from (select * from book) as pat where pat.book_name LIKE ? and pat.rating <= 4) ;";
    	
    	String query2 = " select sum(b.rating) from book b; ";
    	
    	try
    	{
    	PreparedStatement stat = this.connection.prepareStatement(query);
    	stat.setString(1, keyword);
        stat.executeUpdate();
        
        Statement stat2 = this.connection.createStatement();
    	result = stat2.executeQuery(query2);
    	result.next();
    	number = result.getDouble(1);
    	
	    stat2.close();
        stat.close();
    	}catch (SQLException e) {
			e.printStackTrace();
			}
    
    	
		return number;
    	
	}
    
    /**
     * Delete publishers in publisher table who haven't published a book yet. 
     * @return count of all rows of the publisher table after delete operation.
     */
    public int function10() {
    	int number = 0;
    	ResultSet result;
    	connect();
    	
    	String query = "delete from publisher where publisher_id != '' and publisher_id in (select * from (select distinct p1.publisher_id from book b1, publisher p1 where p1.publisher_id not in " + 
    			"				(select distinct p.publisher_id from book b, publisher p " + 
    			"				where p.publisher_id in (select b.publisher_id))) as t) ;"; 
    	
    	String query2 = " select count(*) from publisher ;";
    	
    	try
    	{
    	PreparedStatement stat = this.connection.prepareStatement(query);
    	stat.executeUpdate();
    	
    	PreparedStatement stat2 = this.connection.prepareStatement(query2);
    	result = stat2.executeQuery(query2);
    	result.next();
    	number = result.getInt(1);
    	
	    stat.close();
    	}catch (SQLException e) {
			e.printStackTrace();
		}
    	
		return number;
	}

}	
			
		