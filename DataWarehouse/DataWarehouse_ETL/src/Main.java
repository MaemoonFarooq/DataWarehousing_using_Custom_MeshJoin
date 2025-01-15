import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class Main {
    public static void main(String[] args) {
    	
    	System.out.println("Welcome to real DWH with extended mesh join");
    	System.out.println("Please enter your Database credentials:");
        //Taking input from user to connect to the database
        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter Database username:");
        String user = scanner.nextLine();

        System.out.print("Enter Database password:");
        String password = scanner.nextLine();

        String dbUrl = "jdbc:mysql://localhost:3306/Datawarehouse"; 

        // Queue to hold the row data for check 
        Queue<String[]> queue = new LinkedBlockingQueue<>(); 

        // HashMap to store the transactional data split up into 2 parts products and customer with relevant information
        Map<Integer, String[]> hashMap = new HashMap<>();

        // Partions to store the MD information
        List<List<String[]>> productPartitions = new ArrayList<>();
        List<List<String[]>> customerPartitions = new ArrayList<>();

        try {
        	Connection connection = null;
            connection = DriverManager.getConnection(dbUrl, user, password); // Assign connection here
            System.out.println("Database connection established");

            // Queries to retrive the data from the tables in SQL stored previously using the python script
            String query1 = "SELECT * FROM Orders";
            String query2 = "SELECT * FROM Products";
            String query3 = "SELECT * FROM Customer";
            
            //excuting all the queires to start up the DWH
            PreparedStatement preparedStatement1 = connection.prepareStatement(query1);
            ResultSet transactionSet = preparedStatement1.executeQuery();
            
            PreparedStatement productStatement = connection.prepareStatement(query2);
            ResultSet productSet = productStatement.executeQuery();
            
            PreparedStatement customerStatement = connection.prepareStatement(query3);
            ResultSet customerSet = customerStatement.executeQuery();

            //First thread that would start the flow of the transactional data
            Thread QueueAndStreamingDataThread = new Thread(() -> {
            	
                try {
                	System.out.println("Transaction Stream Starting:");
                	//Retriving and storing the transactional data from the sql
                    while (transactionSet.next()) {
                        int orderId = transactionSet.getInt("OrderID");
                        String orderDate = transactionSet.getString("OrderDate");
                        int productId = transactionSet.getInt("ProductID");
                        int quantityOrdered = transactionSet.getInt("QuantityOrdered");
                        int customerId = transactionSet.getInt("CustomerID");
                        int timeId = transactionSet.getInt("TimeID");

                        // loading a row
                        String[] row = new String[] {
                            String.valueOf(orderId),
                            orderDate,
                            String.valueOf(productId),
                            String.valueOf(quantityOrdered),
                            String.valueOf(orderId),
                            String.valueOf(orderDate),
                            String.valueOf(customerId),
                            String.valueOf(timeId)
                        };

                        // Add the row to the queue
                        queue.add(row);

                        // Hashing the rows with two hash functions so that there is no chance of overwriting as that would cause time complexity of O(N) 
                        // we want O(1)complexity
                        hashForProduct(hashMap,orderId,orderDate,productId, quantityOrdered);
                        hashForCustomer(hashMap,orderId,orderDate, customerId, timeId);

                        //System.out.println("Queue contents before flushing:");
                        //printQueue(queue);

                       // System.out.println("HashMap contents");
                       // printHashMap(hashMap);

                        // addinng a wait for 5 seconds so that the MD could join with the relevant information
                        Thread.sleep(3000); 

                        //flushing both the queue and the hash map as the limit is set to 5
                        if (queue.size() >= 5) {
                            HashmapAndQueueFlush(queue, hashMap); 
                        }
                    }

                    // Print the HashMap
                    System.out.println("Final HashMap contents:");
                   // printHashMap(hashMap);

                } catch (SQLException | InterruptedException e) {
                    System.err.println("Error occurred in data fetching and processing thread.");
                    e.printStackTrace();
                }
            });
            
            //Second Thread to manage the flow
            Thread MasterDataAndMeshJoinThread = new Thread(() -> {
                try {
                    // Process the productSet and partition it into 5 parts
                    List<String[]> productData = new ArrayList<>();
                    while (productSet.next()) {
                        int productId = productSet.getInt("productID");
                        String productName = productSet.getString("productName");
                        BigDecimal productPrice = productSet.getBigDecimal("productPrice");
                        int supplierId = productSet.getInt("supplierID");
                        String supplierName = productSet.getString("supplierName");
                        int storeId = productSet.getInt("storeID");
                        String storeName = productSet.getString("storeName");

                        String[] productRow = new String[] {
                            String.valueOf(productId),
                            productName,
                            String.valueOf(productPrice),
                            String.valueOf(supplierId),
                            supplierName,
                            String.valueOf(storeId),
                            storeName
                        };
                        productData.add(productRow);
                    }
                    partitionData(productData, productPartitions);

                    // Process the customerSet and partition it into 5 parts
                    List<String[]> customerData = new ArrayList<>();
                    while (customerSet.next()) {
                        int customerId = customerSet.getInt("customer_id");
                        String customerName = customerSet.getString("customer_name");
                        String gender = customerSet.getString("gender");

                        String[] customerRow = new String[] {
                            String.valueOf(customerId),
                            customerName,
                            gender
                        };
                        customerData.add(customerRow);
                    }
                    partitionData(customerData, customerPartitions);

//                    System.out.println("Master Data Loaded");

                 // Data sets to store matches
                    Set<String> matchedCustomers = new HashSet<>();
                    Set<String> matchedProducts = new HashSet<>();

                    while (true) {
                        // Check for product matches and store them
                        for (List<String[]> productPartition : productPartitions) {
                            for (String[] productRow : productPartition) {
                                int productId = Integer.parseInt(productRow[0]);
                                String productName = productRow[1];
                                BigDecimal productPrice = new BigDecimal(productRow[2]);
                                int supplierId = Integer.parseInt(productRow[3]);
                                String supplierName = productRow[4];
                                int storeId = Integer.parseInt(productRow[5]);
                                String storeName = productRow[6];
                                int hashKey = ProductHashMatch(hashMap, productId);

                                if (hashMap.containsKey(hashKey)) {
                                    String match = String.format(
                                        "Product Match Found: ProductID: %s, ProductName: %s, ProductPrice: %s, SupplierID: %s, SupplierName: %s, StoreID: %s, StoreName: %s, OrderId: %s, OrderDate: %s, QuantityOrdered %s",
                                        productId, productName, productPrice, supplierId, supplierName, storeId, storeName, 
                                        hashMap.get(hashKey)[0], hashMap.get(hashKey)[1], hashMap.get(hashKey)[2]);

                                    matchedProducts.add(match);
                                }
                            }
                        }

                        // Check for customer matches and store them
                        for (List<String[]> customerPartition : customerPartitions) {
                            for (String[] customerRow : customerPartition) {
                                int customerId = Integer.parseInt(customerRow[0]);
                                String customerName = customerRow[1];
                                String gender = customerRow[2];
                                int hashKey = CustomerHashMatch(hashMap, customerId);

                                if (hashMap.containsKey(hashKey)) {
                                    String match = String.format(
                                        "Customer Match Found: CustomerID: %s, Name: %s, Gender: %s, OrderID: %s, OrderDate %s",
                                        customerId, customerName, gender, hashMap.get(hashKey)[0], hashMap.get(hashKey)[1]
                                    );

                                    matchedCustomers.add(match);

                                    if (matchedCustomers.size() >= 5 && matchedProducts.size() >= 5) {
                                        for (String productMatch : matchedProducts) {
                                            String[] productParts = productMatch.split(", ");
                                            String productOrderId = productParts[7].split(": ")[1]; // Extracting the order id in the products part

                                            for (String customerMatch : matchedCustomers) {
                                                String[] customerParts = customerMatch.split(", ");
                                                String customerOrderId = customerParts[3].split(": ")[1]; // Extracting the order id in the customer part

                                             // If the OrderId matches, print the match
                                                if (productOrderId.equals(customerOrderId)) {
                                                    String finaloutput = productMatch + " " + customerMatch;
                                                    String[] finalproducts = finaloutput.split(", ");

                                                    // Print the final array
                                                    System.out.println("Matched Pair based on OrderId:");
                                                    for (String item : finalproducts ) {
                                                        System.out.print(item + ", ");
                                                    }
                                                    
                                                 // Now insert the data into the database
                                                    // Assuming the fields you want to insert are from the finalproducts 
                                                    String productId = finalproducts [0].split(": ")[2];//done
                                                    String productName = finalproducts [1].split(": ")[1];//done
                                                    String productPrice = finalproducts [2].split(": ")[1];//done
                                                    String supplierId = finalproducts [3].split(": ")[1];//done
                                                    String supplierName = finalproducts [4].split(": ")[1];//done
                                                    String storeId = finalproducts [5].split(": ")[1];//done
                                                    String storeName = finalproducts [6].split(": ")[1];//done
                                                    String orderId = finalproducts [7].split(": ")[1];////done
                                                    String orderDate = finalproducts [8].split(": ")[1];//orderDate//done
                                                    String quantityOrdered= "1" ;
                                                    String CustomerID= finalproducts [9].split(": ")[2]; ////done 
                                                    String CustomerName = finalproducts [10].split(": ")[1]; //CustomerName
                                                    String Gender = finalproducts [11].split(": ")[1]; //Gender
                                                      
                                                  // System.out.println("toooooo");
                                                   // System.out.println(Gender); 
                                                    
                                                    

                                                   String sql = "INSERT INTO FactTable (productId, productName, productPrice, supplierId, supplierName, storeId, storeName, orderId, orderDate, quantityOrdered, customerId, customerName, gender) "
                                                           + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                                                	
                                                 Connection connection2 = null;
                                                  connection2 = DriverManager.getConnection(dbUrl, user, password);
                                                    
                                                  PreparedStatement pstmt = connection2.prepareStatement(sql);
                                                  pstmt.setInt(1, Integer.parseInt(productId));
                                                  pstmt.setString(2, productName);
                                                  pstmt.setBigDecimal(3, new BigDecimal(productPrice));
                                                  pstmt.setInt(4, Integer.parseInt(supplierId));
                                                  pstmt.setString(5, supplierName);
                                                  pstmt.setInt(6, Integer.parseInt(storeId));
                                                  pstmt.setString(7, storeName);
                                                  pstmt.setInt(8, Integer.parseInt(orderId));
                                                  pstmt.setTimestamp(9, Timestamp.valueOf(orderDate)); 
                                                  pstmt.setInt(10, Integer.parseInt(quantityOrdered));
                                                  pstmt.setInt(11, Integer.parseInt(CustomerID));
                                                  pstmt.setString(12, CustomerName);
                                                  pstmt.setString(13, Gender);

                                                  pstmt.executeUpdate();
                                                    
                                                    
                                                }    
                                            }
                                        }

                                        matchedCustomers.clear();
                                        matchedProducts.clear();
                                    }
                                }
                            }
                    }
                        Thread.sleep(3000); 
                    }

                } catch (SQLException e) {
                    System.err.println("Error occurred in product and customer processing thread.");
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    System.err.println("Thread was interrupted.");
                }
            });

            // Start both threads
            QueueAndStreamingDataThread.start();
            MasterDataAndMeshJoinThread.start();

            QueueAndStreamingDataThread.join();
            MasterDataAndMeshJoinThread.join();

        } catch (SQLException | InterruptedException e) {
            System.err.println("Error occurred.");
            e.printStackTrace();
        } 
            scanner.close();
        }
    

// helper function that helps match the product id and get the contents
	private static int ProductHashMatch(Map<Integer, String[]> hashMap, int productId) {
        int key = productId * 31 ; 
        return key;
    }
// helper function that helps match te customer id and get the contents
    private static int CustomerHashMatch(Map<Integer, String[]> hashMap, int customerId) {
        int key = customerId * 41 ; 
        return key;
        
    }
    
//  hashForProduct(hashMap,orderId,orderDate,productId, quantityOrdered);
//  hashForCustomer(hashMap,orderId,orderDate, customerId, timeId);

    //Hashing based on ProductID 
    private static void hashForProduct(Map<Integer, String[]> hashMap,int orderID,String orderDate, int productId, int quantityOrdered) {
        int key = productId * 31 ; 
        hashMap.put(key, new String[] {String.valueOf(orderID), String.valueOf(orderDate), String.valueOf(productId), String.valueOf(quantityOrdered)}); 
    }

    //Hashing based on CustomerID 
    private static void hashForCustomer(Map<Integer, String[]> hashMap, int orderID,String orderDate,int customerId, int timeId) {
        int key = customerId * 41 ; 
        hashMap.put(key, new String[] {String.valueOf(orderID), String.valueOf(orderDate),String.valueOf(customerId), String.valueOf(timeId)}); 
    }

    //function to flush the queue and hashMap 
    private static void HashmapAndQueueFlush(Queue<String[]> queue, Map<Integer, String[]> hashMap) {
        System.out.println("Flushing queue...");

        // Process the items in the queue
        while (!queue.isEmpty()) {
            String[] row = queue.poll();
            System.out.printf("Queue processed: OrderID: %s, OrderDate: %s, ProductID: %s, QuantityOrdered: %s, CustomerID: %s, TimeID: %s%n", 
                row[0], row[1], row[2], row[3], row[4], row[5]);
        }

        // Process the items in the HashMap
        System.out.println("Flushing HashMap...");
        for (Map.Entry<Integer, String[]> entry : hashMap.entrySet()) {
            System.out.printf("HashMap Key: %d,%s, %s%n", 
                entry.getKey(), entry.getValue()[0], entry.getValue()[1]);
        }

        // Clearing so that no garbage or data is left
        queue.clear();
        hashMap.clear();

        System.out.println("Queue and HashMap flushed and cleared.");
    }

    // Function to partition data into 5 parts
    private static void partitionData(List<String[]> data, List<List<String[]>> partitions) {
        int partitionSize = (int) Math.ceil(data.size() / 5.0);
        for (int i = 0; i < 5; i++) {
            int start = i * partitionSize;
            int end = Math.min((i + 1) * partitionSize, data.size());
            partitions.add(new ArrayList<>(data.subList(start, end)));
        }
    }


    //Function to print the contents of the queue
    private static void printQueue(Queue<String[]> queue) {
        for (String[] row : queue) {
            System.out.println(Arrays.toString(row));
        }
    }

    //Function to print the contents of the HashMap
    private static void printHashMap(Map<Integer, String[]> hashMap) {
        for (Map.Entry<Integer, String[]> entry : hashMap.entrySet()) {
            System.out.printf("HashMap Key: %d -> Value: %s, %s%n", 
                entry.getKey(), entry.getValue()[0], entry.getValue()[1]);
        }
    }
}
