# Top-N-analysis-with-map-reduce

Objective: To derive the following statistics from YELP dataset using topN design pattern of Map Reduce: 
  1. Top 10 locations zip codes where Maximum number of businesses are happening 
  2. Top 10 Business with average user rating as parameter 

Data Set Descrption: 
  1. Business.csv file with "business_id","full_address","categories"
  2. Review.csv file with "review_id","user_id","business_id","stars"
  3. Users.csv file with "user_id","name","url"
  
Approach:  
1. Top Locations:
   * mapper phase: emits <k,v> pairs as <zipcode, 1> for every entry from business file
   * reduce phase: count the no.of.businesses grouped by zip code and store the results in an associative array.
   * cleanup phase: sort the array and output top 10 results. 
2. Top Businesses:
   * mapper phase: emits <k,v> pairs as <business_id, stars> for every entry from ratings file
   * reduce phase: compute the average of stars grouped by business_id and store the results in an associative array.
   * cleanup phase: sort the array and output top 10 results. 
 
