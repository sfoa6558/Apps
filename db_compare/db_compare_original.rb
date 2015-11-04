
#do an outer loop for rows in csv file
# do an inner loop for two columns
# use a flag for connection details to check what details. 
# stick results into an array
#!/user/bin/env ruby

require 'csv'
require 'pg'
require 'pry'
#require 'hashdiff'
#retrieves data from nc and eis
class DataRetrieval
   attr_accessor :eis_data,:nc_data
   EIS = {host: "eis_a-oak.enova.com", dbname: "testing", user: "fakinyemi", password: "Sfoa6559"}
   NC =  {host: "nc-report_a-oak.cashnetusa.com", dbname: "nc", user: "fakinyemi", password: "Sfoa6559"}   
  
  def initialize(csv)
     @eis_data = [ ]
     @nc_data = [ ] 
    # delete_contents(csv)
     parse(csv)
  end

  def retrieve_data(query_string,conn)
       connection  =  conn_data(conn)
      # puts connection
       connection.exec(query_string) do |result|
         result.each do |row|
           @eis_data << row if conn[:host] == 'eis_a-oak.enova.com' 
           @nc_data << row if conn[:host] == 'nc-report_a-oak.cashnetusa.com'
         end
       end
     # connection.close 
  end 
  
  def compare_data(nc,eis,queries)
     diff_nc_data  =  nc - eis
     diff_eis_data =  eis - nc
     diff_data  =   diff_nc_data + diff_eis_data
     parse_data(diff_data,queries)
  end  
  
  def parse_data(difference,query_list)
      query =  query_list.to_s.split('|')
      nc_query  =   query[0]
      eis_query = query[1]
      nc  =  nc_query.to_s
      eis  =   eis_query.to_s 
      output_data(nc,eis,difference)
 end

 def output_data(nc,eis,difference)
     results = 'There is no difference in your queries'  if difference.count == 0
     results = difference.to_s   if difference.count > 0
     db_out = File.open("db_compare.out", "a")
     db_out.puts "RESULTS" unless File.foreach("db_compare.out").grep(/RESULTS/).any?
     db_out.puts
     db_out.puts nc + "  " + eis
     db_out.puts results
     db_out.close
 end 
 
 def delete_contents(csv)
     File.truncate(csv,0)
 end
 
  def parse(csv)
        CSV.foreach(csv,headers: true )do |row| 
                 queries  =   row 
                 row.to_s.split('|').each do |query|
                     retrieve_data(query,EIS)  if  query.to_s.include? "installments_fact_materialized"
                     retrieve_data(query,NC)   if  query.to_s.include? "nc_reporting.installments"
                 end
               
                 compare_data(@eis_data,@nc_data,queries)
       end  
  end  


 private
    def conn_data(conn)
        PG.connect(conn)
    end


end
#Output a table of connections to the DB


dataRetrieval  =   DataRetrieval.new('db_compare.csv')

