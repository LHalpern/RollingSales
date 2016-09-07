# RollingSales
Analyze Manhattan Rolling Condominium and Cooperative Sales

Using Pandas and Bokeh to Analyze and Visualze NYC Cooperative and Condominium Apartment Sales 

My goal is to review the rolling NYC sales data to see what patterns emerge in the area of cooperative and condominium apartment sales.  My initial question is which cooperative and condominium apartment buildings have the most sales by number and percentage.  I hope that this will identify some interesting patterns that can be further explored.
 
The overall plan is below:
 
     1) Load the rolling sales data into a dataframe and clean it   
     2) Use information from the Department of Finance to convert the condominium lot number to building lot number so that sales per building can be determined       
     3) Merge the data with information on number of residential units per building from the PLUTO database so that percentage sales per building can be calculated
     4) Use "groupby" and "sort" to obtain the top 10 sales per building (by both number and percentage)
     5) Create a series of bar charts using Bokeh to visualize this data
