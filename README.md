# dockerized-extract-load-pipeline

This project aim to extract data every certain time from mysql database on a server and load it into local postgres database in a dockerized way which we have a postgres image and python code image inside docker compose and all the required settings for python image prepared in the dockerfile but: 
- 
## I had two cases to handle: 
- the new data may be new
- or an updated data
- so I had to choose the best solution to handle it



# As you may see in el_class.py => load_postgres function: 
- there are two solutions:
- B: which delete all the union rows from postgres database then insert all the data
- C: which make a copy Update set using another table then insert the different rows

- After benchmark I concluded that solution B would be better when dealing with bigger data

  
## Feel free to use any part of the project 
### you need to edit create your own .env file
### and customize the sql queries depending on your case
