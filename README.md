# NetNinjas
## Introduction
This search engine project consists of a Java-based backend for crawling and page ranking, and a frontend for user interface. The backend crawls and indexes web pages, calculates page ranks and serves search results to the frontend. The frontend provides a user-friendly interface for searching and viewing results.    

[//]: # (***The website is available on ```http://3.228.112.141:8080/```, ```http://netninja.cis5550.net:8080/```, and ```https://netninja.cis5550.net:8443/```***)

## Folder Structure
**pages**
The frontend codes of the project using HTML&CSS

**src**
The backend codes of the project using Java

**lib**
The jar libraries backend codes depend on

**bin**
The classess files

## External Resources
- Boostrap(https://getbootstrap.com/) - Powerful, extensible, and feature-packed frontend toolkit. Build and customize with Sass, utilize prebuilt grid system and components, and bring projects to life with powerful JavaScript plugins.
- Gson(https://github.com/google/gson) - Gson is a Java library that can be used to convert Java Objects into their JSON representation. It can also be used to convert a JSON string to an equivalent Java object.
- GloVe(https://nlp.stanford.edu/projects/glove/) - GloVe is an unsupervised learning algorithm for obtaining vector representations for words. Training is performed on aggregated global word-word co-occurrence statistics from a corpus, and the resulting representations showcase interesting linear substructures of the word vector space.
- stopwords.txt(https://gist.github.com/larsyencken/1440509) - A txt file contains all common English stopwords
- englishwords.txt(https://github.com/dwyl/english-words) - A txt file contains all common English words.

## Supported Routes
### Frontend Routes
#### Route 1
***`get("/")`***

This route displays the main page of Nenninjas search engine. On this page, user can type in any word or phrases to search. 

*Request Params*:  
- None  

*Query Params*:
- None

*Example Input*:  
```
http://netninja.cis5550.net:8080/
```

*Example Response*: 
Page with Netninjas title and single search bar just like Google. 

#### Route 2
***`get("/searchResults")`***

This API shows the search results and display those results to the user. 

*Request Params*:  
- None  

*Query Params*:
- **query**: the user input search query passed from the frontend

*Example Input*:  
```
http://netninja.cis5550.net:8080/searchResults?query=amazon
```

*Example Response*: 
The webpage displays search results and features a search bar at the top for users to refine their search. The left side of the screen presents each result in a format that includes the page title, URL, and a snippet. On the top right corner, there is a list of five synonym words related to the search input.

### Backend Routes
#### Route 1
***`get("/search") `*** 

This search API accepts a query string as input and return a list of search results that match the query.

*Request Params*:  
- None  

*Query Params*:
- **query**: the user input search query passed from the frontend
- **pageSize**: the number of results on a single page
- **pageNum**: the current page number

*Example Input*:  
```
http://netninja.cis5550.net:8080/search?query=apple?&pageSize=10&pageNum=1
```

*Example Response*: 
```json 
{ "count": [ { "url": "1591", "title": "", "snippet": "" } ], "results": [ { "url": "https://music.apple.com:443/mz/browse", "title": " apple music", "snippet": "apple music african davido stays focused on the bag on unavailable...." }, { "url": "https://music.apple.com:443/lr/browse", "title": " apple music", "snippet": "apple music african a seminal voice in abujas hiphop scene comes to the fore...." }]}
```

#### Route 2
***`get("/synonym")`***  

This route will return sentences that are similar to input query and synonym words of each non-stop words

*Request Params*:  
- None  

*Query Params*:  
- **query**: the user input search query passed from the frontend

*Example Input*:
```  
http://netninja.cis5550.net:8080/synonym?query=pizza%20near%20Sydney_help
```

*Example Response*:  
```json
{ "results": { "similarSentences": [ "sandwich near sydney help", "pizza nearby sydney help", "pizza near melbourne help", "pizza near sydney helping" ], "storedSynonyms": [ { "rootWord": "help", "similarWords": [ "helping", "bring", "need", "take", "helps" ] }, { "rootWord": "sydney", "similarWords": [ "melbourne", "adelaide", "brisbane", "perth", "auckland" ] }, { "rootWord": "near", "similarWords": [ "nearby", "town", "area", "outskirts", "northeast" ] }, { "rootWord": "pizza", "similarWords": [ "sandwich", "sandwiches", "snack", "bakery", "fries" ] } ] } }  
```

#### Route 3
***`get("/autocomplete")`***   
This API accepts a query string as input and return a list of suggested search terms based on the last word of the search string.

*Request Params*:  
- None

*Query Params*:
- **query**: the user input search query passed from the frontend

*Example Input*:  
```
http://netninja.cis5550.net:8080/autocomplete?query=appl
```

*Example Response*:  
```json
{ "suggestions": [ { "term": "appl", "count": 0 }, { "term": "applause", "count": 0 }, { "term": "applauses", "count": 0 }, { "term": "applausive", "count": 0 }, { "term": "applausively", "count": 0 }, { "term": "applaud", "count": 0 }, { "term": "applaudable", "count": 0 }, { "term": "applaudably", "count": 0 }, { "term": "applauds", "count": 0 }, { "term": "applauder", "count": 0 } ] }
```

#### Route 4
***`get("/emptyquery")`***  
This route can be called when the input query is null. The response will be up to 5 most recent search histories.

*Request Params*:  
- None

*Query Params*:  
- None

*Example Input*:  
```
http://netninja.cis5550.net:8080/emptyquery
```

*Example Response*:  
```json
{ "results": [ { "url": "https://en.wikipedia.org:443/wiki/2023_in_hip_hop_music"}, { "url": "https://en.wikipedia.org:443/wiki/Taylor_Swift_(album)"} ] } 
```

## Instructions to Download Necessary Files and Start the Server both locally and on AWS EC2
***Note: The below steps are used only if you want to run program by yourself. Our server has been already started in the background. You can use our website directly.***
__________
## Run Program locally:
### Download large files:
***Put these three files into the /data folder***  
- `englishwords.txt`: ```https://drive.google.com/file/d/1SaXCfJu-zUjSiWVWMDbGxpu3QhbFaZKc/view?usp=share_link```  
- `glove.6B.50d.txt`: ```https://drive.google.com/file/d/1ZPHgJB1L90dNspy085HD0L7UWjgRYNua/view?usp=share_link ``` 
- `stopwords.txt`: ```https://drive.google.com/file/d/1VpvjO7y1P5LA-gbdQunnQhzqbWIZQ9t2/view?usp=share_link  ```

***Download six workers folders from Google Drive and only leave id, urlpages.table, index.table, and pageranks.table in the folders***
- `Google Drive Link`: ```https://drive.google.com/drive/u/1/folders/1ZJzV18o8drNQDO7vvCZWd3MWLW1HS1Wk```

***Note***: Each worker folder needs to be stay on the root directory of /NetNinjas-ssh.   
***important***: Copy and paste worker2/pageranks.table into data/. Then rename the 'pageranks.table' to 'pageranks2.table'   

### Compile all Ranking Related Files and Jar All of Them Into ranking.jar
First compile those files:
1. ```javac -cp lib/webserver.jar:lib/kvs.jar:lib/gson.jar --source-path src src/cis5550/jobs/ProcessInput.java```
2. ```javac -cp lib/webserver.jar:lib/kvs.jar:lib/gson.jar --source-path src src/cis5550/jobs/RankScore.java```
3. ```javac -cp lib/webserver.jar:lib/kvs.jar:lib/gson.jar --source-path src src/cis5550/jobs/SearchApi.java```   

Then jar them into ranking.jar:   
- ```jar -cvf ../lib/ranking.jar cis5550/jobs/*.class```

### Start the backend server(make sure you run those command in local environment)
To start the server, first start the KVS master:
- ```java -cp lib/kvs.jar:lib/webserver.jar:lib/tools.jar cis5550.kvs.Master 8000 ```

And then six KVS workers:   
- ```java -cp lib/kvs.jar:lib/webserver.jar:lib/tools.jar cis5550.kvs.Worker 8001 worker1 localhost:8000```
- ```java -cp lib/kvs.jar:lib/webserver.jar:lib/tools.jar cis5550.kvs.Worker 8002 worker2 localhost:8000```    
- ```java -cp lib/kvs.jar:lib/webserver.jar:lib/tools.jar cis5550.kvs.Worker 8003 worker3 localhost:8000```
- ```java -cp lib/kvs.jar:lib/webserver.jar:lib/tools.jar cis5550.kvs.Worker 8004 worker4 localhost:8000``` 
- ```java -cp lib/kvs.jar:lib/webserver.jar:lib/tools.jar cis5550.kvs.Worker 8005 worker5 localhost:8000```
- ```java -cp lib/kvs.jar:lib/webserver.jar:lib/tools.jar cis5550.kvs.Worker 8006 worker6 localhost:8000```

***Before proceding to the next step, please wait until all KVS workers has been active with data loaded*** 
- Check the status of backend KVS workers: ```http://localhost:8000/```

Finally start the backend server locally and wait until all cached data is prepared:
- ***java -cp lib/kvs.jar:lib/webserver.jar:lib/gson.jar:lib/ranking.jar:lib/tools.jar src/cis5550/jobs/SearchApi.java localhost:8000 8080 8443***

Note that the last three arguments correspond to **KVSClient address**, **Backend unsecure port number**, and ***Backend secure port number*** respectively.  
____
## Run Program on EC2:
***Note: There is no need to download files. All files has been uploaded on the EC2 instances***
### SSH to fource EC2 instances
***Find the two pem files in the /pem folder***    
Master instances:   
- ```ssh ec2-user@3.228.112.141 -i pem/CIS5550-NetNinjas-master.pem```

Worker isntances:
- ```ssh ec2-user@100.24.160.86 -i pem/CIS5550-NetNinjas-worker.pem```
- ```ssh ec2-user@44.207.197.71 -i pem/CIS5550-NetNinjas-worker.pem```
- ```ssh ec2-user@44.208.245.156 -i pem/CIS5550-NetNinjas-worker.pem```

### Start the backend server
To start the server, first start the KVS master on the Master EC2(public IP: 3.228.112.141):

- ```java -cp lib/kvs.jar:lib/webserver.jar:lib/tools.jar cis5550.kvs.Master 8000 ```

And then six KVS workers:  
Worker 1 and 2 run on one EC2 instance(public IP: 100.24.160.86): 
- ```java -cp lib/kvs.jar:lib/webserver.jar:lib/tools.jar cis5550.kvs.Worker 8001 worker1 100.24.160.86:8000```
- ```java -cp lib/kvs.jar:lib/webserver.jar:lib/tools.jar cis5550.kvs.Worker 8002 worker2 100.24.160.86:8000```  

Worker 3 and 4 run on one EC2 instance(public IP: 44.207.197.71):   
- ```java -cp lib/kvs.jar:lib/webserver.jar:lib/tools.jar cis5550.kvs.Worker 8001 worker3 100.24.160.86:8000```
- ```java -cp lib/kvs.jar:lib/webserver.jar:lib/tools.jar cis5550.kvs.Worker 8002 worker4 100.24.160.86:8000```

Worker 5 and 6 run on one EC2 instance(public IP: 44.208.245.156):   
- ```java -cp lib/kvs.jar:lib/webserver.jar:lib/tools.jar cis5550.kvs.Worker 8001 worker5 100.24.160.86:8000```
- ```java -cp lib/kvs.jar:lib/webserver.jar:lib/tools.jar cis5550.kvs.Worker 8002 worker6 100.24.160.86:8000```

***Before proceding to the next step, please wait until all KVS workers has been active with data loaded*** 
- Check the status of backend KVS workers: ```http://100.24.160.86:8000/```

Finally start the backend server on the Master EC2(public IP: 3.228.112.141) and wait until all cached data is prepared:
- ***java -cp lib/kvs.jar:lib/webserver.jar:lib/gson.jar:lib/ranking.jar:lib/tools.jar src/cis5550/jobs/SearchApi.java 100.24.160.86:8000 8080 8443***

## EC2 Deployment and Setup
We have deployed our web application on AWS EC2 instances and set up a domain name for it. The domain name is ```netninja.cis5550.net```
### Git setup
To install Git on an EC2 instance running Amazon Linux 2, you can use the following command:
```bash
sudo yum install git -y
git --version
```

To set up Git on your machine, follow these steps:
1. Configure Git: After installing Git, you need to configure your Git username and email address using the following commands:

```bash
git config --global user.name "Your Name"
git config --global user.email "youremail@example.com"
```
Replace "Your Name" and "youremail@example.com" with your actual name and email address.

2. Check your Git configuration: You can check your Git configuration using the following command:
```bash
git config --list
```

3. Set up SSH key: If you plan to use Git with SSH, you need to set up an SSH key. To do this, follow these steps:
  a. Generate an SSH key using the following command:
  ```bash
    ssh-keygen -t rsa -b 4096 -C "youremail@example.com"
  ```
  Replace "youremail@example.com" with your actual email address.

  b. Copy the SSH key to your clipboard using the following command:
  ```bash
    pbcopy < ~/.ssh/id_rsa.pub
  ```
  If you are on a Linux machine, use the following command instead:
  ```bash
    cat ~/.ssh/id_rsa.pub
  ```
 
  c. Add the SSH key to your Git account: Log in to your Git account and go to your account settings. Click on "SSH and GPG keys" and then click on "New SSH key". Paste the SSH  key that you copied in the previous step into the "Key" field and give the key a title.
  
### gh github CLI
To install GitHub CLI (gh) on Linux EC2, you can follow these steps:

1. Open your SSH terminal and update the package list by running the following command:
```bash
sudo yum update
```

2. Install the required dependencies by running the following command:
```bash
sudo yum install -y ca-certificates git
```

3. Download the latest version of the gh binary from the GitHub CLI releases page by running the following command:
```bash
curl -L -o gh.tar.gz https://github.com/cli/cli/releases/download/v2.1.0/gh_2.1.0_linux_amd64.tar.gz
```
Note: You can replace the version number with the latest version available on the releases page.

4. Extract the gh binary from the tarball by running the following command:

```bash
tar -xzf gh.tar.gz gh_2.1.0_linux_amd64/bin/gh --strip-components=1
```
Note: Replace the version number and path to the gh binary in the command with the appropriate values for your system.

5. Move the gh binary to the /usr/local/bin directory by running the following command:

```bash
sudo mv gh /usr/local/bin/
```
Verify that gh is installed correctly by running the following command:

```bash
gh --version
```
This should display the version number of gh installed on your system.
